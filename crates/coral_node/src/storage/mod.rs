//! `AgentStorage` — the put/get/list storage abstraction backing the
//! per-agent FS, with a `BTreeMap`-backed [`MemoryStorage`] reference impl
//! (gated behind `#[cfg(any(test, feature = "memory-storage"))]`) and an
//! on-disk [`LocalStorage`] backend in the `local` submodule.

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::io;
use thiserror::Error;

pub mod git;

pub mod local;
pub use local::LocalStorage;

pub mod per_agent_git;
pub use per_agent_git::PerAgentGitStorage;

#[cfg(any(test, feature = "memory-storage"))]
pub mod memory;

#[cfg(any(test, feature = "memory-storage"))]
pub use memory::MemoryStorage;

/// Outcome of a [`AgentStorage::put_if_absent`] call. Distinguishes
/// "wrote the bytes" from "key already existed, nothing changed" — used
/// by content-addressed write paths to know whether a write happened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutOutcome {
    /// The bytes were written and the key now resolves.
    Created,
    /// The key already existed; the caller's bytes were discarded and the
    /// prior value is preserved.
    Existed,
}

/// One page of keys returned from [`AgentStorage::list`]. `next_cursor`
/// is `Some(last_key_in_page)` when more keys exist after this page;
/// pass it back as `after` on the next call to continue. `None` signals
/// the listing is exhausted under the supplied prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListPage {
    pub keys: Vec<String>,
    pub next_cursor: Option<String>,
}

/// Typed errors raised by `AgentStorage` impls. The taxonomy lets callers
/// distinguish "key absent" (often expected) from "retry me" from "fail
/// hard."
///
/// Implementations follow a "soft-not-found" convention:
/// [`AgentStorage::get`] returns `Ok(None)` for an absent key instead of
/// `Err(NotFound)` because the caller almost always wants to branch on
/// presence rather than treat absence as an error. The `NotFound` variant
/// exists for backends that surface it through the error channel for
/// *other* operations; impls are expected to swallow it into `Ok(())` for
/// [`AgentStorage::delete`] (idempotent contract).
#[derive(Debug, Error)]
pub enum StorageError {
    /// A key was expected but does not resolve. `AgentStorage::get` does
    /// **not** return this — `Ok(None)` is the absence signal — but
    /// impls may use it on other operations where the contract demands
    /// presence.
    #[error("key not found: {0}")]
    NotFound(String),
    /// `put_if_absent` raced against a prior writer; the key existed at
    /// commit time. The `PutOutcome::Existed` return value is the normal
    /// path; this variant is reserved for backends that surface the
    /// condition as a hard error (e.g. an S3 `412 PreconditionFailed`
    /// classified as a fault rather than the expected outcome).
    #[error("conflict on key: {0}")]
    Conflict(String),
    /// Retryable I/O failure: network blip, throttled request, transient
    /// disk error. The activity layer can retry with backoff.
    #[error("transient storage error: {0}")]
    Transient(String),
    /// Non-retryable failure: invalid key, auth denied, quota exceeded.
    /// The activity layer should fail the workflow rather than spin on
    /// retries.
    #[error("permanent storage error: {0}")]
    Permanent(String),
    /// A `get` targeted a key that resolves to a directory, not a file
    /// blob. Not a transient failure — a read aimed at the wrong kind of
    /// path — so it is surfaced typed, letting the FS facade fold it into
    /// a model correction rather than a retryable storage error.
    #[error("path is a directory, not a file: {0}")]
    IsADirectory(String),
    /// Escape hatch for errors that don't fit the taxonomy. Prefer
    /// classifying when possible.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl StorageError {
    /// Classify an `io::Error` from a local filesystem call. Exposed so
    /// every backend uses the same mapping for the I/O kinds it shares
    /// with the local impl.
    pub fn from_io(err: io::Error) -> Self {
        use io::ErrorKind as K;
        match err.kind() {
            K::NotFound => StorageError::NotFound(err.to_string()),
            K::AlreadyExists => StorageError::Conflict(err.to_string()),
            K::PermissionDenied => StorageError::Permanent(err.to_string()),
            K::IsADirectory => StorageError::IsADirectory(err.to_string()),
            K::Interrupted | K::TimedOut | K::WouldBlock => {
                StorageError::Transient(err.to_string())
            }
            // EISDIR (os error 21) on toolchains/platforms that don't surface
            // it as the typed `IsADirectory` kind: still a directory read, not
            // a transient failure — classify it so a bad read never wedges the
            // retry loop.
            _ if err.raw_os_error() == Some(21) => StorageError::IsADirectory(err.to_string()),
            _ => StorageError::Other(anyhow::Error::from(err)),
        }
    }
}

/// Convenience `Result` alias used by every method on [`AgentStorage`].
pub type StorageResult<T> = Result<T, StorageError>;

/// The pluggable per-agent storage backend. Implementations:
///
/// - [`MemoryStorage`] — in-process `BTreeMap`, gated behind a feature
///   flag, used by tests.
/// - [`LocalStorage`] — on-disk POSIX directory; atomic semantics via
///   tempfile + rename and `O_EXCL`.
///
/// # Contract
///
/// * `put`: atomic single-shot write. On success, the new bytes are
///   visible at `key`; on failure, the prior value (or absence) is
///   preserved — no partial state.
/// * `put_if_absent`: atomic conditional write. Returns
///   [`PutOutcome::Created`] when the bytes were committed,
///   [`PutOutcome::Existed`] when the key already had a value. Backends
///   that cannot express the condition natively are expected to emulate
///   it (e.g. local-FS uses `OpenOptions::create_new`).
/// * `get`: returns `Ok(Some(_))` when the key resolves, `Ok(None)` when
///   the key does not. Absence is *not* an error.
/// * `get_many`: parallel fetch. Result vec has the same length and
///   order as `keys`; absent keys are `None` in their slot.
/// * `delete`: idempotent. Removing an absent key is `Ok(())`.
/// * `list`: lex-sorted ascending under `prefix`. `after` is exclusive
///   (start strictly after this key); `limit` caps the page size and
///   `ListPage::next_cursor` carries continuation. Impls may return
///   fewer keys than `limit`.
///
/// All bounds (`Send + Sync + 'static`) keep the trait usable as a
/// `dyn AgentStorage` behind an `Arc` and shareable across async tasks.
#[async_trait]
pub trait AgentStorage: Send + Sync + 'static {
    async fn put(&self, key: &str, value: Bytes) -> StorageResult<()>;
    async fn put_if_absent(&self, key: &str, value: Bytes) -> StorageResult<PutOutcome>;
    async fn get(&self, key: &str) -> StorageResult<Option<Bytes>>;
    async fn get_many(&self, keys: &[&str]) -> StorageResult<Vec<Option<Bytes>>>;
    async fn delete(&self, key: &str) -> StorageResult<()>;
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StorageResult<ListPage>;
}

/// Git's content address for a file's bytes — the 40-char hex `sha1` of
/// `"blob <len>\0<content>"`. Content-deterministic: identical bytes always
/// hash to the same `BlobSha`. That determinism is what makes it safe to
/// pin in a reference (a Temporal retry recomputes the same sha) and to use
/// as a dedup key (`hash → filepath`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlobSha(String);

impl BlobSha {
    /// Reconstruct a `BlobSha` from its hex digest — e.g. when reading a
    /// pinned sha back out of the provenance DB. The string is taken
    /// as-is (it originates from a prior [`as_str`](BlobSha::as_str) /
    /// commit-manifest round-trip), so no validation is performed.
    pub fn from_hex(hex: impl Into<String>) -> Self {
        Self(hex.into())
    }

    /// Compute a file's `BlobSha` from its bytes, without a repository.
    ///
    /// Delegates to libgit2's object hasher, so the result is identical to
    /// the sha [`PerAgentGitStorage`](super::PerAgentGitStorage)'s `commit`
    /// records for the same bytes — letting a reference be pinned at write
    /// time (DB) and resolved later (git) by the same address. That equality
    /// is asserted
    /// by a guard test; if it ever drifts (e.g. an `autocrlf`/filter config
    /// crept in) staleness detection silently breaks.
    pub fn of_bytes(bytes: &[u8]) -> Self {
        let oid = git2::Oid::hash_object(git2::ObjectType::Blob, bytes)
            .expect("libgit2 blob hashing of in-memory bytes is infallible");
        Self(oid.to_string())
    }

    /// The 40-char lowercase hex digest.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BlobSha {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Per-agent versioning layered on [`AgentStorage`]: a content-addressed,
/// commit-per-tick history scoped to exactly two operations, each addressing
/// one agent by its FS prefix. There is deliberately no branch / checkout /
/// merge / rebase / history-rewrite surface — the working tree always
/// reflects HEAD, and historical reads resolve a blob sha directly without
/// ever moving HEAD.
///
/// As a supertrait, one `dyn VersionedStorage` handle exposes both the data
/// plane (put/get/list) and versioning (commit/read-at-sha), which the
/// cycle-boundary commit activity needs together. Git is the local
/// implementation ([`PerAgentGitStorage`]); keeping the surface on the trait
/// — not the impl — is the long-term commitment so an object-store backend
/// can offer the same two operations later.
#[async_trait]
pub trait VersionedStorage: AgentStorage {
    /// Commit one agent's working tree (the subtree under `agent_prefix`) as a
    /// single tick and return the `(path, blob_sha)` manifest of every file in
    /// the committed tree (paths relative to the prefix, e.g.
    /// `outputs/output.md`).
    ///
    /// Idempotent: a clean tree (identical to HEAD) is a no-op that still
    /// returns the current manifest, so a Temporal retry of an already-
    /// committed tick converges without creating a divergent commit. Every
    /// returned sha names a durably committed object, resolvable by
    /// [`VersionedStorage::read_at`] across restarts — so a caller can pin a
    /// returned sha knowing it will always read back.
    async fn commit(
        &self,
        agent_prefix: &str,
        message: &str,
    ) -> StorageResult<Vec<(String, BlobSha)>>;

    /// Resolve a blob sha to its bytes within one agent's repo. The sha *is*
    /// the content address; the call is read-only and never mutates HEAD or
    /// the working tree. Returns `Ok(None)` when no such blob exists in that
    /// agent's object database (or the sha is malformed).
    async fn read_at(&self, agent_prefix: &str, sha: &BlobSha) -> StorageResult<Option<Bytes>>;
}

#[cfg(test)]
mod tests {
    //! Trait-level conformance suite shared by every backend so behaviour
    //! stays byte-identical across impls.
    use super::*;
    use std::sync::Arc;

    /// Verify the round-trip for `put` + `get`.
    async fn verify_put_get_round_trip(storage: Arc<dyn AgentStorage>) {
        let key = "a";
        let value = Bytes::from_static(b"hello");
        storage.put(key, value.clone()).await.unwrap();
        let got = storage.get(key).await.unwrap();
        assert_eq!(got.as_deref(), Some(value.as_ref()));
    }

    /// `get` on an absent key returns `Ok(None)` (soft-not-found).
    async fn verify_get_absent_returns_none(storage: Arc<dyn AgentStorage>) {
        let got = storage.get("never-written").await.unwrap();
        assert!(got.is_none(), "expected None for absent key, got {got:?}");
    }

    /// `put_if_absent` returns `Created` on first write, `Existed` on
    /// subsequent writes — and never overwrites the existing bytes.
    async fn verify_put_if_absent_semantics(storage: Arc<dyn AgentStorage>) {
        let key = "evidence/abc";
        let first = storage
            .put_if_absent(key, Bytes::from_static(b"v1"))
            .await
            .unwrap();
        assert_eq!(first, PutOutcome::Created);

        let second = storage
            .put_if_absent(key, Bytes::from_static(b"v2"))
            .await
            .unwrap();
        assert_eq!(second, PutOutcome::Existed);

        let got = storage.get(key).await.unwrap().unwrap();
        assert_eq!(got.as_ref(), b"v1");
    }

    /// `get_many` preserves input order and uses `None` for missing keys.
    async fn verify_get_many_preserves_order_and_missing_slots(storage: Arc<dyn AgentStorage>) {
        storage.put("a", Bytes::from_static(b"A")).await.unwrap();
        storage.put("b", Bytes::from_static(b"B")).await.unwrap();
        storage.put("c", Bytes::from_static(b"C")).await.unwrap();

        let keys: &[&str] = &["c", "missing", "a", "b"];
        let got = storage.get_many(keys).await.unwrap();
        assert_eq!(got.len(), 4);
        assert_eq!(got[0].as_deref(), Some(b"C".as_ref()));
        assert!(got[1].is_none(), "missing key slot should be None");
        assert_eq!(got[2].as_deref(), Some(b"A".as_ref()));
        assert_eq!(got[3].as_deref(), Some(b"B".as_ref()));
    }

    /// `delete` removes a present key and is idempotent for absent keys.
    async fn verify_delete_idempotent(storage: Arc<dyn AgentStorage>) {
        storage
            .put("doomed", Bytes::from_static(b"bye"))
            .await
            .unwrap();
        storage.delete("doomed").await.unwrap();
        assert!(storage.get("doomed").await.unwrap().is_none());

        storage.delete("doomed").await.unwrap();
        storage.delete("never-written").await.unwrap();
    }

    /// `list` returns lex-sorted keys under `prefix`, honors `after`,
    /// and paginates through `limit` + `next_cursor`.
    async fn verify_list_prefix_and_pagination(storage: Arc<dyn AgentStorage>) {
        for k in ["outputs/01", "outputs/02", "outputs/03", "outputs/04"] {
            storage.put(k, Bytes::from_static(b"x")).await.unwrap();
        }
        storage
            .put("evidence/aa", Bytes::from_static(b"y"))
            .await
            .unwrap();

        let page = storage.list("outputs/", None, 100).await.unwrap();
        assert_eq!(
            page.keys,
            vec![
                "outputs/01".to_string(),
                "outputs/02".to_string(),
                "outputs/03".to_string(),
                "outputs/04".to_string(),
            ]
        );
        assert!(page.next_cursor.is_none());

        let page1 = storage.list("outputs/", None, 2).await.unwrap();
        assert_eq!(
            page1.keys,
            vec!["outputs/01".to_string(), "outputs/02".to_string()]
        );
        assert_eq!(page1.next_cursor.as_deref(), Some("outputs/02"));

        let page2 = storage
            .list("outputs/", page1.next_cursor.as_deref(), 2)
            .await
            .unwrap();
        assert_eq!(
            page2.keys,
            vec!["outputs/03".to_string(), "outputs/04".to_string()]
        );
        // Page exactly `limit` but no more keys exist → cursor None.
        assert!(page2.next_cursor.is_none());

        // `after` is exclusive: an existing key is skipped.
        let after_two = storage
            .list("outputs/", Some("outputs/02"), 100)
            .await
            .unwrap();
        assert_eq!(
            after_two.keys,
            vec!["outputs/03".to_string(), "outputs/04".to_string()]
        );

        assert!(!page.keys.iter().any(|k| k.starts_with("evidence/")));
    }

    /// Empty prefix lists everything (lex-sorted across the whole store).
    async fn verify_list_empty_prefix_lists_everything(storage: Arc<dyn AgentStorage>) {
        storage.put("a", Bytes::from_static(b"1")).await.unwrap();
        storage.put("b", Bytes::from_static(b"2")).await.unwrap();
        let page = storage.list("", None, 100).await.unwrap();
        assert_eq!(page.keys, vec!["a".to_string(), "b".to_string()]);
    }

    // ---- MemoryStorage runs ---------------------------------------------

    fn fresh_memory() -> Arc<dyn AgentStorage> {
        Arc::new(MemoryStorage::new())
    }

    #[tokio::test]
    async fn memory_put_get_round_trip() {
        verify_put_get_round_trip(fresh_memory()).await;
    }

    #[tokio::test]
    async fn memory_get_absent_returns_none() {
        verify_get_absent_returns_none(fresh_memory()).await;
    }

    #[tokio::test]
    async fn memory_put_if_absent_semantics() {
        verify_put_if_absent_semantics(fresh_memory()).await;
    }

    #[tokio::test]
    async fn memory_get_many_preserves_order_and_missing_slots() {
        verify_get_many_preserves_order_and_missing_slots(fresh_memory()).await;
    }

    #[tokio::test]
    async fn memory_delete_idempotent() {
        verify_delete_idempotent(fresh_memory()).await;
    }

    #[tokio::test]
    async fn memory_list_prefix_and_pagination() {
        verify_list_prefix_and_pagination(fresh_memory()).await;
    }

    #[tokio::test]
    async fn memory_list_empty_prefix_lists_everything() {
        verify_list_empty_prefix_lists_everything(fresh_memory()).await;
    }

    // ---- PerAgentGitStorage runs ----------------------------------------
    // The production backend's data plane must behave byte-identically to a
    // plain backend; running the same conformance suite against it proves the
    // git-versioning layer adds no data-plane drift. The `TempDir` is bound so
    // it outlives the storage handle for the test's duration.

    fn fresh_per_agent_git() -> (tempfile::TempDir, Arc<dyn AgentStorage>) {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Arc::new(PerAgentGitStorage::new(tmp.path()).unwrap());
        (tmp, storage)
    }

    #[tokio::test]
    async fn per_agent_git_put_get_round_trip() {
        let (_t, s) = fresh_per_agent_git();
        verify_put_get_round_trip(s).await;
    }

    #[tokio::test]
    async fn per_agent_git_get_absent_returns_none() {
        let (_t, s) = fresh_per_agent_git();
        verify_get_absent_returns_none(s).await;
    }

    #[tokio::test]
    async fn per_agent_git_put_if_absent_semantics() {
        let (_t, s) = fresh_per_agent_git();
        verify_put_if_absent_semantics(s).await;
    }

    #[tokio::test]
    async fn per_agent_git_get_many_preserves_order_and_missing_slots() {
        let (_t, s) = fresh_per_agent_git();
        verify_get_many_preserves_order_and_missing_slots(s).await;
    }

    #[tokio::test]
    async fn per_agent_git_delete_idempotent() {
        let (_t, s) = fresh_per_agent_git();
        verify_delete_idempotent(s).await;
    }

    #[tokio::test]
    async fn per_agent_git_list_prefix_and_pagination() {
        let (_t, s) = fresh_per_agent_git();
        verify_list_prefix_and_pagination(s).await;
    }

    #[tokio::test]
    async fn per_agent_git_list_empty_prefix_lists_everything() {
        let (_t, s) = fresh_per_agent_git();
        verify_list_empty_prefix_lists_everything(s).await;
    }

    /// The trait must be object-safe: this fails to compile if a future
    /// change accidentally adds a generic method or an `impl Trait`
    /// return type. Tested via a `dyn` cast through `Arc`.
    #[tokio::test]
    async fn agent_storage_is_object_safe() {
        let _: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
    }

    /// `StorageError::from_io` classifies the standard `io::ErrorKind`s
    /// the local backend cares about. Pinned here so the taxonomy doesn't
    /// drift silently across the trait + impl boundary.
    #[test]
    fn storage_error_from_io_classification() {
        let nf = StorageError::from_io(io::Error::new(io::ErrorKind::NotFound, "x"));
        assert!(matches!(nf, StorageError::NotFound(_)));

        let ae = StorageError::from_io(io::Error::new(io::ErrorKind::AlreadyExists, "x"));
        assert!(matches!(ae, StorageError::Conflict(_)));

        let pd = StorageError::from_io(io::Error::new(io::ErrorKind::PermissionDenied, "x"));
        assert!(matches!(pd, StorageError::Permanent(_)));

        let to = StorageError::from_io(io::Error::new(io::ErrorKind::TimedOut, "x"));
        assert!(matches!(to, StorageError::Transient(_)));

        // A directory read is a caller fault, not a transient failure — it
        // must not land in a retryable bucket, or a bad read wedges forever.
        let dir = StorageError::from_io(io::Error::new(io::ErrorKind::IsADirectory, "x"));
        assert!(matches!(dir, StorageError::IsADirectory(_)));

        // Unknown kinds fall into Other.
        let other = StorageError::from_io(io::Error::other("x"));
        assert!(matches!(other, StorageError::Other(_)));
    }
}
