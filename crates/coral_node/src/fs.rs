//! `AgentFs` — facade over the pluggable per-agent [`crate::storage::AgentStorage`] backend.
//!
//! Schema under `<prefix>` (empty for single-host bootstrap, otherwise
//! `graphs/<graph_id>/agents/<agent_id>/`):
//!
//! ```text
//! mandate.md             — standing instruction (pure prose; no metadata)
//! outputs/output.md      — the single, kept-current Output (pure prose, overwritten each cycle)
//! evidence/<slug>-<hash>.json — raw tool-call record; interpretable slug name, content-hash suffix
//! notes/                 — mutable private working memory
//! claims/<slug>.json     — claim_seed registry
//! conflicts/<id>.json    — parent-reconciled disagreement record
//! retirement.json        — terminal marker (presence ⇒ agent retired, not crashed)
//! ```
//!
//! The single canonical Output is the deliverable a parent or auditor reads;
//! [`AgentFs::persist_output`] overwrites it each cycle and enforces that
//! every cited evidence path resolves (via [`FsError::EmptyEvidence`] /
//! [`FsError::EvidenceNotFound`]). A stable path (not a content-addressed
//! name) is what lets a parent pin the output's version across refreshes.
//! `notes/` is private scratch that [`AgentFs::apply_ops`] writes to,
//! rejecting anything that escapes `<root>/notes/`.

use crate::agent_ref::{AgentId, GraphId};
use crate::conflict::ConflictRecord;
use crate::decision::{ConflictId, FsOp, Remainder};
use crate::evidence::{EvidenceId, EvidenceRecord};
use crate::mandate::{Mandate, OutputId};
use crate::storage::{AgentStorage, LocalStorage, PutOutcome};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

/// Cap on tail-object size (`<prefix>outputs/_tail.json`,
/// `<prefix>evidence/_tail.json`). 64 entries gives 8× headroom over
/// the default `recent_outputs` / `recent_evidence` window of 8 and
/// keeps the tail object under ~8 KB.
const TAIL_K: usize = 64;

/// One entry in a `_tail.json` object — the filename relative to the
/// indexed prefix plus the wall-clock timestamp the entry was added.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TailEntry {
    /// Bare filename, relative to the indexed prefix so the tail
    /// survives a prefix change at relocation time.
    pub filename: String,
    /// When the entry was added to the tail. Distinct from the file's
    /// own `created_at` so a replayed activity's index update is
    /// distinguishable from the underlying record's timestamp.
    pub added_at: DateTime<Utc>,
}

/// The on-disk shape of `<prefix>outputs/_tail.json` and
/// `<prefix>evidence/_tail.json`. `entries[0]` is the most recently
/// written file; the vector is truncated to [`TAIL_K`] on every update.
///
/// When `entries.len() < TAIL_K` the tail is the authoritative list
/// of every file ever written under the indexed prefix (modulo a
/// torn-write). At capacity, older files may exist on disk that fell
/// off the tail — readers needing the lex-greatest N across the whole
/// history must fall back to the LIST path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TailObject {
    pub entries: Vec<TailEntry>,
}

/// The single canonical Output path. Each agent keeps ONE kept-current
/// Output here, overwritten each cycle. A *stable* path (not content-
/// addressed) is what lets a parent pin a child's output version and the
/// runtime detect staleness when the child later refreshes it.
pub const CANONICAL_OUTPUT: &str = "outputs/output.md";
/// Bare filename of the canonical Output, relative to `outputs/`.
const CANONICAL_OUTPUT_FILENAME: &str = "output.md";
/// Tail-object key suffix for `evidence/`.
const EVIDENCE_TAIL_SUFFIX: &str = "evidence/_tail.json";
/// Tail-object key suffix for `notes/`. Unlike evidence, notes are
/// agent-authored, mutable, and deletable, so the tail is maintained on every
/// `apply_ops` write/delete rather than on a content-addressed first-write.
const NOTES_TAIL_SUFFIX: &str = "notes/_tail.json";

/// Tail-record predicate for the content-addressed `evidence/` prefix: a
/// `.json` record, never the sidecar.
fn is_json_record(filename: &str) -> bool {
    filename.ends_with(".json") && filename != "_tail.json"
}

/// Tail-record predicate for `notes/`: any agent-authored file (markdown or
/// otherwise, at any depth) except the runtime-owned recency sidecar.
fn is_note_record(filename: &str) -> bool {
    filename != "_tail.json" && !filename.ends_with("/_tail.json")
}

/// A recency-ordered window of bare filenames under one indexed prefix, plus
/// how many files lie beyond it. Built for the cycle seed's pointer index:
/// `more` lets the seed tell the model the view is partial (and by how much)
/// so it explores (`list`/`search`) for the rest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecentWindow {
    /// Up to `n` filenames, most-recent-first.
    pub filenames: Vec<String>,
    /// How many files exist under the prefix beyond those in `filenames`.
    pub more: Remainder,
}

/// Typed errors the `AgentFs` raises. The run loop matches on these to
/// distinguish provenance/traversal violations from real storage
/// failures, so adding variants is a breaking change for that consumer.
#[derive(Debug, Error)]
pub enum FsError {
    /// `persist_output` was called with an empty citation slice.
    #[error("output rejected: citation list is empty (provenance contract)")]
    EmptyEvidence,
    /// A citation named an evidence path with no record on disk.
    #[error("output rejected: evidence path {0} not found on disk")]
    EvidenceNotFound(String),
    /// A citation named a path outside the runtime-authored `evidence/`
    /// directory. Citations must point at real evidence records, so the
    /// writer refuses a path the model could have hand-authored elsewhere.
    #[error("citation rejected: {0} is not under evidence/")]
    CitationNotEvidence(String),
    /// [`AgentFs::read_output`] found no canonical Output on disk — the
    /// agent has not emitted one yet. Typed so the reconcile path can fold
    /// it into a correction context.
    #[error("no canonical output found on disk")]
    OutputNotFound,
    /// [`AgentFs::read_file`] was asked for a path that resolves to no
    /// file. The model picked a filename that does not exist; surfaced as
    /// a typed error so the cycle can fold it into a failure observation
    /// the next step adapts to.
    #[error("file {0} not found")]
    FileNotFound(String),
    /// [`AgentFs::write_conflict`] was called with fewer than two
    /// alternatives — a single-alternative conflict carries no
    /// information so the writer rejects it as a structural error.
    #[error("conflict rejected: only {count} alternatives (need >= 2)")]
    ConflictAlternativesTooFew { count: usize },
    /// An `FsOp` path contained `..`, an absolute root, or a Windows
    /// prefix — anything that could escape the agent's root.
    #[error("path traversal rejected: {0}")]
    PathTraversal(String),
    /// An `FsOp` path was syntactically clean but resolved outside
    /// `<root>/notes/`. Bootstrap `apply_ops` only writes under `notes/`.
    #[error("path outside notes/ rejected: {0}")]
    PathOutsideNotes(String),
    /// An `FsOp` targeted `notes/_tail.json`, the runtime-owned recency
    /// sidecar. It is excluded from the agent's writable surface like
    /// `evidence/` — a model write would corrupt the notes index.
    #[error("path reserved (runtime-owned): {0}")]
    ReservedNotesPath(String),
    /// Wrapped backend error from the underlying
    /// [`crate::storage::AgentStorage`]. The `key` field carries the
    /// logical key (under the agent's prefix) that the operation
    /// targeted, so a failure trail can be reconstructed even when the
    /// backend's error string is opaque.
    #[error("storage error at {key}: {source}")]
    Storage {
        key: String,
        #[source]
        source: crate::storage::StorageError,
    },
}

impl FsError {
    fn storage(key: impl Into<String>, source: crate::storage::StorageError) -> Self {
        FsError::Storage {
            key: key.into(),
            source,
        }
    }
}

/// On-disk record written to `retirement.json`.
#[derive(Debug, Serialize, Deserialize)]
struct RetirementRecord {
    reason: String,
    retired_at: DateTime<Utc>,
}

/// Lifecycle status the agent assigns to a claim. The kernel does not
/// interpret these; they let the agent's future self distinguish a
/// claim still under investigation from one already settled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimStatus {
    Open,
    Resolved,
    Abandoned,
}

/// On-disk record written to `claims/<slug>.json`. The agent reads
/// these at the top of a tick to decide whether a new `claim_seed` is
/// needed or an existing one should be reused. The slug is derived
/// from `seed` via [`claim_slug`] and is not stored on the record
/// (it's the filename).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim {
    pub seed: String,
    pub description: String,
    pub status: ClaimStatus,
    pub created_at: DateTime<Utc>,
}

/// Maximum byte length of the kebab body in a slug, before the hash
/// suffix. Keeps `claims/` listings readable and leaves headroom under
/// typical filesystem name limits.
const SLUG_BODY_MAX: usize = 80;

/// Derive an interpretable kebab slug from a seed/label.
///
/// Rules: lowercase, runs of non-`[a-z0-9]` collapse to `-`, leading and
/// trailing `-` are trimmed, and the body is truncated to
/// [`SLUG_BODY_MAX`] bytes (slugs are ASCII, so the byte truncation is
/// always on a char boundary). May return an empty string when the seed
/// has no alphanumerics.
///
/// This is the human-readable file-name *body* (`tsmc-cowos-capacity`),
/// never a hash. It is not guaranteed unique — two seeds can slugify to
/// the same body — so a writer that needs a unique *path* disambiguates
/// against the DB index (the metadata layer owns uniqueness).
pub fn slug(seed: &str) -> String {
    let mut body = String::with_capacity(seed.len());
    let mut prev_dash = true;
    for ch in seed.chars() {
        let lc = ch.to_ascii_lowercase();
        if lc.is_ascii_alphanumeric() {
            body.push(lc);
            prev_dash = false;
        } else if !prev_dash {
            body.push('-');
            prev_dash = true;
        }
    }
    while body.ends_with('-') {
        body.pop();
    }
    if body.len() > SLUG_BODY_MAX {
        body.truncate(SLUG_BODY_MAX);
        while body.ends_with('-') {
            body.pop();
        }
    }
    body
}

/// Derive the on-disk slug for a claim from its seed string: the
/// interpretable [`slug`] plus an unconditional `-<first 8 hex chars of
/// sha256(seed)>` suffix. The hash suffix makes the result a collision-free
/// function of the seed alone — same seed always resolves to the same file,
/// and two seeds that slugify to the same body still get distinct
/// filenames. If the slug body is empty, the result is just the suffix.
pub fn claim_slug(seed: &str) -> String {
    let body = slug(seed);
    let digest = Sha256::digest(seed.as_bytes());
    let suffix = hex::encode(&digest[..4]);

    if body.is_empty() {
        suffix
    } else {
        format!("{body}-{suffix}")
    }
}

/// Build the `evidence/` relative path for one record: the interpretable
/// [`slug`] of `slug_seed` (the model's `claim_seed`, or a runtime label for
/// synthetic records) plus a `-<first 8 hex of the content hash>` suffix.
///
/// Unlike [`claim_slug`], the suffix is keyed on the record's *content* (the
/// `EvidenceId`), not the seed — so different content always gets a distinct
/// path even under the same seed, and identical content is idempotent. When
/// the slug body is empty the path is just the suffix.
///
/// Public because it is the citation-resolution contract: given a record's
/// `claim_seed` and `EvidenceId`, this yields the exact path a later citation
/// must name.
pub fn evidence_relpath(slug_seed: &str, id: &EvidenceId) -> String {
    let body = slug(slug_seed);
    let suffix = &id.as_str()[..8];
    if body.is_empty() {
        format!("evidence/{suffix}.json")
    } else {
        format!("evidence/{body}-{suffix}.json")
    }
}

/// Per-agent filesystem, expressed as a facade over an `AgentStorage`
/// backend. Cheap to clone — holds an `Arc` to the storage and a small
/// key prefix.
///
/// Construct via [`AgentFs::open`] for single-host use (`<root>` is
/// one agent's directory, prefix is empty), or via
/// [`AgentFs::new_with_storage`] to drive against another storage
/// backend with a custom prefix.
#[derive(Clone)]
pub struct AgentFs {
    storage: Arc<dyn AgentStorage>,
    /// Key prefix applied to every operation. Empty for single-host;
    /// `graphs/<graph_id>/agents/<agent_id>/` under multi-agent
    /// topology. Always either empty or ends in `/`.
    prefix: String,
}

impl std::fmt::Debug for AgentFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `dyn AgentStorage` carries no `Debug` bound, so summarise by prefix.
        f.debug_struct("AgentFs")
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl AgentFs {
    /// Open an on-disk agent FS rooted at `root`.
    ///
    /// Wraps a [`LocalStorage`] backend with an empty prefix; equivalent
    /// to `new_with_storage(Arc::new(LocalStorage::new(root)?), "", mandate)`.
    ///
    /// Idempotent: opening an existing FS does not clobber
    /// `mandate.md`, `outputs/`, `evidence/`, `notes/`, or
    /// `retirement.json` — the mandate file is only written when absent.
    pub async fn open(root: PathBuf, mandate: &Mandate) -> anyhow::Result<Self> {
        let storage = Arc::new(LocalStorage::new(root)?);
        Self::new_with_storage(storage, String::new(), mandate).await
    }

    /// Build an `AgentFs` over any storage backend with the supplied key
    /// prefix.
    ///
    /// `prefix` is normalized to either `""` or "`...something/`" — a
    /// trailing slash is appended if missing. Writing `mandate.md`
    /// (when absent) is the only state side effect; directory creation
    /// is handled lazily by the backend.
    pub async fn new_with_storage(
        storage: Arc<dyn AgentStorage>,
        prefix: impl Into<String>,
        mandate: &Mandate,
    ) -> anyhow::Result<Self> {
        let mut prefix = prefix.into();
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        let me = Self { storage, prefix };

        // Idempotent: re-open must not clobber an existing mandate.
        // The file is pure prose — just the standing instruction. Mandate
        // config (cadence/model/...) is not persisted here; it flows
        // in-memory and lives in the DB (A1: content in the FS, metadata
        // in the DB).
        let mandate_key = me.key("mandate.md");
        let existing = me
            .storage
            .get(&mandate_key)
            .await
            .map_err(|e| FsError::storage(&mandate_key, e))?;
        if existing.is_none() {
            me.storage
                .put(&mandate_key, Bytes::from(mandate.text.clone()))
                .await
                .map_err(|e| FsError::storage(&mandate_key, e))?;
        }

        // Reconcile any tail that lags on-disk reality after a crash
        // mid-PUT, so the O(1) read path can trust a sub-capacity tail
        // as complete. One LIST + at most one PUT per indexed prefix.
        // `outputs/` has no tail — it holds one canonical, overwritten file.
        me.reconcile_tail(EVIDENCE_TAIL_SUFFIX, "evidence/", is_json_record)
            .await?;
        me.reconcile_tail(NOTES_TAIL_SUFFIX, "notes/", is_note_record)
            .await?;

        Ok(me)
    }

    /// Build an `AgentFs` over `storage` at `prefix` without the
    /// `mandate.md` read/write or the tail-index reconciliation
    /// that [`AgentFs::new_with_storage`] performs. Makes no I/O calls.
    ///
    /// Use when the caller has no `Mandate` in scope, or when the
    /// operation does not touch `evidence/` / `notes/` and the
    /// per-attach LIST is wasted work. Callers must not use `attach`
    /// for fresh-FS paths that rely on tail-index invariants
    /// (`list_recent_evidence`, `recent_note_filenames`); those need
    /// `new_with_storage`'s reconcile step.
    pub fn attach(storage: Arc<dyn AgentStorage>, prefix: impl Into<String>) -> Self {
        let mut prefix = prefix.into();
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        Self { storage, prefix }
    }

    /// Build an `AgentFs` scoped to an arbitrary agent's
    /// `graphs/<graph_id>/agents/<agent_id>/` prefix on the supplied
    /// storage backend. Cross-agent reads (a parent reading a child's
    /// canonical `outputs/output.md`) flow through this constructor.
    ///
    /// An `attach` wrapper — no `mandate.md` read, no tail-index
    /// reconcile. The caller does not have the other agent's `Mandate`
    /// in scope, and reconcile-target reads are point lookups that do
    /// not depend on the tail's freshness.
    ///
    /// Mirrors `crate::workflow::FsHandle::for_agent`'s prefix scheme
    /// so a future schema bump touches one call site rather than every
    /// spawn / reconcile / retire helper.
    pub fn open_for_agent(
        storage: Arc<dyn AgentStorage>,
        graph_id: GraphId,
        agent_id: AgentId,
    ) -> Self {
        let prefix = format!("graphs/{}/agents/{}/", graph_id, agent_id);
        Self::attach(storage, prefix)
    }

    /// Reconcile a tail object against the on-disk reality under its
    /// indexed prefix, called once per `new_with_storage` to recover
    /// from any prior crash that PUT an object without updating the
    /// tail.
    ///
    /// Algorithm: LIST under the prefix; if every on-disk filename is
    /// already in the tail, no PUT. Otherwise rebuild the tail as the
    /// lex-greatest `TAIL_K` filenames in newest-first order,
    /// preserving any prior `added_at` so a no-op re-reconciliation
    /// produces byte-identical bytes, and PUT it back.
    async fn reconcile_tail(
        &self,
        tail_suffix: &str,
        indexed_prefix: &str,
        is_record: fn(&str) -> bool,
    ) -> anyhow::Result<()> {
        let full_prefix = self.key(indexed_prefix);
        let page = self
            .storage
            .list(&full_prefix, None, usize::MAX)
            .await
            .map_err(|e| FsError::storage(&full_prefix, e))?;
        // Reduce keys to record filenames (strip indexed prefix, drop sidecars).
        let mut on_disk: Vec<String> = page
            .keys
            .into_iter()
            .filter_map(|k| k.strip_prefix(&full_prefix).map(|s| s.to_string()))
            .filter(|f| is_record(f))
            .collect();
        if on_disk.is_empty() {
            return Ok(());
        }
        on_disk.sort();

        let tail_key = self.key(tail_suffix);
        let existing_tail: TailObject = match self
            .storage
            .get(&tail_key)
            .await
            .map_err(|e| FsError::storage(&tail_key, e))?
        {
            Some(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            None => TailObject::default(),
        };

        let tail_filenames: std::collections::HashSet<&str> = existing_tail
            .entries
            .iter()
            .map(|e| e.filename.as_str())
            .collect();

        // Fast path: no PUT when every on-disk filename is in the
        // tail. Stale tail entries pointing to deleted files are
        // dropped silently by the read path, not GC'd here.
        if on_disk.iter().all(|f| tail_filenames.contains(f.as_str())) {
            return Ok(());
        }

        // Preserve original `added_at` so a no-op subsequent reconcile
        // produces byte-identical bytes.
        let existing_added_at: std::collections::HashMap<&str, DateTime<Utc>> = existing_tail
            .entries
            .iter()
            .map(|e| (e.filename.as_str(), e.added_at))
            .collect();

        // Lex-greatest TAIL_K, reversed so entry 0 is the newest.
        let take_from = on_disk.len().saturating_sub(TAIL_K);
        let mut chosen: Vec<&str> = on_disk[take_from..].iter().map(String::as_str).collect();
        chosen.reverse();
        let now = Utc::now();
        let rebuilt = TailObject {
            entries: chosen
                .into_iter()
                .map(|f| TailEntry {
                    filename: f.to_string(),
                    added_at: existing_added_at.get(f).copied().unwrap_or(now),
                })
                .collect(),
        };

        let bytes = serde_json::to_vec(&rebuilt)?;
        self.storage
            .put(&tail_key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&tail_key, e))?;
        Ok(())
    }

    /// Borrow the underlying storage. Exposed for higher layers that
    /// need direct trait access without a per-shape method.
    pub fn storage(&self) -> &Arc<dyn AgentStorage> {
        &self.storage
    }

    /// Borrow the agent's key prefix (always either empty or ending in `/`).
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Persist an `EvidenceRecord` under an interpretable, content-
    /// disambiguated path and return that path — the handle the model
    /// cites in a later `WriteOutput`.
    ///
    /// The filename is `evidence/<slug(slug_seed)>-<short content hash>.json`:
    /// the body is the human-readable [`slug`] of the model's `claim_seed`
    /// (or a runtime label for synthetic records), and the suffix is the
    /// first 8 hex of the record's content hash. So different content always
    /// lands at a distinct path (no collision detection), while the same
    /// content under the same seed resolves to the same path — making the
    /// write idempotent under retries via
    /// [`crate::storage::AgentStorage::put_if_absent`]. Two unrelated seeds
    /// that produce byte-identical content get two files: cross-content dedup
    /// lives in the DB index, not in the filename.
    pub async fn record_evidence(
        &self,
        record: EvidenceRecord,
        slug_seed: &str,
    ) -> anyhow::Result<String> {
        let relpath = evidence_relpath(slug_seed, &record.id);
        let key = self.key(&relpath);
        let bytes = serde_json::to_vec_pretty(&record)?;
        let outcome: PutOutcome = self
            .storage
            .put_if_absent(&key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        // Tail update only on first write — a replayed record would
        // otherwise shuffle the entry to the front, polluting recency
        // with retry artefacts.
        if matches!(outcome, PutOutcome::Created) {
            let filename = relpath
                .strip_prefix("evidence/")
                .unwrap_or(&relpath)
                .to_string();
            self.append_to_tail(EVIDENCE_TAIL_SUFFIX, filename).await?;
        }
        Ok(relpath)
    }

    /// Return `Ok(())` if `path` is a citable evidence record (under
    /// `evidence/` and present on disk). `Err(FsError::CitationNotEvidence)`
    /// for a path outside `evidence/`, `Err(FsError::EvidenceNotFound)` when
    /// it resolves to nothing.
    pub async fn evidence_must_exist(&self, path: &str) -> anyhow::Result<()> {
        let rel = self.evidence_relpath_checked(path)?;
        let key = self.key(&rel);
        let got = self
            .storage
            .get(&key)
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        if got.is_some() {
            Ok(())
        } else {
            Err(FsError::EvidenceNotFound(path.to_string()).into())
        }
    }

    /// Read an evidence record's raw on-disk bytes (the stored `.json`).
    /// The bytes — not a re-serialization — are what callers hash for the
    /// blob sha (so it matches what git would commit) and parse for the
    /// `tool`/`args` discriminator. `Err(FsError::CitationNotEvidence)` for a
    /// path outside `evidence/`, `Err(FsError::EvidenceNotFound)` when it
    /// does not resolve.
    pub async fn read_evidence_bytes(&self, path: &str) -> anyhow::Result<Bytes> {
        let rel = self.evidence_relpath_checked(path)?;
        let key = self.key(&rel);
        let got = self
            .storage
            .get(&key)
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        got.ok_or_else(|| FsError::EvidenceNotFound(path.to_string()).into())
    }

    /// Clean a model-supplied citation path and confirm it is a citable
    /// `evidence/` record. Traversal is rejected by [`Self::clean_relpath`]; a
    /// clean path outside `evidence/`, or the runtime-owned `_tail.json`
    /// sidecar (which is not an evidence record), is rejected as
    /// [`FsError::CitationNotEvidence`].
    fn evidence_relpath_checked(&self, path: &str) -> anyhow::Result<String> {
        let rel = self.clean_relpath(path)?;
        if rel == "evidence" || !rel.starts_with("evidence/") || rel == EVIDENCE_TAIL_SUFFIX {
            return Err(FsError::CitationNotEvidence(path.to_string()).into());
        }
        Ok(rel)
    }

    /// Persist the agent's single, kept-current Output as pure prose at the
    /// canonical path `outputs/output.md`, **overwriting** any prior cycle's
    /// body. A stable path (not a content-addressed filename) is what lets a
    /// parent pin this output's version and the runtime detect staleness when
    /// it is later refreshed — see [`CANONICAL_OUTPUT`].
    ///
    /// Enforces the provenance contract: at least one citation, and every
    /// cited path must resolve to a record under `evidence/`. Only the body is
    /// written to the FS — citations live in the DB reference graph, never in
    /// the file (A1: content in the FS, provenance in the DB). Returns the
    /// [`OutputId`] fingerprint of the body for the `ChildOutput` signal.
    /// Idempotent under retries: the same body PUTs byte-identical bytes.
    pub async fn persist_output(
        &self,
        body: &str,
        citations: &[String],
    ) -> anyhow::Result<OutputId> {
        if citations.is_empty() {
            return Err(FsError::EmptyEvidence.into());
        }
        // Verify every cited path is a real evidence record before the write.
        for path in citations {
            self.evidence_must_exist(path).await?;
        }
        let key = self.key(CANONICAL_OUTPUT);
        self.storage
            .put(&key, Bytes::from(body.as_bytes().to_vec()))
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        Ok(OutputId::new(body))
    }

    /// Apply a batch of model-authored filesystem ops. Writes and
    /// deletes are accepted **only** under `notes/`; any path that
    /// escapes `<root>/notes/` rejects the entire batch before any write
    /// happens.
    ///
    /// `notes/` being the model's sole writable surface *is* the
    /// authorship boundary: `evidence/` is runtime-authored, so the
    /// model can never hand-forge an observation — what keeps provenance
    /// non-fakeable. Any future widening of the model's writable set
    /// must keep `evidence/` excluded.
    pub async fn apply_ops(&self, ops: Vec<FsOp>) -> anyhow::Result<()> {
        let notes_prefix = self.key("notes/");
        // Pre-validate so a bad path mid-batch leaves no partial state.
        let mut planned: Vec<(String, FsOp)> = Vec::with_capacity(ops.len());
        for op in ops {
            let raw = match &op {
                FsOp::WriteFile { path, .. } | FsOp::DeleteFile { path } => path.as_str(),
            };
            let resolved = self.resolve_notes_key(raw)?;
            let rel = resolved.strip_prefix(&notes_prefix).unwrap_or(&resolved);
            if !is_note_record(rel) {
                return Err(FsError::ReservedNotesPath(raw.to_string()).into());
            }
            planned.push((resolved, op));
        }

        for (key, op) in planned {
            let rel = key.strip_prefix(&notes_prefix).map(str::to_string);
            match op {
                FsOp::WriteFile { content, .. } => {
                    self.storage
                        .put(&key, Bytes::from(content.into_bytes()))
                        .await
                        .map_err(|e| FsError::storage(&key, e))?;
                    if let Some(rel) = rel {
                        self.append_to_tail(NOTES_TAIL_SUFFIX, rel).await?;
                    }
                }
                FsOp::DeleteFile { .. } => {
                    // Idempotent: missing key is fine.
                    self.storage
                        .delete(&key)
                        .await
                        .map_err(|e| FsError::storage(&key, e))?;
                    if let Some(rel) = rel {
                        self.remove_from_tail(NOTES_TAIL_SUFFIX, &rel).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Read the agent's single canonical Output body (`outputs/output.md`).
    ///
    /// Returns `Err(FsError::OutputNotFound)` when the agent has not yet
    /// emitted an output, so callers can fold the miss into a typed error.
    /// Read-only by construction; composes with [`AgentFs::open_for_agent`]
    /// for cross-agent reads (a parent reading its child's current Output).
    pub async fn read_output(&self) -> anyhow::Result<String> {
        let key = self.key(CANONICAL_OUTPUT);
        let got = self
            .storage
            .get(&key)
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        match got {
            Some(bytes) => Ok(String::from_utf8(bytes.to_vec())?),
            None => Err(FsError::OutputNotFound.into()),
        }
    }

    /// Return the most recent (up to) `n` `EvidenceRecord`s on disk,
    /// in ascending filename order.
    ///
    /// Evidence filenames are sha256 digests, so lex-greatest is not
    /// the same set as most-recently-written. The tail-fast-path only
    /// engages when the tail is provably complete (under `TAIL_K`);
    /// beyond that the LIST fallback handles the lex window. The
    /// common case — well under `TAIL_K` records alive — stays O(1).
    pub async fn list_recent_evidence(&self, n: usize) -> anyhow::Result<Vec<EvidenceRecord>> {
        let prefix = self.key("evidence/");
        self.read_recent_window_with_tail::<EvidenceRecord>(&prefix, EVIDENCE_TAIL_SUFFIX, n, false)
            .await
    }

    /// Write `retirement.json` with the supplied reason and
    /// `retired_at` timestamp. Overwrites any prior retirement record.
    ///
    /// `retired_at` is supplied by the caller, not stamped here, so
    /// the Temporal workflow path can source a deterministic timestamp
    /// from activity-scheduled-time and replay produces byte-identical
    /// bytes. The in-process loop passes `Utc::now()`.
    pub async fn persist_retirement(
        &self,
        reason: &str,
        retired_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let record = RetirementRecord {
            reason: reason.to_string(),
            retired_at,
        };
        let key = self.key("retirement.json");
        let bytes = serde_json::to_vec_pretty(&record)?;
        self.storage
            .put(&key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        Ok(())
    }

    /// Write a claim under `claims/<slug>.json`. Slug is derived from
    /// `claim.seed` via [`claim_slug`]. Overwrites any existing file at
    /// that slug — status updates flow through the same path.
    pub async fn write_claim(&self, claim: &Claim) -> anyhow::Result<()> {
        let key = self.claim_key(&claim.seed);
        let bytes = serde_json::to_vec_pretty(claim)?;
        self.storage
            .put(&key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        Ok(())
    }

    /// Read the claim written for `seed`. Returns `Ok(None)` when no
    /// record is present so callers can distinguish "first time
    /// minting this seed" from "I/O failed".
    pub async fn read_claim(&self, seed: &str) -> anyhow::Result<Option<Claim>> {
        let key = self.claim_key(seed);
        let got = self
            .storage
            .get(&key)
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        match got {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Return every claim currently on disk in ascending filename
    /// order. The order is deterministic but not chronological — file
    /// names are slugs, not ULIDs. Callers that need recency should
    /// sort by `created_at` themselves.
    pub async fn list_claims(&self) -> anyhow::Result<Vec<Claim>> {
        let prefix = self.key("claims/");
        self.read_recent_json::<Claim>(&prefix, usize::MAX).await
    }

    /// Persist a [`ConflictRecord`] under
    /// `<prefix>conflicts/<id>.json` and return its content-addressed
    /// [`ConflictId`].
    ///
    /// Validates `record.alternatives.len() >= 2` and returns
    /// [`FsError::ConflictAlternativesTooFew`] otherwise.
    ///
    /// Idempotent under retries: `ConflictId` is content-addressed
    /// over `(alternatives, resolution)`, and `put_if_absent` makes
    /// dedup atomic. `timestamp` is not in the hash; the bytes from
    /// the first call are the bytes that stay on disk.
    ///
    /// No tail-index update — `conflicts/` is bounded to dozens per
    /// agent so the tail-index pattern is unjustified overhead.
    pub async fn write_conflict(&self, record: &ConflictRecord) -> anyhow::Result<ConflictId> {
        if record.alternatives.len() < 2 {
            return Err(FsError::ConflictAlternativesTooFew {
                count: record.alternatives.len(),
            }
            .into());
        }
        let id = record.id.clone();
        let key = self.conflict_key(&id);
        let bytes = serde_json::to_vec_pretty(record)?;
        self.storage
            .put_if_absent(&key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        Ok(id)
    }

    /// Point lookup of one persisted [`ConflictRecord`] by its
    /// content-addressed [`ConflictId`]. Returns `Ok(None)` when
    /// absent — conflict reads are audit-only and a missing id is not
    /// a contract violation, so the shape mirrors `read_claim`.
    pub async fn read_conflict(&self, id: &ConflictId) -> anyhow::Result<Option<ConflictRecord>> {
        let key = self.conflict_key(id);
        let got = self
            .storage
            .get(&key)
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        match got {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Return every conflict record under `<prefix>conflicts/` in
    /// ascending filename order. LIST + `get_many` shape, same as
    /// [`AgentFs::list_claims`].
    pub async fn list_conflicts(&self) -> anyhow::Result<Vec<ConflictRecord>> {
        let prefix = self.key("conflicts/");
        self.read_recent_json::<ConflictRecord>(&prefix, usize::MAX)
            .await
    }

    // ---- read-only navigation (pull surface) ---------------------------
    //
    // `read_file` / `list_dir` / `search` are the model's pull-navigation
    // primitives — the read half of the `Read`/`List`/`Search` actions.
    // They are always read-only and scoped to this `AgentFs`'s root via
    // `clean_relpath` (no `..`/absolute escape). A descendant-subtree read
    // is the same primitive called on an `AgentFs` opened at the child's
    // prefix (`open_for_agent`); the subtree *authorization* — which
    // descendants a parent may read — lives in the workflow layer where
    // topology is known, not here.

    /// Read the full UTF-8 contents of one file under the agent root.
    ///
    /// Read scope is the whole FS (`notes/`, `outputs/`, `evidence/`,
    /// `claims/`, `conflicts/`, `mandate.md`), unlike the write path which
    /// is confined to `notes/`. Returns [`FsError::FileNotFound`] when the
    /// path resolves to nothing. Bodies are decoded lossily — the FS only
    /// ever stores text/JSON the kernel wrote.
    pub async fn read_file(&self, path: &str) -> anyhow::Result<String> {
        let rel = self.clean_relpath(path)?;
        let key = self.key(&rel);
        let got = self
            .storage
            .get(&key)
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        match got {
            Some(bytes) => Ok(String::from_utf8_lossy(&bytes).into_owned()),
            None => Err(FsError::FileNotFound(path.to_string()).into()),
        }
    }

    /// List the entries directly under a directory in the agent root,
    /// ascending. Files appear as bare names; nested directories appear as
    /// a single `name/` marker (storage is flat, so this is derived from
    /// key prefixes). The `_tail.json` recency sidecar is filtered out. A
    /// directory with no entries lists empty rather than erroring — the
    /// model may probe a dir before anything is written to it.
    pub async fn list_dir(&self, path: &str) -> anyhow::Result<Vec<String>> {
        let rel = self.clean_relpath(path)?;
        let dir = if rel.is_empty() || rel.ends_with('/') {
            rel
        } else {
            format!("{rel}/")
        };
        let prefix = self.key(&dir);
        let page = self
            .storage
            .list(&prefix, None, usize::MAX)
            .await
            .map_err(|e| FsError::storage(&prefix, e))?;
        let mut names = std::collections::BTreeSet::new();
        for k in page.keys {
            let Some(rest) = k.strip_prefix(&prefix) else {
                continue;
            };
            if rest.is_empty() || rest == "_tail.json" {
                continue;
            }
            match rest.split_once('/') {
                Some((subdir, _)) => {
                    names.insert(format!("{subdir}/"));
                }
                None => {
                    names.insert(rest.to_string());
                }
            }
        }
        Ok(names.into_iter().collect())
    }

    /// Substring-search file contents under `path` (or the whole agent
    /// root when `None`), returning `(relative_path, first_matching_line)`
    /// per file that contains `query`. Recursive within the scope,
    /// read-only, case-sensitive. A cheap navigation aid, not a full-text
    /// index — one LIST plus one batched read of the scope.
    pub async fn search(
        &self,
        query: &str,
        path: Option<&str>,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let prefix = match path {
            Some(p) => {
                let rel = self.clean_relpath(p)?;
                let dir = if rel.is_empty() || rel.ends_with('/') {
                    rel
                } else {
                    format!("{rel}/")
                };
                self.key(&dir)
            }
            None => self.prefix.clone(),
        };
        let page = self
            .storage
            .list(&prefix, None, usize::MAX)
            .await
            .map_err(|e| FsError::storage(&prefix, e))?;
        let keys: Vec<String> = page
            .keys
            .into_iter()
            .filter(|k| !k.ends_with("/_tail.json"))
            .collect();
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let blobs = self
            .storage
            .get_many(&refs)
            .await
            .map_err(|e| FsError::storage(&prefix, e))?;
        let mut hits = Vec::new();
        for (key, blob) in keys.iter().zip(blobs) {
            let Some(bytes) = blob else { continue };
            let text = String::from_utf8_lossy(&bytes);
            if let Some(line) = text.lines().find(|l| l.contains(query)) {
                let rel = key.strip_prefix(&self.prefix).unwrap_or(key).to_string();
                hits.push((rel, line.trim().to_string()));
            }
        }
        hits.sort();
        Ok(hits)
    }

    /// The `outputs/` bucket for the cycle seed's index. The agent keeps a
    /// single canonical Output, so this is a one-file existence check, not a
    /// recency window: the canonical filename if it exists, else empty.
    pub async fn recent_output_filenames(&self, _n: usize) -> anyhow::Result<RecentWindow> {
        // Existence check only. The seed is rebuilt every cycle across many
        // agents, so list keys (no body transfer) rather than GET the
        // (possibly large) body just to test presence. `outputs/` holds only
        // the canonical file, so a one-key page settles it.
        let prefix = self.key("outputs/");
        let page = self
            .storage
            .list(&prefix, None, 1)
            .await
            .map_err(|e| FsError::storage(&prefix, e))?;
        let present = page
            .keys
            .iter()
            .any(|k| k.ends_with(CANONICAL_OUTPUT_FILENAME));
        let filenames = if present {
            vec![CANONICAL_OUTPUT_FILENAME.to_string()]
        } else {
            Vec::new()
        };
        Ok(RecentWindow {
            filenames,
            more: Remainder::None,
        })
    }

    /// Recency window of `notes/` filenames for the cycle seed's index.
    /// See [`AgentFs::recent_window`].
    pub async fn recent_note_filenames(&self, n: usize) -> anyhow::Result<RecentWindow> {
        self.recent_window(NOTES_TAIL_SUFFIX, "notes/", n).await
    }

    /// Up to `n` filenames under `indexed_prefix`, most-recent-first, plus how
    /// many lie beyond the window. Recency comes from the `_tail.json` sidecar;
    /// a missing or torn tail falls back to a lexicographic LIST — which loses
    /// strict recency but never the set. The `more` count is exact while the
    /// tail is sub-capacity (it is then the complete set) and a lower bound at
    /// capacity, so we never pay a full LIST just to count.
    async fn recent_window(
        &self,
        tail_suffix: &str,
        indexed_prefix: &str,
        n: usize,
    ) -> anyhow::Result<RecentWindow> {
        if n == 0 {
            return Ok(RecentWindow {
                filenames: Vec::new(),
                more: Remainder::None,
            });
        }
        let tail_key = self.key(tail_suffix);
        let tail_bytes = self
            .storage
            .get(&tail_key)
            .await
            .map_err(|e| FsError::storage(&tail_key, e))?;
        if let Some(bytes) = tail_bytes {
            if let Ok(tail) = serde_json::from_slice::<TailObject>(&bytes) {
                let take = tail.entries.len().min(n);
                let filenames = tail.entries[..take]
                    .iter()
                    .map(|e| e.filename.clone())
                    .collect();
                let more = if tail.entries.len() == TAIL_K {
                    // At capacity: the (TAIL_K - take) tail entries past the
                    // window are a hard floor, and older files may exist on
                    // disk beyond the tail — a lower bound, no LIST.
                    Remainder::AtLeast(TAIL_K - take)
                } else {
                    // Sub-capacity: the tail is the complete set, so exact.
                    match tail.entries.len() - take {
                        0 => Remainder::None,
                        k => Remainder::Exactly(k),
                    }
                };
                return Ok(RecentWindow { filenames, more });
            }
        }
        // LIST fallback already pays the listing, so the count is exact here.
        let mut all = self.list_dir(indexed_prefix).await?;
        let total = all.len();
        let start = total.saturating_sub(n);
        all.drain(..start);
        let more = match total - all.len() {
            0 => Remainder::None,
            k => Remainder::Exactly(k),
        };
        Ok(RecentWindow {
            filenames: all,
            more,
        })
    }

    /// Whether a file exists at `path` (relative to the agent root). One
    /// point GET; used by `build_seed` to decide whether to pin a standing
    /// note that has aged out of the recency window.
    pub async fn file_exists(&self, path: &str) -> anyhow::Result<bool> {
        let rel = self.clean_relpath(path)?;
        let key = self.key(&rel);
        Ok(self
            .storage
            .get(&key)
            .await
            .map_err(|e| FsError::storage(&key, e))?
            .is_some())
    }

    /// Validate `raw` as a read target relative to the agent root and
    /// return the cleaned, `/`-joined relative path (no storage prefix
    /// applied). Rejects any component that could escape the root (`..`,
    /// an absolute root, a Windows prefix). Read-only ops, so there is no
    /// write surface to confine beyond traversal safety.
    ///
    /// An empty component set (`.` or `""`) resolves to the empty relpath —
    /// the agent's own root. That is within the sandbox, not an escape, so
    /// listing or searching it is legitimate; a read of it is a clean
    /// not-found (the root is a prefix, not a file).
    fn clean_relpath(&self, raw: &str) -> anyhow::Result<String> {
        let candidate = Path::new(raw);
        let mut parts: Vec<String> = Vec::new();
        for comp in candidate.components() {
            match comp {
                Component::Normal(part) => match part.to_str() {
                    Some(s) => parts.push(s.to_string()),
                    None => return Err(FsError::PathTraversal(raw.to_string()).into()),
                },
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(FsError::PathTraversal(raw.to_string()).into());
                }
            }
        }
        Ok(parts.join("/"))
    }

    // ---- key construction ----------------------------------------------

    fn key(&self, tail: &str) -> String {
        if self.prefix.is_empty() {
            tail.to_string()
        } else {
            format!("{}{tail}", self.prefix)
        }
    }

    fn claim_key(&self, seed: &str) -> String {
        self.key(&format!("claims/{}.json", claim_slug(seed)))
    }

    fn conflict_key(&self, id: &ConflictId) -> String {
        self.key(&format!("conflicts/{}.json", id))
    }

    /// Prepend `filename` to the tail object at `<prefix><tail_suffix>`
    /// and truncate to [`TAIL_K`].
    ///
    /// Plain GET-modify-PUT without CAS: single-writer-per-agent is
    /// the engine-wide contract. A failed tail-PUT after a successful
    /// object-PUT leaves the tail lagging; the reader's
    /// `read_recent_window_with_tail` detects this and falls back to
    /// LIST.
    async fn append_to_tail(&self, tail_suffix: &str, filename: String) -> anyhow::Result<()> {
        let key = self.key(tail_suffix);
        let existing = self
            .storage
            .get(&key)
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        let mut tail: TailObject = match existing {
            Some(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            None => TailObject::default(),
        };
        // Defensive dedup: drop any prior entry for the same filename
        // so the newest position is the only one we keep.
        tail.entries.retain(|e| e.filename != filename);
        tail.entries.insert(
            0,
            TailEntry {
                filename,
                added_at: Utc::now(),
            },
        );
        if tail.entries.len() > TAIL_K {
            tail.entries.truncate(TAIL_K);
        }
        let bytes = serde_json::to_vec(&tail)?;
        self.storage
            .put(&key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        Ok(())
    }

    /// Drop `filename` from the tail object at `<prefix><tail_suffix>`, if
    /// present. For `notes/`, where files are deletable; no-op (no PUT) when
    /// the tail is absent or the filename was not tracked.
    async fn remove_from_tail(&self, tail_suffix: &str, filename: &str) -> anyhow::Result<()> {
        let key = self.key(tail_suffix);
        let Some(bytes) = self
            .storage
            .get(&key)
            .await
            .map_err(|e| FsError::storage(&key, e))?
        else {
            return Ok(());
        };
        let mut tail: TailObject = serde_json::from_slice(&bytes).unwrap_or_default();
        let before = tail.entries.len();
        tail.entries.retain(|e| e.filename != filename);
        if tail.entries.len() == before {
            return Ok(());
        }
        let bytes = serde_json::to_vec(&tail)?;
        self.storage
            .put(&key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        Ok(())
    }

    /// Tail-fast-path read for `outputs/` or `evidence/` recent windows.
    ///
    /// `prefix` is the indexed prefix. `tail_suffix` is the tail
    /// object's relative key. `lex_monotonic` indicates whether "most
    /// recently written" coincides with "lex-greatest" under this
    /// prefix's naming scheme — `false` for sha256-addressed prefixes,
    /// kept as a parameter for future monotonic schemes.
    ///
    /// Decision matrix:
    ///
    /// - `n == 0` → empty, no I/O.
    /// - Tail missing → fall back to LIST.
    /// - Tail length `< TAIL_K` → tail is complete; use it regardless
    ///   of `lex_monotonic`.
    /// - Tail length `== TAIL_K` AND `lex_monotonic` AND `n <= TAIL_K`
    ///   → use tail.
    /// - Otherwise → fall back to LIST.
    async fn read_recent_window_with_tail<T>(
        &self,
        prefix: &str,
        tail_suffix: &str,
        n: usize,
        lex_monotonic: bool,
    ) -> anyhow::Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        if n == 0 {
            return Ok(Vec::new());
        }
        let tail_key = self.key(tail_suffix);
        let tail_bytes = self
            .storage
            .get(&tail_key)
            .await
            .map_err(|e| FsError::storage(&tail_key, e))?;
        if let Some(bytes) = tail_bytes {
            // A serde error on the tail falls back to LIST — torn
            // writes or schema drift should slow recent-N assembly,
            // not break it.
            let parsed: Result<TailObject, _> = serde_json::from_slice(&bytes);
            if let Ok(tail) = parsed {
                let tail_complete = tail.entries.len() < TAIL_K;
                let fast_path_safe = tail_complete || (lex_monotonic && n <= TAIL_K);
                if fast_path_safe {
                    return self.read_keys_for_tail::<T>(prefix, &tail, n).await;
                }
            }
        }
        // LIST fallback covers: missing tail, capacity-bound tail
        // with non-monotonic filenames, `n > TAIL_K`, and recovery
        // when a tail update failed mid-flight.
        self.read_recent_json::<T>(prefix, n).await
    }

    /// Materialise the trailing-`n` slice of `tail` (newest at index
    /// 0) and return values in ascending filename order. Missing files
    /// in the window are dropped silently, matching the LIST path's
    /// forgiveness for out-of-band deletes.
    async fn read_keys_for_tail<T>(
        &self,
        prefix: &str,
        tail: &TailObject,
        n: usize,
    ) -> anyhow::Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        // Tail is reverse-chronological; the first `n` entries are
        // the most recent. Re-sort ascending so the returned vector
        // matches the lex-sort semantics the caller expects.
        let take_n = tail.entries.len().min(n);
        let mut filenames: Vec<String> = tail.entries[..take_n]
            .iter()
            .map(|e| e.filename.clone())
            .collect();
        filenames.sort();
        if filenames.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<String> = filenames.iter().map(|f| format!("{prefix}{f}")).collect();
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let blobs = self
            .storage
            .get_many(&refs)
            .await
            .map_err(|e| FsError::storage(prefix, e))?;
        let mut out = Vec::with_capacity(blobs.len());
        for (key, blob) in keys.iter().zip(blobs.into_iter()) {
            let bytes = match blob {
                Some(b) => b,
                None => {
                    tracing::debug!(
                        key = key.as_str(),
                        "tail entry resolves to absent key; skipping"
                    );
                    continue;
                }
            };
            out.push(serde_json::from_slice::<T>(&bytes)?);
        }
        Ok(out)
    }

    /// Lex-sort every `.json` key under `prefix`, take the last `n`,
    /// fetch and deserialise. One `list` plus a single `get_many` so
    /// a remote backend pays one round-trip rather than N. O(M) under
    /// the prefix; the tail-index fast path covers the common case.
    async fn read_recent_json<T>(&self, prefix: &str, n: usize) -> anyhow::Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        // `usize::MAX` asks the backend for everything in one page;
        // both `MemoryStorage` and `LocalStorage` honour that without
        // pagination.
        let page = self
            .storage
            .list(prefix, None, usize::MAX)
            .await
            .map_err(|e| FsError::storage(prefix, e))?;
        // Keep only `.json` keys and drop the `_tail.json` sidecar so
        // a fallback LIST does not try to deserialise a `TailObject`
        // as the record type.
        let mut keys: Vec<String> = page
            .keys
            .into_iter()
            .filter(|k| k.ends_with(".json"))
            .filter(|k| !k.ends_with("/_tail.json"))
            .collect();
        keys.sort();
        let start = keys.len().saturating_sub(n);
        let window: Vec<String> = keys.into_iter().skip(start).collect();

        if window.is_empty() {
            return Ok(Vec::new());
        }
        let refs: Vec<&str> = window.iter().map(String::as_str).collect();
        let blobs = self
            .storage
            .get_many(&refs)
            .await
            .map_err(|e| FsError::storage(prefix, e))?;
        let mut out = Vec::with_capacity(blobs.len());
        for (key, blob) in window.iter().zip(blobs.into_iter()) {
            let bytes = match blob {
                Some(b) => b,
                // Key vanished between list and get_many; treat as absent.
                None => {
                    tracing::debug!(key = key.as_str(), "key absent between list and get_many");
                    continue;
                }
            };
            let value: T = serde_json::from_slice(&bytes)?;
            out.push(value);
        }
        Ok(out)
    }

    /// Resolve `raw` to a storage key under `<prefix>notes/`. Paths
    /// must be relative, rooted at `notes/`, contain only normal
    /// components (no `..`, no root, no Windows prefix), and name a
    /// file inside `notes/` (not `notes/` itself).
    ///
    /// Walks `Components` rather than calling `Path::canonicalize`
    /// because write targets may not exist yet, which canonicalize
    /// treats as an error. Symlinks pointing out of `notes/` are a
    /// known follow-up.
    fn resolve_notes_key(&self, raw: &str) -> anyhow::Result<String> {
        let candidate = Path::new(raw);

        // First pass: only Normal components and `.` are allowed.
        let mut cleaned = PathBuf::new();
        for comp in candidate.components() {
            match comp {
                Component::Normal(part) => cleaned.push(part),
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(FsError::PathTraversal(raw.to_string()).into());
                }
            }
        }
        if cleaned.as_os_str().is_empty() {
            return Err(FsError::PathTraversal(raw.to_string()).into());
        }

        // Second pass: must be rooted at `notes/`.
        let tail = match cleaned.strip_prefix("notes") {
            Ok(rest) if !rest.as_os_str().is_empty() => rest.to_path_buf(),
            Ok(_) => return Err(FsError::PathOutsideNotes(raw.to_string()).into()),
            Err(_) => return Err(FsError::PathOutsideNotes(raw.to_string()).into()),
        };

        // Re-emit as a `/`-joined key under `<prefix>notes/`. The
        // first pass already filtered to Normal components; non-UTF-8
        // surfaces as PathTraversal (no other reasonable mapping).
        let mut parts = Vec::new();
        for comp in tail.components() {
            match comp {
                Component::Normal(part) => match part.to_str() {
                    Some(s) => parts.push(s.to_string()),
                    None => return Err(FsError::PathTraversal(raw.to_string()).into()),
                },
                _ => return Err(FsError::PathTraversal(raw.to_string()).into()),
            }
        }
        let joined = parts.join("/");
        Ok(self.key(&format!("notes/{joined}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decision::FsOp;
    use crate::evidence::EvidenceRecord;
    use crate::mandate::Mandate;
    use crate::storage::MemoryStorage;
    use chrono::Utc;
    use serde_json::json;
    use std::time::Duration;
    use tempfile::TempDir;

    async fn fresh_fs() -> (TempDir, AgentFs, Mandate) {
        let tmp = TempDir::new().unwrap();
        let mandate = Mandate::new("research foo", Duration::from_millis(1000), Some(10));
        let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
            .await
            .unwrap();
        (tmp, fs, mandate)
    }

    fn record(tool: &str, args: serde_json::Value, result: serde_json::Value) -> EvidenceRecord {
        EvidenceRecord::new(tool, args, result, Utc::now())
    }

    #[tokio::test]
    async fn open_writes_mandate_as_pure_prose_md() {
        let (tmp, _fs, mandate) = fresh_fs().await;
        let root = tmp.path();
        // mandate.md present
        assert!(root.join("mandate.md").is_file());
        // Body is exactly the prose; no JSON wrapper, no metadata fields
        // (idle_period / step_cap / etc.) leak into the file.
        let body = std::fs::read_to_string(root.join("mandate.md")).unwrap();
        assert_eq!(body, mandate.text);
        assert!(!body.contains('{'), "mandate.md must not be JSON: {body:?}");
        assert!(
            !body.contains("idle_period") && !body.contains("step_cap"),
            "mandate config must not leak into the file body: {body:?}"
        );
    }

    #[tokio::test]
    async fn open_is_idempotent_and_does_not_clobber_mandate() {
        let tmp = TempDir::new().unwrap();
        let original = Mandate::new("first", Duration::from_millis(500), None);
        let _fs = AgentFs::open(tmp.path().to_path_buf(), &original)
            .await
            .unwrap();

        // Re-open with a *different* mandate; the on-disk file must keep
        // the original prose.
        let other = Mandate::new("second", Duration::from_millis(999), Some(7));
        let _fs2 = AgentFs::open(tmp.path().to_path_buf(), &other)
            .await
            .unwrap();

        let body = std::fs::read_to_string(tmp.path().join("mandate.md")).unwrap();
        assert_eq!(body, original.text);
    }

    #[test]
    fn slug_is_interpretable_kebab_without_hash() {
        assert_eq!(slug("TSMC CoWoS Capacity"), "tsmc-cowos-capacity");
        // Runs of non-alphanumerics collapse; ends are trimmed.
        assert_eq!(slug("  Foo / Bar -- baz!! "), "foo-bar-baz");
        // No alphanumerics => empty (the writer must disambiguate).
        assert_eq!(slug("!!! ---"), "");
        // No hash suffix — a plain interpretable name.
        assert!(!slug("hello world").contains(|c: char| c.is_ascii_hexdigit() && c == '-'));
        assert_eq!(slug("hello world"), "hello-world");
    }

    #[test]
    fn slug_truncates_long_bodies_at_boundary() {
        let long = "a".repeat(200);
        let s = slug(&long);
        assert!(s.len() <= SLUG_BODY_MAX);
        assert!(s.chars().all(|c| c == 'a'));
    }

    #[test]
    fn claim_slug_is_slug_plus_stable_hash_suffix() {
        // claim_slug builds on slug() + an unconditional 8-hex suffix.
        let cs = claim_slug("TSMC CoWoS Capacity");
        assert!(cs.starts_with("tsmc-cowos-capacity-"));
        let suffix = cs.rsplit('-').next().unwrap();
        assert_eq!(suffix.len(), 8);
        assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()));
        // Deterministic in the seed.
        assert_eq!(cs, claim_slug("TSMC CoWoS Capacity"));
        // Empty body => just the hash suffix.
        let only_suffix = claim_slug("!!!");
        assert_eq!(only_suffix.len(), 8);
        assert!(only_suffix.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn record_evidence_uses_interpretable_path_and_is_dedup_safe() {
        let (tmp, fs, _m) = fresh_fs().await;
        let rec = record("echo", json!({"msg": "hi"}), json!({"echoed": "hi"}));
        let path = fs.record_evidence(rec.clone(), "TSMC CoWoS").await.unwrap();

        // The returned handle is an interpretable slug under evidence/ with a
        // content-hash suffix — not a bare sha filename.
        assert!(path.starts_with("evidence/tsmc-cowos-"), "got {path}");
        assert!(path.ends_with(".json"), "got {path}");
        assert!(tmp.path().join(&path).is_file());

        // Second write of an identical record under the same seed is a
        // no-op — same path, no duplicate, one entry.
        let path2 = fs.record_evidence(rec.clone(), "TSMC CoWoS").await.unwrap();
        assert_eq!(path, path2);

        // Count evidence record files only — `_tail.json` is the
        // tail-index sidecar, not an evidence record.
        let count = |tmp: &tempfile::TempDir| {
            std::fs::read_dir(tmp.path().join("evidence"))
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
                .filter(|n| n != "_tail.json")
                .count()
        };
        assert_eq!(
            count(&tmp),
            1,
            "duplicate evidence write created extra file"
        );

        // Different content under the SAME seed disambiguates by the content
        // hash suffix — a distinct path, a second file (no silent overwrite).
        let other = record("echo", json!({"msg": "bye"}), json!({"echoed": "bye"}));
        let other_path = fs.record_evidence(other, "TSMC CoWoS").await.unwrap();
        assert_ne!(path, other_path);
        assert!(
            other_path.starts_with("evidence/tsmc-cowos-"),
            "got {other_path}"
        );
        assert_eq!(count(&tmp), 2);
    }

    #[tokio::test]
    async fn record_evidence_falls_back_to_hash_when_seed_has_no_slug() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let rec = record("echo", json!({"msg": "hi"}), json!({"echoed": "hi"}));
        // A seed with no alphanumerics yields an empty slug body, so the
        // path is just the content-hash suffix — still under evidence/.
        let path = fs.record_evidence(rec, "!!! ---").await.unwrap();
        assert!(path.starts_with("evidence/"), "got {path}");
        assert!(path.ends_with(".json"), "got {path}");
        assert!(!path.starts_with("evidence/-"), "no leading dash: {path}");
    }

    #[tokio::test]
    async fn persist_output_rejects_empty_evidence() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let err = fs.persist_output("hello", &[]).await.unwrap_err();
        let downcast = err.downcast_ref::<FsError>().expect("typed FsError");
        assert!(matches!(downcast, FsError::EmptyEvidence));
    }

    #[tokio::test]
    async fn persist_output_rejects_unknown_citation_path() {
        let (tmp, fs, _m) = fresh_fs().await;
        let bogus = "evidence/never-written-deadbeef.json".to_string();
        let err = fs
            .persist_output("hello", &[bogus.clone()])
            .await
            .unwrap_err();
        let downcast = err.downcast_ref::<FsError>().expect("typed FsError");
        match downcast {
            FsError::EvidenceNotFound(missing) => assert_eq!(missing, &bogus),
            other => panic!("expected EvidenceNotFound, got {other:?}"),
        }
        // No output file should have been written — the outputs dir
        // also won't have been created since the write never happened.
        let outputs_dir = tmp.path().join("outputs");
        if outputs_dir.exists() {
            let entries: Vec<_> = std::fs::read_dir(outputs_dir).unwrap().collect();
            assert!(entries.is_empty());
        }
    }

    #[tokio::test]
    async fn persist_output_rejects_citation_outside_evidence() {
        let (_tmp, fs, _m) = fresh_fs().await;
        // A citation must point at a runtime-authored evidence record. A path
        // the model could hand-write itself (under notes/) is rejected even
        // when the file exists.
        fs.apply_ops(vec![FsOp::WriteFile {
            path: "notes/forged.md".into(),
            content: "fake".into(),
        }])
        .await
        .unwrap();
        let err = fs
            .persist_output("hello", &["notes/forged.md".to_string()])
            .await
            .unwrap_err();
        let downcast = err.downcast_ref::<FsError>().expect("typed FsError");
        assert!(matches!(downcast, FsError::CitationNotEvidence(_)));
    }

    #[tokio::test]
    async fn persist_output_rejects_citation_to_evidence_tail_sidecar() {
        let (_tmp, fs, _m) = fresh_fs().await;
        // The recency sidecar lives under evidence/ but is not an evidence
        // record — citing it is rejected at the gate, not deferred to a parse
        // failure downstream.
        fs.record_evidence(record("echo", json!({"k": 1}), json!({"v": 1})), "echo")
            .await
            .unwrap();
        let err = fs
            .persist_output("hello", &["evidence/_tail.json".to_string()])
            .await
            .unwrap_err();
        let downcast = err.downcast_ref::<FsError>().expect("typed FsError");
        assert!(matches!(downcast, FsError::CitationNotEvidence(_)));
    }

    #[tokio::test]
    async fn persist_output_writes_canonical_file_referencing_evidence() {
        let (tmp, fs, _m) = fresh_fs().await;
        let rec = record("echo", json!({"msg": "hi"}), json!({"echoed": "hi"}));
        let path = fs.record_evidence(rec, "echo hi").await.unwrap();

        let out_id = fs.persist_output("hello", &[path.clone()]).await.unwrap();
        assert_eq!(out_id, OutputId::new("hello"));

        // The single canonical Output lands at the stable path.
        let path = tmp.path().join("outputs").join("output.md");
        assert!(path.is_file());
        let body = std::fs::read_to_string(&path).unwrap();
        assert_eq!(body, "hello");
    }

    // ---- read_output + open_for_agent ----

    #[tokio::test]
    async fn read_output_returns_persisted_body() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let rec = record("echo", json!({"q": "k"}), json!({"r": "v"}));
        let ev = fs.record_evidence(rec, "echo k").await.unwrap();
        let _ = fs.persist_output("the claim", &[ev.clone()]).await.unwrap();

        let back = fs.read_output().await.unwrap();
        assert_eq!(
            back, "the claim",
            "read_output must return the persisted body"
        );
    }

    #[tokio::test]
    async fn read_output_returns_typed_error_when_none_written() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let err = fs
            .read_output()
            .await
            .expect_err("missing output must error");
        let typed = err.downcast_ref::<FsError>().expect("typed FsError");
        assert!(matches!(typed, FsError::OutputNotFound));
    }

    #[tokio::test]
    async fn open_for_agent_scopes_storage_to_workflow_id_prefix() {
        use crate::agent_ref::{AgentId, GraphId};
        use crate::storage::MemoryStorage;
        use uuid::Uuid;

        let storage: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
        let graph_id =
            GraphId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap());
        let agent_id =
            AgentId::new(Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap());
        let fs = AgentFs::open_for_agent(storage.clone(), graph_id, agent_id);
        assert_eq!(
            fs.prefix(),
            "graphs/11111111-2222-3333-4444-555555555555/agents/66666666-7777-8888-9999-aaaaaaaaaaaa/",
            "prefix must match the flat workflow-id scheme",
        );

        // Writing evidence under this prefix and reading it back
        // works — cross-agent reads use exactly this shape.
        let rec = record("echo", json!({"x": 1}), json!({"y": 2}));
        let relpath = fs.record_evidence(rec.clone(), "echo x").await.unwrap();
        let key = format!("graphs/{}/agents/{}/{}", graph_id, agent_id, relpath);
        assert!(
            storage.get(&key).await.unwrap().is_some(),
            "evidence must land at the prefixed key",
        );
    }

    #[tokio::test]
    async fn open_for_agent_supports_cross_agent_output_read() {
        // Models the reconcile path: a parent's FS scoped to its own
        // prefix opens a child's FS over the same storage backend
        // (different prefix) and reads the child's output.
        use crate::agent_ref::{AgentId, GraphId};
        use crate::storage::MemoryStorage;
        use uuid::Uuid;

        let storage: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
        let graph_id = GraphId::new(Uuid::new_v4());
        let child_agent_id = AgentId::new(Uuid::new_v4());

        // Child writes an output via its own FS (mandate present).
        let child_mandate = Mandate::new("child", Duration::from_millis(100), None);
        let child_prefix = format!("graphs/{}/agents/{}/", graph_id, child_agent_id);
        let child_fs = AgentFs::new_with_storage(storage.clone(), &child_prefix, &child_mandate)
            .await
            .unwrap();
        let ev = child_fs
            .record_evidence(
                record("echo", json!({"q": "child"}), json!({"r": 1})),
                "child q",
            )
            .await
            .unwrap();
        let _ = child_fs
            .persist_output("child's claim", &[ev])
            .await
            .unwrap();

        // Parent uses `open_for_agent` (no mandate) to read the
        // child's canonical output — the cross-agent reconcile surface.
        let parent_view = AgentFs::open_for_agent(storage, graph_id, child_agent_id);
        let read_back = parent_view.read_output().await.unwrap();
        assert_eq!(read_back, "child's claim");
    }

    #[tokio::test]
    async fn apply_ops_writes_under_notes() {
        let (tmp, fs, _m) = fresh_fs().await;
        fs.apply_ops(vec![FsOp::WriteFile {
            path: "notes/a.md".into(),
            content: "hi".into(),
        }])
        .await
        .unwrap();
        let written = tmp.path().join("notes").join("a.md");
        assert_eq!(std::fs::read_to_string(&written).unwrap(), "hi");

        // Nested subdirectory under notes/ is created on demand.
        fs.apply_ops(vec![FsOp::WriteFile {
            path: "notes/sub/c.md".into(),
            content: "deep".into(),
        }])
        .await
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(tmp.path().join("notes").join("sub").join("c.md")).unwrap(),
            "deep"
        );

        // DeleteFile removes the file and is idempotent on missing paths.
        fs.apply_ops(vec![FsOp::DeleteFile {
            path: "notes/a.md".into(),
        }])
        .await
        .unwrap();
        assert!(!written.exists());
        fs.apply_ops(vec![FsOp::DeleteFile {
            path: "notes/never-existed.md".into(),
        }])
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn apply_ops_rejects_path_traversal() {
        let (tmp, fs, _m) = fresh_fs().await;
        for bad in ["../etc/passwd", "../../escape", "notes/../../escape"] {
            let err = fs
                .apply_ops(vec![FsOp::WriteFile {
                    path: bad.into(),
                    content: "x".into(),
                }])
                .await
                .unwrap_err();
            let downcast = err.downcast_ref::<FsError>().expect("typed FsError");
            assert!(
                matches!(downcast, FsError::PathTraversal(_)),
                "expected PathTraversal for {bad}, got {downcast:?}"
            );
        }

        // Absolute paths are rejected too.
        let err = fs
            .apply_ops(vec![FsOp::WriteFile {
                path: "/etc/passwd".into(),
                content: "x".into(),
            }])
            .await
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<FsError>().unwrap(),
            FsError::PathTraversal(_)
        ));

        // A syntactically clean path that resolves outside notes/ is also
        // rejected — we only allow writes under notes/.
        let err = fs
            .apply_ops(vec![FsOp::WriteFile {
                path: "outputs/forged.json".into(),
                content: "x".into(),
            }])
            .await
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<FsError>().unwrap(),
            FsError::PathOutsideNotes(_)
        ));

        // None of the rejected ops should have produced files anywhere.
        let outputs_dir = tmp.path().join("outputs");
        if outputs_dir.exists() {
            let entries: Vec<_> = std::fs::read_dir(outputs_dir).unwrap().collect();
            assert!(entries.is_empty());
        }
    }

    #[tokio::test]
    async fn list_dir_lists_root_via_dot_and_empty() {
        let (_tmp, fs, _m) = fresh_fs().await;
        fs.apply_ops(vec![FsOp::WriteFile {
            path: "notes/a.md".into(),
            content: "x".into(),
        }])
        .await
        .unwrap();
        for root in [".", ""] {
            let names = fs
                .list_dir(root)
                .await
                .unwrap_or_else(|e| panic!("listing root via {root:?} should succeed: {e:#}"));
            assert!(
                names.iter().any(|n| n == "mandate.md"),
                "root {root:?} listing should include mandate.md, got {names:?}"
            );
            assert!(
                names.iter().any(|n| n == "notes/"),
                "root {root:?} listing should include notes/, got {names:?}"
            );
        }
    }

    #[tokio::test]
    async fn list_dir_rejects_real_escape() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let err = fs.list_dir("../outside").await.unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<FsError>(),
                Some(FsError::PathTraversal(_))
            ),
            "an escaping list path must still be rejected"
        );
    }

    #[tokio::test]
    async fn apply_ops_is_atomic_against_a_bad_path_in_the_middle() {
        let (tmp, fs, _m) = fresh_fs().await;
        let err = fs
            .apply_ops(vec![
                FsOp::WriteFile {
                    path: "notes/good.md".into(),
                    content: "ok".into(),
                },
                FsOp::WriteFile {
                    path: "../escape".into(),
                    content: "bad".into(),
                },
            ])
            .await
            .unwrap_err();
        assert!(err.downcast_ref::<FsError>().is_some());
        // Pre-flight validation rejects the batch before any write.
        assert!(!tmp.path().join("notes").join("good.md").exists());
    }

    async fn write_note(fs: &AgentFs, name: &str) {
        fs.apply_ops(vec![FsOp::WriteFile {
            path: format!("notes/{name}"),
            content: format!("body of {name}"),
        }])
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn notes_recency_window_is_most_recent_first_and_signposts_more() {
        let (_tmp, fs, _m) = fresh_fs().await;
        for name in ["a.md", "b.md", "c.md"] {
            write_note(&fs, name).await;
        }
        let all = fs.recent_note_filenames(8).await.unwrap();
        assert_eq!(
            all.filenames,
            vec!["c.md", "b.md", "a.md"],
            "notes must surface most-recent-first"
        );
        assert_eq!(all.more, Remainder::None, "the whole set fits the window");

        let windowed = fs.recent_note_filenames(2).await.unwrap();
        assert_eq!(windowed.filenames, vec!["c.md", "b.md"]);
        assert_eq!(
            windowed.more,
            Remainder::Exactly(1),
            "a sub-capacity tail yields an exact overflow count"
        );
    }

    #[tokio::test]
    async fn notes_recency_window_reports_a_lower_bound_at_tail_capacity() {
        let (_tmp, fs, _m) = fresh_fs().await;
        for i in 0..(TAIL_K + 3) {
            write_note(&fs, &format!("n-{i:03}.md")).await;
        }
        let got = fs.recent_note_filenames(8).await.unwrap();
        assert_eq!(got.filenames.len(), 8);
        assert_eq!(
            got.more,
            Remainder::AtLeast(TAIL_K - 8),
            "at tail capacity the count is a lower bound, not exact"
        );
    }

    #[tokio::test]
    async fn apply_ops_delete_prunes_the_notes_tail() {
        let (_tmp, fs, _m) = fresh_fs().await;
        for name in ["a.md", "b.md", "c.md"] {
            write_note(&fs, name).await;
        }
        fs.apply_ops(vec![FsOp::DeleteFile {
            path: "notes/b.md".into(),
        }])
        .await
        .unwrap();
        let got = fs.recent_note_filenames(8).await.unwrap();
        assert_eq!(
            got.filenames,
            vec!["c.md", "a.md"],
            "a deleted note must drop out of the recency tail"
        );
    }

    #[tokio::test]
    async fn apply_ops_rewrite_bumps_note_recency() {
        let (_tmp, fs, _m) = fresh_fs().await;
        write_note(&fs, "a.md").await;
        write_note(&fs, "b.md").await;
        // Re-writing a.md should move it back to the front of recency.
        write_note(&fs, "a.md").await;
        let got = fs.recent_note_filenames(8).await.unwrap();
        assert_eq!(
            got.filenames,
            vec!["a.md", "b.md"],
            "rewriting a note bumps it to most-recent without duplicating it"
        );
    }

    #[tokio::test]
    async fn apply_ops_rejects_writes_to_the_reserved_notes_tail_sidecar() {
        let (tmp, fs, _m) = fresh_fs().await;
        let err = fs
            .apply_ops(vec![
                FsOp::WriteFile {
                    path: "notes/good.md".into(),
                    content: "ok".into(),
                },
                FsOp::WriteFile {
                    path: "notes/_tail.json".into(),
                    content: "forged-index".into(),
                },
            ])
            .await
            .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<FsError>().unwrap(),
                FsError::ReservedNotesPath(_)
            ),
            "writing the recency sidecar must be rejected"
        );
        // Atomic: the good op in the same batch must not have landed.
        assert!(!tmp.path().join("notes").join("good.md").exists());
    }

    #[tokio::test]
    async fn notes_tail_reconciles_after_a_lagging_write() {
        let tmp = TempDir::new().unwrap();
        let mandate = Mandate::new("reconcile-notes", Duration::from_millis(100), Some(1));
        {
            let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
                .await
                .unwrap();
            write_note(&fs, "a.md").await;
        }
        // Out-of-band note write that never updated the tail (crash mid-PUT).
        let storage = Arc::new(LocalStorage::new(tmp.path().to_path_buf()).unwrap());
        storage
            .put("notes/orphan.md", Bytes::from_static(b"orphan"))
            .await
            .unwrap();
        // Re-open: open-time reconcile rebuilds the notes tail from the LIST.
        let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
            .await
            .unwrap();
        let got = fs.recent_note_filenames(8).await.unwrap();
        assert!(
            got.filenames.iter().any(|f| f == "orphan.md"),
            "an orphan note must surface after open-time reconcile; got {:?}",
            got.filenames
        );
        assert!(got.filenames.iter().any(|f| f == "a.md"));
    }

    #[tokio::test]
    async fn recent_note_filenames_falls_back_to_list_when_tail_absent() {
        let (_tmp, fs, _m) = fresh_fs().await;
        write_note(&fs, "a.md").await;
        write_note(&fs, "b.md").await;
        // Delete the sidecar without reopening, forcing the LIST fallback.
        fs.storage().delete("notes/_tail.json").await.unwrap();
        let got = fs.recent_note_filenames(8).await.unwrap();
        let mut names = got.filenames.clone();
        names.sort();
        assert_eq!(
            names,
            vec!["a.md", "b.md"],
            "the LIST fallback must recover the full note set"
        );
        assert_eq!(got.more, Remainder::None);
    }

    #[tokio::test]
    async fn file_exists_reports_presence() {
        let (_tmp, fs, _m) = fresh_fs().await;
        assert!(!fs.file_exists("notes/STATUS.md").await.unwrap());
        write_note(&fs, "STATUS.md").await;
        assert!(fs.file_exists("notes/STATUS.md").await.unwrap());
    }

    #[tokio::test]
    async fn authorship_boundary_rejects_model_writes_to_evidence() {
        let (tmp, fs, _m) = fresh_fs().await;
        // A model-driven op targeting evidence/ is refused, and nothing
        // lands under evidence/. Asserted mechanism-agnostically (just
        // `is_err` + untouched dir) so this stays an authorship-boundary
        // test even if the model's writable surface is later widened.
        for op in [
            FsOp::WriteFile {
                path: "evidence/forged.md".into(),
                content: "the model wrote this".into(),
            },
            FsOp::DeleteFile {
                path: "evidence/anything.md".into(),
            },
        ] {
            assert!(
                fs.apply_ops(vec![op]).await.is_err(),
                "model op against evidence/ must be rejected"
            );
        }
        let evidence_dir = tmp.path().join("evidence");
        if evidence_dir.exists() {
            let entries: Vec<_> = std::fs::read_dir(&evidence_dir).unwrap().collect();
            assert!(
                entries.is_empty(),
                "no model write may land under evidence/"
            );
        }
    }

    #[tokio::test]
    async fn authorship_boundary_runtime_writes_evidence_and_model_reads_it() {
        let (_tmp, fs, _m) = fresh_fs().await;
        // The runtime tool-observation path writes evidence...
        let path = fs
            .record_evidence(
                record("web_search", json!({"q": "cowos"}), json!({"hits": 1})),
                "cowos search",
            )
            .await
            .unwrap();
        fs.evidence_must_exist(&path).await.unwrap();

        // ...and the model's read surface returns it (reads are ungated).
        let recent = fs.list_recent_evidence(8).await.unwrap();
        assert!(
            recent.iter().any(|r| path.contains(&r.id.as_str()[..8])),
            "model must be able to read runtime-authored evidence"
        );
    }

    /// `persist_output` keeps ONE canonical Output. The same body PUTs
    /// byte-identical bytes (same `OutputId`); a different body overwrites
    /// the one file in place — `read_output` returns the latest, never an
    /// accumulating history.
    #[tokio::test]
    async fn persist_output_keeps_one_canonical_output_overwritten_in_place() {
        let (tmp, fs, _m) = fresh_fs().await;
        let cite = fs
            .record_evidence(record("echo", json!({"k": 1}), json!({"v": 1})), "echo k")
            .await
            .unwrap();

        let first = fs
            .persist_output("the same claim", &[cite.clone()])
            .await
            .unwrap();
        let second = fs
            .persist_output("the same claim", &[cite.clone()])
            .await
            .unwrap();

        // Same body → same content-addressed OutputId.
        assert_eq!(first, second);

        // Exactly one file under outputs/ — the canonical Output.
        let output_files: Vec<_> = std::fs::read_dir(tmp.path().join("outputs"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(output_files, vec!["output.md".to_string()]);
        assert_eq!(fs.read_output().await.unwrap(), "the same claim");

        // A *different* body overwrites the same file (still one), and
        // mints a fresh id.
        let third = fs
            .persist_output("a different claim", &[cite.clone()])
            .await
            .unwrap();
        assert_ne!(third, first);
        let output_files: Vec<_> = std::fs::read_dir(tmp.path().join("outputs"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(output_files, vec!["output.md".to_string()]);
        assert_eq!(fs.read_output().await.unwrap(), "a different claim");
    }

    #[tokio::test]
    async fn list_recent_evidence_returns_window_in_filename_order() {
        let (_tmp, fs, _m) = fresh_fs().await;
        for i in 0..10 {
            fs.record_evidence(record("echo", json!({ "i": i }), json!({ "i": i })), "echo")
                .await
                .unwrap();
        }
        let recent = fs.list_recent_evidence(8).await.unwrap();
        assert_eq!(recent.len(), 8);

        // Filenames are `echo-<first 8 hex of id>.json`; their order is the
        // first-8-hex order, which (for distinct prefixes) matches sorting the
        // full ids — the window stays deterministic.
        let prefixes: Vec<_> = recent
            .iter()
            .map(|r| r.id.as_str()[..8].to_string())
            .collect();
        let mut sorted = prefixes.clone();
        sorted.sort();
        assert_eq!(prefixes, sorted, "evidence not returned in filename order");
    }

    #[tokio::test]
    async fn persist_retirement_writes_file() {
        let (tmp, fs, _m) = fresh_fs().await;
        let pinned = DateTime::parse_from_rfc3339("2026-05-24T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        fs.persist_retirement("done", pinned).await.unwrap();
        let path = tmp.path().join("retirement.json");
        assert!(path.is_file());
        let v: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(v.get("reason").and_then(|x| x.as_str()), Some("done"));
        // `retired_at` is the caller-supplied timestamp, exactly — pin
        // it so a regression that re-stamps `Utc::now()` internally
        // (defeating workflow-replay determinism) fails loudly.
        // chrono's serde format for `DateTime<Utc>` emits `Z` for the
        // UTC offset (compact RFC 3339 form), not `+00:00`.
        assert_eq!(
            v.get("retired_at").and_then(|x| x.as_str()),
            Some("2026-05-24T12:00:00Z")
        );
    }

    #[tokio::test]
    async fn attach_skips_mandate_and_writes_retirement() {
        // `AgentFs::attach` is the no-mandate, no-reconcile constructor
        // used by the Temporal `persist_retirement` activity body where
        // no `Mandate` is in scope (the retirement-signal short-circuit
        // runs before `assemble_context`).
        let storage: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
        let fs = AgentFs::attach(Arc::clone(&storage), "graphs/g1/agents/a1");
        let pinned = DateTime::parse_from_rfc3339("2026-05-24T13:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        fs.persist_retirement("attached", pinned).await.unwrap();

        // mandate.md is *not* created — attach skipped it.
        let mandate = storage.get("graphs/g1/agents/a1/mandate.md").await.unwrap();
        assert!(
            mandate.is_none(),
            "attach must not write mandate.md (no mandate in scope)"
        );

        // retirement.json lives under the prefix and carries the
        // caller-supplied retired_at byte-for-byte.
        let key = "graphs/g1/agents/a1/retirement.json";
        let bytes = storage.get(key).await.unwrap().expect("retirement.json");
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v.get("reason").and_then(|x| x.as_str()), Some("attached"));
        assert_eq!(
            v.get("retired_at").and_then(|x| x.as_str()),
            Some("2026-05-24T13:00:00Z")
        );
    }

    #[tokio::test]
    async fn attach_normalizes_prefix_with_trailing_slash() {
        // `new_with_storage` appends `/` to non-empty prefixes; `attach`
        // must follow the same rule so callers passing
        // `"graphs/g1/agents/a1"` and `"graphs/g1/agents/a1/"` land in
        // the same place.
        let storage: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
        let bare = AgentFs::attach(Arc::clone(&storage), "graphs/g1/agents/a1");
        let with_slash = AgentFs::attach(Arc::clone(&storage), "graphs/g1/agents/a1/");
        // The prefix() accessor exposes the normalized form.
        assert_eq!(bare.prefix(), with_slash.prefix());
        assert_eq!(bare.prefix(), "graphs/g1/agents/a1/");
        // Empty prefix stays empty (no spurious leading slash).
        let empty = AgentFs::attach(Arc::clone(&storage), "");
        assert_eq!(empty.prefix(), "");
    }

    // ---- claim_seed persistence -------------------------------

    use crate::decision::{
        ClaimSeed, Decide, Decision, FsIndex, Seed, Session, ToolCall as DecisionToolCall,
    };

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-05-06T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn claim_slug_is_kebab_lowercase_and_carries_hash_suffix() {
        let s = claim_slug("Phase 2 Clearance");
        // body is kebab-case lowercased
        assert!(s.starts_with("phase-2-clearance-"));
        // suffix is exactly 8 lowercase hex chars
        let suffix = s.rsplit('-').next().unwrap();
        assert_eq!(suffix.len(), 8);
        assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn claim_slug_is_deterministic_for_same_seed() {
        assert_eq!(claim_slug("seed-x"), claim_slug("seed-x"));
        assert_eq!(claim_slug("Phase 2"), claim_slug("Phase 2"));
    }

    #[test]
    fn claim_slug_differs_for_seeds_that_kebab_to_the_same_body() {
        // Both kebab to "abc"; the hash suffix must keep them distinct.
        let a = claim_slug("abc");
        let b = claim_slug("ABC");
        assert_ne!(a, b);
        assert!(a.starts_with("abc-"));
        assert!(b.starts_with("abc-"));
    }

    #[test]
    fn claim_slug_handles_empty_body() {
        let s = claim_slug("!!!");
        assert_eq!(s.len(), 8);
        assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn claim_slug_truncates_long_bodies() {
        let long = "a".repeat(200);
        let s = claim_slug(&long);
        // body 80 chars + '-' + 8 hex chars
        assert_eq!(s.len(), SLUG_BODY_MAX + 1 + 8);
    }

    #[tokio::test]
    async fn write_claim_round_trip_via_read_claim() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let claim = Claim {
            seed: "phase-2-clearance".into(),
            description: "Did drug X pass phase 2?".into(),
            status: ClaimStatus::Open,
            created_at: now(),
        };
        fs.write_claim(&claim).await.unwrap();

        let back = fs.read_claim("phase-2-clearance").await.unwrap();
        assert_eq!(back, Some(claim));
    }

    #[tokio::test]
    async fn read_claim_returns_none_for_missing_seed() {
        let (_tmp, fs, _m) = fresh_fs().await;
        assert_eq!(fs.read_claim("never-written").await.unwrap(), None);
    }

    #[tokio::test]
    async fn write_claim_overwrites_for_status_updates() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let mut claim = Claim {
            seed: "drug-x-p2".into(),
            description: "?".into(),
            status: ClaimStatus::Open,
            created_at: now(),
        };
        fs.write_claim(&claim).await.unwrap();
        claim.status = ClaimStatus::Resolved;
        fs.write_claim(&claim).await.unwrap();

        let back = fs.read_claim("drug-x-p2").await.unwrap().unwrap();
        assert_eq!(back.status, ClaimStatus::Resolved);
    }

    #[tokio::test]
    async fn list_claims_returns_all_in_filename_order() {
        let (_tmp, fs, _m) = fresh_fs().await;
        for s in ["alpha", "bravo", "charlie"] {
            fs.write_claim(&Claim {
                seed: s.into(),
                description: s.into(),
                status: ClaimStatus::Open,
                created_at: now(),
            })
            .await
            .unwrap();
        }
        let listed = fs.list_claims().await.unwrap();
        assert_eq!(listed.len(), 3);
        // Filename order is stable: same call twice yields same order.
        let again = fs.list_claims().await.unwrap();
        assert_eq!(listed, again);
    }

    /// Mock `Decide` impl that consults `claims/` before issuing
    /// `Decision::CallTool`. Reuses an existing seed if it finds a
    /// matching `description`; otherwise mints a new seed (and writes
    /// the claim file) before emitting the decision.
    ///
    /// The "matching description" lookup stands in for the real
    /// LLM-side recognition step ("is this conceptually the same
    /// claim I already opened?"). The point of the test is the
    /// seed-reuse path, not how the agent recognizes the match.
    struct ClaimAwareMock {
        fs: AgentFs,
        topic: String,
        new_seed: String,
    }

    #[async_trait::async_trait]
    impl Decide for ClaimAwareMock {
        async fn decide(&self, _session: &Session) -> anyhow::Result<Decision> {
            // Reuse a seed if a claim already exists for this topic.
            let claims = self.fs.list_claims().await?;
            let existing = claims.into_iter().find(|c| c.description == self.topic);
            let seed = match existing {
                Some(c) => c.seed,
                None => {
                    let seed = self.new_seed.clone();
                    self.fs
                        .write_claim(&Claim {
                            seed: seed.clone(),
                            description: self.topic.clone(),
                            status: ClaimStatus::Open,
                            created_at: now(),
                        })
                        .await?;
                    seed
                }
            };
            Ok(Decision::CallTools {
                calls: vec![DecisionToolCall::new(
                    "echo",
                    serde_json::json!({"q": self.topic}),
                    ClaimSeed::new(seed),
                )],
            })
        }
    }

    fn empty_session(mandate: Mandate) -> Session {
        Session::new(Seed::new(mandate, vec![], FsIndex::default()))
    }

    #[tokio::test]
    async fn seed_reuse_round_trip_returns_existing_claim_seed() {
        let (_tmp, fs, mandate) = fresh_fs().await;

        // Tick 0: claim already on disk from a prior tick.
        fs.write_claim(&Claim {
            seed: "phase-2-clearance".into(),
            description: "Did drug X pass phase 2?".into(),
            status: ClaimStatus::Open,
            created_at: now(),
        })
        .await
        .unwrap();

        let mock = ClaimAwareMock {
            fs: fs.clone(),
            topic: "Did drug X pass phase 2?".into(),
            new_seed: "should-not-be-minted".into(),
        };

        let decision = mock.decide(&empty_session(mandate)).await.unwrap();
        match decision {
            Decision::CallTools { calls } => {
                assert_eq!(calls.len(), 1);
                // Same seed string → same ClaimSeed (== same kernel-side claim id).
                assert_eq!(calls[0].claim_seed, ClaimSeed::new("phase-2-clearance"));
            }
            other => panic!("expected CallTools, got {other:?}"),
        }

        // Mock must not have minted a second claim file.
        let listed = fs.list_claims().await.unwrap();
        assert_eq!(listed.len(), 1);
    }

    #[tokio::test]
    async fn new_seed_creation_path_writes_claim_and_emits_call_tool() {
        let (_tmp, fs, mandate) = fresh_fs().await;
        assert!(fs.list_claims().await.unwrap().is_empty());

        let mock = ClaimAwareMock {
            fs: fs.clone(),
            topic: "Did drug X pass phase 2?".into(),
            new_seed: "phase-2-clearance".into(),
        };

        let decision = mock.decide(&empty_session(mandate)).await.unwrap();
        match decision {
            Decision::CallTools { calls } => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].claim_seed, ClaimSeed::new("phase-2-clearance"));
            }
            other => panic!("expected CallTools, got {other:?}"),
        }

        // The claim file is now on disk and a future tick would find it.
        let back = fs.read_claim("phase-2-clearance").await.unwrap().unwrap();
        assert_eq!(back.description, "Did drug X pass phase 2?");
        assert_eq!(back.status, ClaimStatus::Open);
    }

    #[test]
    fn stable_claim_ids_for_identical_seed_strings() {
        // Sanity: the kernel-side derivation from `ClaimSeed` is
        // identity today (the seed *is* the id), so identical seed
        // strings compare equal. This test pins that invariant so a
        // future change to the derivation has to update it
        // deliberately.
        assert_eq!(ClaimSeed::new("phase-2"), ClaimSeed::new("phase-2"));
        assert_ne!(ClaimSeed::new("phase-2"), ClaimSeed::new("phase-3"));
    }

    // ---- facade-level adversarial tests -----------------------

    /// Mock `AgentStorage` that returns `Transient` for the next N
    /// `put` calls, then delegates to an inner `MemoryStorage`. Lets
    /// the agent-loop verify that a typed `StorageError::Transient`
    /// surfaces through the `FsError::Storage` wrapper without being
    /// degraded into a generic anyhow error.
    struct FlakyPutStorage {
        inner: MemoryStorage,
        fail_remaining: tokio::sync::Mutex<u32>,
    }

    impl FlakyPutStorage {
        fn new(fail_count: u32) -> Self {
            Self {
                inner: MemoryStorage::new(),
                fail_remaining: tokio::sync::Mutex::new(fail_count),
            }
        }
    }

    #[async_trait::async_trait]
    impl AgentStorage for FlakyPutStorage {
        async fn put(&self, key: &str, value: Bytes) -> crate::storage::StorageResult<()> {
            let mut remaining = self.fail_remaining.lock().await;
            if *remaining > 0 {
                *remaining -= 1;
                return Err(crate::storage::StorageError::Transient(format!(
                    "simulated transient on put({key})"
                )));
            }
            drop(remaining);
            self.inner.put(key, value).await
        }
        async fn put_if_absent(
            &self,
            key: &str,
            value: Bytes,
        ) -> crate::storage::StorageResult<PutOutcome> {
            self.inner.put_if_absent(key, value).await
        }
        async fn get(&self, key: &str) -> crate::storage::StorageResult<Option<Bytes>> {
            self.inner.get(key).await
        }
        async fn get_many(
            &self,
            keys: &[&str],
        ) -> crate::storage::StorageResult<Vec<Option<Bytes>>> {
            self.inner.get_many(keys).await
        }
        async fn delete(&self, key: &str) -> crate::storage::StorageResult<()> {
            self.inner.delete(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            after: Option<&str>,
            limit: usize,
        ) -> crate::storage::StorageResult<crate::storage::ListPage> {
            self.inner.list(prefix, after, limit).await
        }
    }

    #[tokio::test]
    async fn agent_fs_propagates_typed_storage_transient_error_through_fs_error() {
        // Build an AgentFs over MemoryStorage first (so the mandate
        // write succeeds), then swap in a FlakyPutStorage backed by a
        // fresh memory store for the actual operation under test.
        // Constructing the AgentFs directly against FlakyPutStorage(1)
        // would consume the failure budget on the mandate write
        // inside `new_with_storage`.
        let mandate = Mandate::new("flaky", Duration::from_millis(100), Some(1));
        let storage: Arc<dyn AgentStorage> = Arc::new(FlakyPutStorage::new(1));
        // Burn the single allowed failure on a throwaway put so
        // new_with_storage's own mandate.md write lands on a healthy put.
        storage
            .put("mandate.md", Bytes::from_static(b"seed"))
            .await
            .ok(); // first put consumes the failure
        let fs = AgentFs::new_with_storage(storage, "", &mandate)
            .await
            .unwrap();

        // Seed an evidence record so persist_output's evidence check
        // resolves. `put_if_absent` is delegated straight to inner so
        // this is unaffected by the put-failure counter.
        let rec = record("echo", json!({"k": "v"}), json!({"r": "v"}));
        let cite = fs.record_evidence(rec, "echo kv").await.unwrap();

        // The flaky counter was already exhausted on the pre-seed put;
        // verify a normal write succeeds. Then exhaust a new flaky
        // storage on an isolated AgentFs.
        let _ = fs.persist_output("ok", &[cite]).await.unwrap();

        // Independent verification: a fresh flaky storage produces an
        // FsError::Storage whose inner StorageError is Transient.
        let flaky: Arc<dyn AgentStorage> = Arc::new(FlakyPutStorage::new(1));
        let err = flaky.put("k", Bytes::from_static(b"v")).await.unwrap_err();
        assert!(
            matches!(err, crate::storage::StorageError::Transient(_)),
            "expected Transient, got {err:?}"
        );

        // And: that error type survives the FsError::Storage wrap that
        // `persist_retirement` (a plain `put`) performs.
        let mandate2 = Mandate::new("flaky2", Duration::from_millis(100), Some(1));
        let storage2: Arc<dyn AgentStorage> = Arc::new(FlakyPutStorage::new(2));
        // First flaky consumes the mandate.md write inside new_with_storage.
        let fs2 = match AgentFs::new_with_storage(storage2, "", &mandate2).await {
            Ok(f) => f,
            Err(e) => {
                let typed = e
                    .downcast_ref::<FsError>()
                    .expect("mandate.md write should surface FsError");
                match typed {
                    FsError::Storage { source, .. } => {
                        assert!(matches!(source, crate::storage::StorageError::Transient(_)));
                    }
                    other => panic!("expected FsError::Storage, got {other:?}"),
                }
                return;
            }
        };
        // If construction somehow succeeded, the second put (retirement)
        // must surface the typed error.
        let err = fs2.persist_retirement("bye", Utc::now()).await.unwrap_err();
        let typed = err
            .downcast_ref::<FsError>()
            .expect("expected FsError wrapping the storage error");
        match typed {
            FsError::Storage { source, .. } => {
                assert!(matches!(source, crate::storage::StorageError::Transient(_)));
            }
            other => panic!("expected FsError::Storage, got {other:?}"),
        }
    }

    /// `AgentFs::new_with_storage` accepts a `MemoryStorage` backend so
    /// tests that don't want a tempdir can run hermetically. Smoke-check
    /// the round-trip.
    #[tokio::test]
    async fn agent_fs_over_memory_storage_round_trips_basic_operations() {
        let mandate = Mandate::new("mem", Duration::from_millis(100), Some(2));
        let storage: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
        let fs = AgentFs::new_with_storage(storage, "graphs/g1/agents/a1", &mandate)
            .await
            .unwrap();
        // Prefix normalisation: trailing slash auto-appended.
        assert_eq!(fs.prefix(), "graphs/g1/agents/a1/");

        let rec = record("t", json!({}), json!({}));
        let cite = fs.record_evidence(rec, "t").await.unwrap();
        let out_id = fs.persist_output("hello", &[cite]).await.unwrap();
        assert_eq!(out_id, OutputId::new("hello"));
        assert_eq!(fs.read_output().await.unwrap(), "hello");
    }

    // ---- tail-index integration -------------------------------

    /// The `evidence/_tail.json` object must be present after a
    /// `record_evidence` put — the evidence tail is written on each
    /// first-write.
    #[tokio::test]
    async fn tail_index_evidence_is_written_on_each_put() {
        let (tmp, fs, _m) = fresh_fs().await;
        let _id = fs
            .record_evidence(record("echo", json!({"k": 1}), json!({"v": 1})), "echo")
            .await
            .unwrap();

        let evidence_tail = tmp.path().join("evidence").join("_tail.json");
        assert!(evidence_tail.is_file(), "evidence/_tail.json missing");

        let parsed: TailObject =
            serde_json::from_slice(&std::fs::read(&evidence_tail).unwrap()).unwrap();
        assert_eq!(parsed.entries.len(), 1);
        assert!(parsed.entries[0].filename.ends_with(".json"));
    }

    /// Same shape for `evidence/`: tail object absent, LIST fallback
    /// returns every on-disk record.
    #[tokio::test]
    async fn list_recent_evidence_recovers_via_list_when_tail_object_absent() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let _id_a = fs
            .record_evidence(record("echo", json!({"a": 1}), json!({"r": 1})), "echo a")
            .await
            .unwrap();
        let _id_b = fs
            .record_evidence(record("echo", json!({"b": 2}), json!({"r": 2})), "echo b")
            .await
            .unwrap();
        fs.storage().delete("evidence/_tail.json").await.unwrap();
        let got = fs.list_recent_evidence(8).await.unwrap();
        assert_eq!(got.len(), 2);
    }

    /// `list_recent_evidence` returns an empty Vec when the prefix has no
    /// writes yet — "safe to call right after open". Verifies the
    /// no-tail-object path.
    #[tokio::test]
    async fn list_recent_evidence_returns_empty_when_no_writes() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let ev = fs.list_recent_evidence(8).await.unwrap();
        assert!(ev.is_empty());
    }

    // ---- conflict-log FS writer ------------------

    use crate::agent_ref::AgentRef;
    use crate::conflict::{ConflictKind, ConflictRecord};
    use crate::decision::{ConflictAlternative, ConflictResolution};
    use uuid::Uuid;

    fn alt(child_slug: &str, claim: &str, output_hex: &str) -> ConflictAlternative {
        ConflictAlternative {
            source_child: AgentRef::new(
                format!("graphs/g1/agents/{child_slug}"),
                AgentId::new(Uuid::new_v4()),
            ),
            source_output_id: OutputId::from_hex(output_hex.repeat(32)),
            claim: claim.to_string(),
        }
    }

    fn ts_fixed() -> chrono::DateTime<Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-05-25T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[tokio::test]
    async fn write_conflict_persists_held_open_record_under_content_addressed_path() {
        let (tmp, fs, _m) = fresh_fs().await;
        let record = ConflictRecord::new(
            ts_fixed(),
            vec![
                alt("child-a", "value is 42", "aa"),
                alt("child-b", "value is 43", "bb"),
            ],
            None,
        );
        let expected_id = record.id.clone();

        let id = fs.write_conflict(&record).await.unwrap();
        assert_eq!(id, expected_id, "write_conflict returns the record's id");

        // File exists at the expected path.
        let path = tmp.path().join("conflicts").join(format!("{}.json", id));
        assert!(
            path.is_file(),
            "conflict file missing at {}",
            path.display()
        );

        // Round-trips through read_conflict.
        let back = fs.read_conflict(&id).await.unwrap().expect("present");
        assert_eq!(back, record);
        assert_eq!(back.kind, ConflictKind::HeldOpen);
    }

    #[tokio::test]
    async fn write_conflict_persists_resolved_record_with_resolution_intact() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let resolution = ConflictResolution {
            chosen_alternative_idx: 1,
            reasoning: "newer evidence".into(),
        };
        let record = ConflictRecord::new(
            ts_fixed(),
            vec![
                alt("child-a", "claim a", "aa"),
                alt("child-b", "claim b", "bb"),
            ],
            Some(resolution.clone()),
        );

        let id = fs.write_conflict(&record).await.unwrap();
        let back = fs.read_conflict(&id).await.unwrap().expect("present");
        assert_eq!(back.kind, ConflictKind::Resolved);
        assert_eq!(back.resolution.as_ref().unwrap(), &resolution);
    }

    #[tokio::test]
    async fn write_conflict_rejects_fewer_than_two_alternatives() {
        let (_tmp, fs, _m) = fresh_fs().await;
        // Bypass `ConflictRecord::new`'s validation-free constructor —
        // we want to confirm the writer is the second line of defence.
        let bad = ConflictRecord {
            id: ConflictId::from_hex("00".repeat(32)),
            timestamp: ts_fixed(),
            kind: ConflictKind::HeldOpen,
            alternatives: vec![alt("only-child", "lonely claim", "cc")],
            resolution: None,
        };
        let err = fs.write_conflict(&bad).await.unwrap_err();
        match err.downcast_ref::<FsError>() {
            Some(FsError::ConflictAlternativesTooFew { count }) => assert_eq!(*count, 1),
            other => panic!("expected ConflictAlternativesTooFew, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn write_conflict_is_idempotent_under_retries() {
        let (tmp, fs, _m) = fresh_fs().await;
        let alts = vec![
            alt("child-a", "claim a", "aa"),
            alt("child-b", "claim b", "bb"),
        ];
        // First write at t0, second write at a later wall-clock t1 —
        // both should land on the same content-addressed file because
        // timestamp is NOT in the id.
        let r1 = ConflictRecord::new(ts_fixed(), alts.clone(), None);
        let id1 = fs.write_conflict(&r1).await.unwrap();

        let later = ts_fixed() + chrono::Duration::seconds(60);
        let r2 = ConflictRecord::new(later, alts, None);
        let id2 = fs.write_conflict(&r2).await.unwrap();
        assert_eq!(id1, id2, "retry must produce the same id");

        // Directory still holds exactly one file.
        let files: Vec<_> = std::fs::read_dir(tmp.path().join("conflicts"))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(files.len(), 1, "expected one file, got {files:?}");
    }

    #[tokio::test]
    async fn read_conflict_returns_none_for_missing_id() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let bogus = ConflictId::from_hex("ee".repeat(32));
        let got = fs.read_conflict(&bogus).await.unwrap();
        assert!(got.is_none(), "expected None for missing conflict id");
    }

    #[tokio::test]
    async fn list_conflicts_returns_all_written_records() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let r1 = ConflictRecord::new(
            ts_fixed(),
            vec![alt("a", "claim a", "aa"), alt("b", "claim b", "bb")],
            None,
        );
        let r2 = ConflictRecord::new(
            ts_fixed(),
            vec![alt("c", "claim c", "cc"), alt("d", "claim d", "dd")],
            Some(ConflictResolution {
                chosen_alternative_idx: 0,
                reasoning: "first".into(),
            }),
        );
        fs.write_conflict(&r1).await.unwrap();
        fs.write_conflict(&r2).await.unwrap();

        let listed = fs.list_conflicts().await.unwrap();
        assert_eq!(listed.len(), 2);
        let ids: std::collections::HashSet<_> =
            listed.iter().map(|c| c.id.as_str().to_string()).collect();
        assert!(ids.contains(r1.id.as_str()));
        assert!(ids.contains(r2.id.as_str()));
    }

    #[tokio::test]
    async fn conflicts_land_under_agent_prefix_on_memory_storage() {
        // Same coverage as `open_for_agent_scopes_storage_to_workflow_id_prefix`
        // but for the conflicts/ prefix — confirms the path scheme survives
        // a non-empty agent prefix without colliding across agents.
        let storage = Arc::new(MemoryStorage::new());
        let dyn_storage: Arc<dyn AgentStorage> = storage.clone();
        let graph_id = GraphId::new(Uuid::new_v4());
        let agent_id = AgentId::new(Uuid::new_v4());
        let fs = AgentFs::open_for_agent(dyn_storage, graph_id, agent_id);

        let record = ConflictRecord::new(
            ts_fixed(),
            vec![
                alt("child-a", "claim a", "aa"),
                alt("child-b", "claim b", "bb"),
            ],
            None,
        );
        let id = fs.write_conflict(&record).await.unwrap();

        // The on-disk key carries the agent prefix.
        let expected_key = format!("graphs/{graph_id}/agents/{agent_id}/conflicts/{id}.json");
        let bytes = storage.get(&expected_key).await.unwrap();
        assert!(
            bytes.is_some(),
            "conflict not at expected key {expected_key}"
        );
    }
}
