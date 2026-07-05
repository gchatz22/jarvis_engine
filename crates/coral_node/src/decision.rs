//! `Decision` — what the agent wants the runtime to do next, for one step of
//! the inner cycle loop. Pure data; every variant carries enough information
//! that a Decision can be serialized, replayed, or audited without the
//! original `Decide` implementation in hand. `Idle` is the sole *terminal*
//! step — it ends the cycle and sets the next wake cadence; every other
//! variant is a *repertoire* step that runs, produces an observation, and
//! continues the loop.
//!
//! Also hosts the inner-loop surface: `Seed` (the thin orienting snapshot
//! built once per cycle), `Session` (the accumulating seed-plus-observations
//! the model reasons over each step), the `Decide` trait, and `MockDecide`.
//!
//! # Parent-child variants
//!
//! Four variants describe parent-driven topology changes the LLM may
//! propose: [`Decision::SpawnChild`], [`Decision::ReconcileChildren`],
//! [`Decision::RetireChild`], and [`Decision::ReplaceChild`]. Each
//! variant captures the LLM's *intent*; the runtime arm that executes
//! it lives in `coral_temporal::workflow` and routes the variant to
//! either a Temporal activity or an SDK workflow command. The variants
//! carry only kernel-visible data (mandates, agent references, output
//! ids, claim summaries) — no host-side workflow handles leak through.
//!
//! Cross-host stable identity travels as [`AgentRef`] (workflow id +
//! structural [`AgentId`](crate::agent_ref::AgentId)). Every variant
//! that names another agent uses [`AgentRef`] rather than a bare id so
//! the same value can address an SDK signal *and* index the structural
//! DB without a resolve step.
//!
//! ## `ReconcileChildren` shape
//!
//! The LLM is the only thing in the loop with enough context to
//! summarize what a child claimed, so [`Decision::ReconcileChildren`]
//! carries those summaries inline rather than asking the runtime to
//! introspect arbitrary child output bodies. Shape:
//!
//! ```text
//! ReconcileChildren {
//!     sources:  Vec<ReconcileSource>,         // 1+ child outputs to fold in
//!     conflict: Option<ConflictRecordIntent>, // Some iff the LLM observed disagreement
//! }
//! ```
//!
//! The reconcile activity persists the decision verbatim: one synthetic
//! evidence record per [`ReconcileSource`] in the parent's `evidence/`
//! directory (so the parent's next `WriteOutput` can cite the children
//! through the existing evidence contract) and — only when `conflict`
//! is `Some` — one [`ConflictRecordIntent`] written as a
//! [`crate::conflict::ConflictRecord`] under the parent's `conflicts/`.
//! The activity never edits claim text or chosen-alternative indices;
//! the LLM owns those. `Some` with fewer than two alternatives is
//! structurally not a conflict and the activity rejects it; `None` is
//! the concordance fold-in case (synthetic evidence only, no conflict
//! file written).

use crate::agent_ref::AgentRef;
use crate::mandate::{Mandate, OutputId};
use crate::model_client::ToolSpec;
use crate::trigger::Trigger;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

/// What the agent has decided to do this tick.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Decision {
    /// Invoke one or more tools in parallel for the same tick. A
    /// one-element vector preserves the original single-call semantics.
    /// The run loop dispatches every entry in the same tick, persists one
    /// `EvidenceRecord` per call, and stages the paired `tool_result`
    /// blocks for the next prompt bundle.
    ///
    /// Struct (not tuple) variant because serde's
    /// `#[serde(tag = "type")]` on this enum rejects tagged newtype
    /// variants carrying a sequence — `CallTools(Vec<ToolCall>)` would
    /// emit cleanly but fail to deserialize back. The named field keeps
    /// the wire form `{"type":"call_tools","calls":[...]}` flat and
    /// round-trippable.
    CallTools { calls: Vec<ToolCall> },
    /// Write the agent's single, kept-current Output: the prose `body` is
    /// persisted to the canonical `outputs/output.md` (overwriting the prior
    /// cycle's), and `citations` lists the `evidence/` paths the output rests
    /// on (the handles returned in tool-call and reconcile observations). The
    /// run loop refuses to persist an output whose citations don't all resolve
    /// to runtime-authored evidence.
    WriteOutput {
        body: String,
        citations: Vec<String>,
    },
    /// Mutate the per-agent filesystem.
    RewriteFs { ops: Vec<FsOp> },
    /// Read the full contents of one file in the agent's own FS (or, on the
    /// Temporal path, a descendant agent's FS — always read-only). A
    /// repertoire step: the file body is appended to the session as the
    /// observation the next step reasons over. This is half of the
    /// pull-navigation surface — the model fetches what it needs rather than
    /// being handed a fat context window.
    Read { path: String },
    /// List the filenames under a directory in the agent's own FS (or a
    /// descendant's, read-only). Repertoire step; the listing is the
    /// observation.
    List { path: String },
    /// Substring-search file contents under an optional path scope. `None`
    /// scopes the search to the whole own FS. Repertoire step; the matches
    /// are the observation.
    Search {
        query: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
    },
    /// Tell the scheduler to wait at least `next_after` before the next
    /// idle wake. This is the **sole terminal** step: it ends the inner
    /// cycle and hands control back to the wake boundary.
    Idle {
        #[serde(with = "crate::duration_ms")]
        next_after: Duration,
    },
    /// Parent spawns a child agent at decision time. The host-side
    /// `register_child_in_structural_db` activity allocates the child's
    /// `AgentId` deterministically and instantiates an `AgentWorkflow`
    /// under `graphs/<gid>/agents/<new_aid>`. This variant carries only
    /// the agent's logical name + mandate; the structural id is
    /// host-side state.
    SpawnChild {
        agent_name: String,
        mandate: Mandate,
    },
    /// Parent folds N child outputs into its own context as synthetic
    /// evidence; optionally records a conflict if the children disagree.
    ///
    /// The variant carries the claim summaries inline (via
    /// `ConflictAlternative.claim`) rather than asking the activity to
    /// introspect arbitrary JSON outputs — summarization is the LLM's
    /// job. `conflict` is `Some` iff the children disagree; the activity
    /// writes the conflict record only on `Some`.
    ReconcileChildren {
        /// 1+ child outputs to fold in. Each becomes one synthetic
        /// evidence record in the parent's `evidence/` directory at
        /// activity-execution time.
        sources: Vec<ReconcileSource>,
        /// `Some` iff the parent observed disagreement among the
        /// `sources`. `None` reconciliations are concordance fold-ins
        /// (no conflict record written).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conflict: Option<ConflictRecordIntent>,
    },
    /// Parent terminates a child. The workflow host signals the child's
    /// existing retire arm via
    /// `WorkflowContext::signal_external_workflow(..)`. No replacement
    /// is spawned — for that, use `ReplaceChild`.
    RetireChild { child_ref: AgentRef, reason: String },
    /// Parent retires a child and spawns a replacement with a new
    /// mandate. The replacement gets a fresh `AgentId` + workflow id —
    /// not an in-place mandate swap on the existing child. The flat
    /// workflow-id scheme means ids do not encode topology, so a
    /// "replace" is structurally a retire + spawn from the kernel's
    /// point of view.
    ReplaceChild {
        child_ref: AgentRef,
        new_mandate: Mandate,
    },
}

impl Decision {
    /// `Some(next_after)` iff this is the sole terminal step (`Idle`), which
    /// ends the cycle and sets the next wake cadence. Every other variant is
    /// a repertoire step that runs, yields an observation, and continues the
    /// inner loop. The cycle driver uses this to decide whether to break.
    pub fn idle_after(&self) -> Option<Duration> {
        match self {
            Decision::Idle { next_after } => Some(*next_after),
            _ => None,
        }
    }
}

/// One child output the parent wants folded into its own context. The
/// `reconcile_children` activity reads the child's single canonical
/// `outputs/output.md` and writes one synthetic evidence record per source
/// into the parent's `evidence/` directory; the parent's subsequent
/// `WriteOutput { citations: [..] }` then cites those synthetic records by
/// path, so cross-agent provenance becomes a normal evidence trail.
/// `output_id` is
/// the version the parent observed at decide time; the activity reads the
/// child's current canonical output (kept-current semantics).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileSource {
    pub child_ref: AgentRef,
    pub output_id: OutputId,
}

/// The parent's account of a disagreement among the cited child outputs.
/// `alternatives.len() >= 2` is the load-bearing invariant (a single
/// alternative is not a conflict); the `reconcile_children` activity
/// validates that and writes the resulting
/// `<agent_root>/conflicts/<id>.json`.
///
/// `resolution` is `None` for "held open" — the parent records the
/// disagreement but does not pick a winner. `Some` carries the chosen
/// alternative index + the parent's reasoning.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictRecordIntent {
    /// At least two alternatives; the type does not enforce the bound,
    /// the writing activity does (so a malformed `Decision` is a
    /// `NeedsCorrection` rather than a panic).
    pub alternatives: Vec<ConflictAlternative>,
    /// `None` = held open; `Some` = parent picked a winner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<ConflictResolution>,
}

/// One side of a conflict. The `claim` text is the parent LLM's summary
/// of what this source asserts — summarization happens at decision time
/// because only the LLM has the context to do it well; the activity just
/// persists what's in the decision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictAlternative {
    pub source_child: AgentRef,
    pub source_output_id: OutputId,
    pub claim: String,
}

/// How the parent resolved a conflict. The chosen index points into the
/// surrounding `ConflictRecordIntent.alternatives` vec; bounds-checking
/// is the writing activity's job. `reasoning` becomes part of the
/// persisted conflict record so the resolution is auditable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictResolution {
    pub chosen_alternative_idx: usize,
    pub reasoning: String,
}

/// Content-addressed id of a persisted conflict record
/// (`<agent_root>/conflicts/<id>.json`). Carried on the
/// `reconcile_children` activity's output so the writer can return the
/// id of the record it just wrote.
///
/// Mirrors the [`OutputId`] / [`EvidenceId`] precedent: transparent
/// serde so the on-disk filename and wire form are the underlying hex
/// digits, `Display` for log/trace formatting, `from_hex` for
/// deserialization-path construction.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConflictId(String);

impl ConflictId {
    /// Wrap a pre-computed hex string. Trusts the caller; the
    /// canonicalize-and-hash constructor lives next to the writer in
    /// `crate::conflict`.
    pub fn from_hex(hex: impl Into<String>) -> Self {
        ConflictId(hex.into())
    }

    /// Borrow the hex digits.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ConflictId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One element of a `Decision::CallTools`. `claim_seed` is an opaque
/// hint the agent attaches so the resulting evidence can be linked to
/// the claim it was meant to support. `tool_use_id` is the vendor's
/// `tool_use.id` from the model response that introduced this call —
/// the kernel propagates it back through the next prompt's
/// `tool_result` blocks so the assistant turn's `tool_use` ids stay
/// paired with their results across the tick boundary, per both
/// Anthropic's and Cohere's tool-use protocols.
///
/// Named `decision::ToolCall` to avoid confusion with the vendor-wire
/// `model_client::ToolCall` shape (`{ id, name, arguments }`). The two
/// types are deliberately distinct: this one is the kernel's intent;
/// the vendor one is the parsed wire payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCall {
    pub name: String,
    pub args: serde_json::Value,
    pub claim_seed: ClaimSeed,
    /// Vendor-supplied `tool_use.id` for the model response that emitted
    /// this call. `None` when the decision was synthesized by a non-model
    /// `Decide` (e.g. `MockDecide` in tests), in which case no
    /// `tool_result` pairing is required because there is no preceding
    /// assistant `tool_use` block to answer.
    ///
    /// Carried through the parser today; the current cross-tick prompt
    /// path conveys tool results via `recent_evidence` (text bullets in
    /// the next bundle), not via `tool_use`/`tool_result` block pairing.
    /// This field is reserved for the future cross-tick rendering path
    /// that would emit paired blocks across the tick boundary — kept on
    /// the wire today so a fixture or replay captured at this version
    /// stays useful after that path lands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_use_id: Option<String>,
}

impl ToolCall {
    /// Build a `ToolCall` with no vendor `tool_use_id`. Convenience for
    /// tests and non-model `Decide` impls that don't have one to thread
    /// through.
    pub fn new(name: impl Into<String>, args: serde_json::Value, claim_seed: ClaimSeed) -> Self {
        Self {
            name: name.into(),
            args,
            claim_seed,
            tool_use_id: None,
        }
    }

    /// Build a `ToolCall` with a vendor-supplied `tool_use.id`. Used by
    /// the `LlmDecide` parser when it has the id available from the
    /// model's response.
    pub fn with_tool_use_id(
        name: impl Into<String>,
        args: serde_json::Value,
        claim_seed: ClaimSeed,
        tool_use_id: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            args,
            claim_seed,
            tool_use_id: Some(tool_use_id.into()),
        }
    }
}

/// Opaque seed used to deterministically derive a claim id from a tool
/// call. The kernel doesn't interpret it; it's a string the agent picks.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClaimSeed(pub String);

impl ClaimSeed {
    pub fn new(s: impl Into<String>) -> Self {
        ClaimSeed(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A single mutation against the per-agent filesystem. Paths are relative
/// to the agent's FS root; the FS layer is responsible for sandboxing
/// and validation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum FsOp {
    /// Create or overwrite a file with `content`.
    WriteFile { path: String, content: String },
    /// Remove a file. Idempotent at the type level; the FS layer decides
    /// what to do if the path is absent.
    DeleteFile { path: String },
}

/// The thin, orienting snapshot built once at the start of each cycle.
///
/// Unlike the old fat context bundle, the seed carries only what the model
/// needs to *orient* — its mandate, the triggers that woke it, and an
/// `index` of filenames it can pull from. File *contents* are never pushed;
/// the model fetches what it needs via the `Read`/`List`/`Search` repertoire
/// steps. This is the push→pull pivot: a small constant seed plus on-demand
/// navigation, rather than a window whose size has to be tuned per mandate.
///
/// Owned data (not borrows) so it can be moved into an async task, queued,
/// or serialized for audit/replay (and, on the Temporal path, packed into a
/// `decide_step` activity input) without fighting lifetimes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seed {
    pub mandate: Mandate,
    pub triggers: Vec<Trigger>,
    pub index: FsIndex,
    /// The agent's granted runtime tools with their typed schemas, resolved
    /// from the registry at seed-build time. Carried so the decide path can
    /// offer them to a model as first-class functions (the Cohere flatten
    /// path); empty by default and ignored by adapters that don't flatten.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_tools: Vec<ToolSpec>,
}

impl Seed {
    pub fn new(mandate: Mandate, triggers: Vec<Trigger>, index: FsIndex) -> Self {
        Self {
            mandate,
            triggers,
            index,
            runtime_tools: Vec::new(),
        }
    }
}

/// Pointers — filenames only, never contents — into the agent's working
/// memory, most-recent-first. The model reads what it needs via the FS-nav
/// steps; nothing here carries a file body, which is what keeps the seed a
/// small constant rather than a tuned window.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsIndex {
    /// Filenames under `notes/`, most-recent-first.
    #[serde(default)]
    pub notes: Vec<String>,
    /// Filenames under `outputs/`, most-recent-first.
    #[serde(default)]
    pub outputs: Vec<String>,
    /// How many `notes/` files exist beyond `notes` — the index is a recency
    /// window, not the whole directory, so the model should `list`/`search` to
    /// reach the rest.
    #[serde(default)]
    pub notes_more: Remainder,
    /// How many `outputs/` files exist beyond `outputs`.
    #[serde(default)]
    pub outputs_more: Remainder,
}

/// How many files lie beyond a surfaced index window. Exact while the recency
/// index is sub-capacity (the count is in hand); a lower bound once it is at
/// capacity (the true total would need a full listing we decline to pay).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Remainder {
    /// The window is the complete set; nothing lies beyond it.
    #[default]
    None,
    /// Exactly this many files lie beyond the window.
    Exactly(usize),
    /// At least this many files lie beyond the window.
    AtLeast(usize),
}

impl Remainder {
    /// One fewer file now lies beyond the window — e.g. a pinned file that was
    /// counted as overflow is now surfaced. `Exactly(1)` collapses to `None`.
    pub fn decremented(self) -> Remainder {
        match self {
            Remainder::None => Remainder::None,
            Remainder::Exactly(k) if k <= 1 => Remainder::None,
            Remainder::Exactly(k) => Remainder::Exactly(k - 1),
            Remainder::AtLeast(k) => Remainder::AtLeast(k.saturating_sub(1)),
        }
    }
}

/// The accumulating in-cycle context the model reasons over: the orienting
/// `seed` plus the ordered `(action, observation)` steps taken THIS cycle.
///
/// Discarded at cycle end — cross-cycle continuity is the per-agent FS
/// (`notes/`), not this. On the Temporal path the session is rebuilt only
/// from journaled activity results held in workflow state, so replay is
/// deterministic; it is never recomputed from a live FS read in the
/// workflow body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub seed: Seed,
    pub steps: Vec<Step>,
}

impl Session {
    /// Start a fresh cycle from its orienting seed.
    pub fn new(seed: Seed) -> Self {
        Self {
            seed,
            steps: Vec::new(),
        }
    }

    /// Append one completed step (the action taken and what it observed).
    pub fn push(&mut self, action: Decision, observation: Observation) {
        self.steps.push(Step {
            action,
            observation,
        });
    }

    /// Number of repertoire steps taken so far this cycle. Counted against
    /// the runaway fuse.
    pub fn len(&self) -> usize {
        self.steps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }
}

/// One completed repertoire step: the action the model took and the
/// observation it produced.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    pub action: Decision,
    pub observation: Observation,
}

/// The result of executing one repertoire action, rendered for the model to
/// read on its next step.
///
/// `ok == false` carries a failure the model is expected to adapt to within
/// the *same* cycle — a tool error, an unsatisfiable output. There is no
/// cross-cycle correction state: the failure is just an observation the next
/// step reasons over, so self-correction happens inline.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    pub ok: bool,
    pub content: String,
}

impl Observation {
    /// A successful step's observation.
    pub fn ok(content: impl Into<String>) -> Self {
        Self {
            ok: true,
            content: content.into(),
        }
    }

    /// A recoverable failure the model should adapt to within this cycle.
    pub fn err(content: impl Into<String>) -> Self {
        Self {
            ok: false,
            content: content.into(),
        }
    }
}

/// Trait every model adapter (mock, real LLM, deterministic policy)
/// implements. The run loop talks to nothing else when it needs "what
/// should the agent do next?" — that constraint is the whole point of
/// this trait.
///
/// `Send + Sync` are required because the loop owns its `Decide`
/// implementation behind shared state (typically `Arc<dyn Decide>`) and
/// awaits across `.await` points on a multi-threaded runtime.
#[async_trait::async_trait]
pub trait Decide: Send + Sync {
    /// Pick the next step given the accumulating cycle `session` (the
    /// orienting seed plus every `(action, observation)` taken so far). The
    /// driver calls this once per inner-loop iteration; returning `Idle`
    /// ends the cycle.
    async fn decide(&self, session: &Session) -> anyhow::Result<Decision>;
}

/// Scripted `Decide` for tests: pops decisions from a queue in FIFO order.
///
/// We use `std::sync::Mutex<VecDeque<Decision>>` rather than the tokio
/// flavor because the body of `decide` is purely synchronous (pop, return)
/// — no `.await` is held while the lock is taken, so the std mutex is
/// strictly cheaper and keeps the trait body free of an unnecessary
/// async-runtime dependency at the lock point.
///
/// When the script is exhausted, `decide` returns an error rather than
/// panicking so a misconfigured test surfaces as a normal `Result` failure
/// the harness can report.
pub struct MockDecide {
    script: Mutex<VecDeque<Decision>>,
}

impl MockDecide {
    /// Build a mock from a script of decisions to return, in order.
    pub fn new(script: Vec<Decision>) -> Self {
        Self {
            script: Mutex::new(script.into()),
        }
    }

    /// Number of decisions still queued. Useful in tests that want to
    /// assert the loop drained the script.
    pub fn remaining(&self) -> usize {
        self.script.lock().expect("MockDecide mutex poisoned").len()
    }
}

#[async_trait::async_trait]
impl Decide for MockDecide {
    async fn decide(&self, _session: &Session) -> anyhow::Result<Decision> {
        let mut q = self.script.lock().expect("MockDecide mutex poisoned");
        q.pop_front()
            .ok_or_else(|| anyhow!("MockDecide script exhausted"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fs_index_deserializes_pre_signpost_payload_with_defaults() {
        // A seed serialized before the `*_has_more` fields existed (e.g. an
        // in-flight carryover or journaled `build_seed` output) must still
        // deserialize, defaulting the new flags to false.
        let old = json!({ "notes": ["a.md"], "outputs": ["b.json"] });
        let idx: FsIndex = serde_json::from_value(old).unwrap();
        assert_eq!(idx.notes, vec!["a.md".to_string()]);
        assert_eq!(idx.outputs, vec!["b.json".to_string()]);
        assert_eq!(idx.notes_more, Remainder::None);
        assert_eq!(idx.outputs_more, Remainder::None);
    }

    #[test]
    fn remainder_decremented_consumes_one_overflow() {
        assert_eq!(Remainder::None.decremented(), Remainder::None);
        assert_eq!(Remainder::Exactly(1).decremented(), Remainder::None);
        assert_eq!(Remainder::Exactly(3).decremented(), Remainder::Exactly(2));
        assert_eq!(Remainder::AtLeast(56).decremented(), Remainder::AtLeast(55));
    }

    #[test]
    fn call_tools_single_call_round_trip() {
        let d = Decision::CallTools {
            calls: vec![ToolCall::new(
                "echo",
                json!({"msg": "hi"}),
                ClaimSeed::new("seed-1"),
            )],
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
        // tool_use_id absent on the wire when None (skip_serializing_if).
        assert!(!s.contains("tool_use_id"), "wire shape: {s}");
        // Tagged enum: tag stays `call_tools` and the vec lives under
        // `calls` — the field name is wire-stable.
        assert!(s.contains("\"type\":\"call_tools\""), "wire shape: {s}");
        assert!(s.contains("\"calls\":["), "wire shape: {s}");
    }

    #[test]
    fn call_tools_multi_call_round_trip() {
        let d = Decision::CallTools {
            calls: vec![
                ToolCall::with_tool_use_id(
                    "read_a",
                    json!({"path": "a.md"}),
                    ClaimSeed::new("seed-a"),
                    "toolu_a",
                ),
                ToolCall::with_tool_use_id(
                    "read_b",
                    json!({"path": "b.md"}),
                    ClaimSeed::new("seed-b"),
                    "toolu_b",
                ),
                ToolCall::with_tool_use_id(
                    "read_c",
                    json!({"path": "c.md"}),
                    ClaimSeed::new("seed-c"),
                    "toolu_c",
                ),
            ],
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
        if let Decision::CallTools { calls } = back {
            assert_eq!(calls.len(), 3);
            assert_eq!(calls[0].tool_use_id.as_deref(), Some("toolu_a"));
            assert_eq!(calls[2].name, "read_c");
        } else {
            panic!("expected CallTools");
        }
    }

    #[test]
    fn write_output_round_trip_non_empty_citations() {
        let p1 = "evidence/alpha-aabbccdd.json".to_string();
        let p2 = "evidence/beta-11223344.json".to_string();
        let d = Decision::WriteOutput {
            body: "hello".into(),
            citations: vec![p1.clone(), p2.clone()],
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
        if let Decision::WriteOutput { citations, .. } = back {
            assert_eq!(citations, vec![p1, p2]);
        } else {
            panic!("expected WriteOutput");
        }
    }

    #[test]
    fn write_output_round_trip_empty_citations() {
        let d = Decision::WriteOutput {
            body: "draft".into(),
            citations: vec![],
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
        if let Decision::WriteOutput { citations, .. } = back {
            assert!(citations.is_empty());
        } else {
            panic!("expected WriteOutput");
        }
    }

    #[test]
    fn rewrite_fs_round_trip() {
        let d = Decision::RewriteFs {
            ops: vec![
                FsOp::WriteFile {
                    path: "notes/a.md".into(),
                    content: "hi".into(),
                },
                FsOp::DeleteFile {
                    path: "notes/old.md".into(),
                },
            ],
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn idle_round_trip_serializes_duration_as_ms() {
        let d = Decision::Idle {
            next_after: Duration::from_millis(2500),
        };
        let s = serde_json::to_string(&d).unwrap();
        assert!(s.contains("\"next_after\":2500"), "got {s}");
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
    }

    // ---- Parent-child topology variants ---------------------------------

    use crate::agent_ref::{AgentId, AgentRef};
    use crate::mandate::OutputId;
    use uuid::Uuid;

    /// Hand-picked, valid UUID v4 reused across these tests so the
    /// wire-form assertions are exact.
    fn fixed_agent_id() -> AgentId {
        AgentId::new(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap())
    }

    fn fixed_agent_ref() -> AgentRef {
        AgentRef::new("graphs/g1/agents/a-child", fixed_agent_id())
    }

    fn fixed_output_id() -> OutputId {
        // 64-hex-char placeholder; `OutputId::from_hex` trusts the caller
        // (mirrors `EvidenceId::from_hex`).
        OutputId::from_hex("ab".repeat(32))
    }

    #[test]
    fn spawn_child_round_trip_carries_mandate_and_agent_name() {
        let d = Decision::SpawnChild {
            agent_name: "fetcher".into(),
            mandate: Mandate::new("fetch foo", Duration::from_millis(500), Some(8)),
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
        // Wire tag matches `rename_all = "snake_case"`.
        assert!(s.contains("\"type\":\"spawn_child\""), "wire shape: {s}");
        assert!(s.contains("\"agent_name\":\"fetcher\""), "wire shape: {s}");
        // Mandate's `idle_period` round-trips as ms (shared
        // `duration_ms` helper).
        assert!(s.contains("\"idle_period\":500"), "wire shape: {s}");
    }

    #[test]
    fn reconcile_children_round_trip_with_no_conflict_omits_conflict_field() {
        let d = Decision::ReconcileChildren {
            sources: vec![ReconcileSource {
                child_ref: fixed_agent_ref(),
                output_id: fixed_output_id(),
            }],
            conflict: None,
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
        assert!(
            s.contains("\"type\":\"reconcile_children\""),
            "wire shape: {s}"
        );
        // `skip_serializing_if` on `conflict` keeps the wire form lean
        // for the common concordance-fold case.
        assert!(!s.contains("conflict"), "wire shape: {s}");
    }

    #[test]
    fn reconcile_children_round_trip_with_conflict_and_resolution() {
        let alt_a = ConflictAlternative {
            source_child: fixed_agent_ref(),
            source_output_id: fixed_output_id(),
            claim: "value is 42".into(),
        };
        let alt_b = ConflictAlternative {
            source_child: AgentRef::new(
                "graphs/g1/agents/b-child",
                AgentId::new(Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap()),
            ),
            source_output_id: OutputId::from_hex("cd".repeat(32)),
            claim: "value is 43".into(),
        };
        let d = Decision::ReconcileChildren {
            sources: vec![ReconcileSource {
                child_ref: fixed_agent_ref(),
                output_id: fixed_output_id(),
            }],
            conflict: Some(ConflictRecordIntent {
                alternatives: vec![alt_a, alt_b],
                resolution: Some(ConflictResolution {
                    chosen_alternative_idx: 0,
                    reasoning: "primary source has higher confidence".into(),
                }),
            }),
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
        assert!(
            s.contains("\"chosen_alternative_idx\":0"),
            "wire shape: {s}"
        );
    }

    #[test]
    fn reconcile_children_round_trip_with_held_open_resolution() {
        // `resolution: None` is "held open" — explicit recorded
        // disagreement without a winner. The wire form must omit the
        // `resolution` field (matching `#[serde(skip_serializing_if)]`).
        let intent = ConflictRecordIntent {
            alternatives: vec![
                ConflictAlternative {
                    source_child: fixed_agent_ref(),
                    source_output_id: fixed_output_id(),
                    claim: "disagree A".into(),
                },
                ConflictAlternative {
                    source_child: fixed_agent_ref(),
                    source_output_id: OutputId::from_hex("ef".repeat(32)),
                    claim: "disagree B".into(),
                },
            ],
            resolution: None,
        };
        let s = serde_json::to_string(&intent).unwrap();
        assert!(!s.contains("resolution"), "wire shape: {s}");
        let back: ConflictRecordIntent = serde_json::from_str(&s).unwrap();
        assert_eq!(intent, back);
    }

    #[test]
    fn retire_child_round_trip_carries_child_ref_and_reason() {
        let d = Decision::RetireChild {
            child_ref: fixed_agent_ref(),
            reason: "no longer needed".into(),
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
        assert!(s.contains("\"type\":\"retire_child\""), "wire shape: {s}");
        assert!(
            s.contains("\"reason\":\"no longer needed\""),
            "wire shape: {s}"
        );
    }

    #[test]
    fn replace_child_round_trip_carries_new_mandate() {
        let d = Decision::ReplaceChild {
            child_ref: fixed_agent_ref(),
            new_mandate: Mandate::new("retry fetch", Duration::from_millis(250), None),
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
        assert!(s.contains("\"type\":\"replace_child\""), "wire shape: {s}");
        assert!(s.contains("\"text\":\"retry fetch\""), "wire shape: {s}");
    }

    #[test]
    fn reconcile_source_round_trip_field_names() {
        let r = ReconcileSource {
            child_ref: fixed_agent_ref(),
            output_id: fixed_output_id(),
        };
        let v = serde_json::to_value(&r).unwrap();
        assert!(v.get("child_ref").is_some());
        assert!(v.get("output_id").is_some());
        let back: ReconcileSource = serde_json::from_value(v).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn conflict_alternative_round_trip_carries_claim_text_verbatim() {
        let a = ConflictAlternative {
            source_child: fixed_agent_ref(),
            source_output_id: fixed_output_id(),
            claim: "this exact text matters".into(),
        };
        let s = serde_json::to_string(&a).unwrap();
        assert!(s.contains("\"claim\":\"this exact text matters\""));
        let back: ConflictAlternative = serde_json::from_str(&s).unwrap();
        assert_eq!(a, back);
    }

    #[test]
    fn conflict_resolution_round_trip() {
        let r = ConflictResolution {
            chosen_alternative_idx: 3,
            reasoning: "weighted by source reliability".into(),
        };
        let s = serde_json::to_string(&r).unwrap();
        let back: ConflictResolution = serde_json::from_str(&s).unwrap();
        assert_eq!(r, back);
        assert!(s.contains("\"chosen_alternative_idx\":3"));
    }

    // ---- ConflictId surface ---------------------------------------------

    #[test]
    fn conflict_id_is_transparent_serde() {
        let id = ConflictId::from_hex("ab".repeat(32));
        let s = serde_json::to_string(&id).unwrap();
        // Transparent: serializes as the bare hex string with quotes.
        assert!(s.starts_with('"') && s.ends_with('"'));
        let back: ConflictId = serde_json::from_str(&s).unwrap();
        assert_eq!(back, id);
        // Display + as_str round-trip the hex digits verbatim.
        assert_eq!(id.to_string(), "ab".repeat(32));
        assert_eq!(id.as_str(), "ab".repeat(32));
    }

    #[test]
    fn claim_seed_is_transparent() {
        let cs = ClaimSeed::new("abc");
        let s = serde_json::to_string(&cs).unwrap();
        assert_eq!(s, "\"abc\"");
        let back: ClaimSeed = serde_json::from_str(&s).unwrap();
        assert_eq!(back, cs);
        assert_eq!(back.as_str(), "abc");
    }

    #[test]
    fn fs_op_round_trip() {
        let op = FsOp::WriteFile {
            path: "p".into(),
            content: "c".into(),
        };
        let s = serde_json::to_string(&op).unwrap();
        assert!(s.contains("\"op\":\"write_file\""));
        let back: FsOp = serde_json::from_str(&s).unwrap();
        assert_eq!(op, back);

        let del = FsOp::DeleteFile { path: "p".into() };
        let s2 = serde_json::to_string(&del).unwrap();
        assert!(s2.contains("\"op\":\"delete_file\""));
        let back2: FsOp = serde_json::from_str(&s2).unwrap();
        assert_eq!(del, back2);
    }

    // ---- Seed / Session / Decide / MockDecide ---------------------------

    use crate::mandate::Mandate;
    use crate::trigger::Trigger;

    fn dummy_seed() -> Seed {
        Seed::new(
            Mandate::new("research foo", Duration::from_millis(1000), Some(10)),
            vec![Trigger::ScheduledWake],
            FsIndex::default(),
        )
    }

    fn dummy_session() -> Session {
        Session::new(dummy_seed())
    }

    #[tokio::test]
    async fn mock_decide_returns_scripted_decisions_in_order() {
        let script = vec![
            Decision::Read {
                path: "notes/a.md".into(),
            },
            Decision::Idle {
                next_after: Duration::from_millis(100),
            },
        ];
        let mock = MockDecide::new(script.clone());
        assert_eq!(mock.remaining(), 2);

        let first = mock.decide(&dummy_session()).await.unwrap();
        assert_eq!(first, script[0]);
        assert_eq!(mock.remaining(), 1);

        let second = mock.decide(&dummy_session()).await.unwrap();
        assert_eq!(second, script[1]);
        assert_eq!(mock.remaining(), 0);
    }

    #[tokio::test]
    async fn mock_decide_errors_when_script_exhausted() {
        let mock = MockDecide::new(vec![]);
        let err = mock.decide(&dummy_session()).await.unwrap_err();
        assert!(
            err.to_string().contains("script exhausted"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn mock_decide_is_object_safe_via_dyn_decide() {
        // Compile-time check: Decide is dyn-compatible. The run loop will
        // hold an `Arc<dyn Decide>`, so this property is load-bearing.
        let mock: Box<dyn Decide> = Box::new(MockDecide::new(vec![Decision::Idle {
            next_after: Duration::from_millis(10),
        }]));
        let d = mock.decide(&dummy_session()).await.unwrap();
        assert!(matches!(d, Decision::Idle { .. }));
    }

    // ---- idle_after: terminal detection ---------------------------------

    #[test]
    fn idle_is_the_sole_terminal_step() {
        assert_eq!(
            Decision::Idle {
                next_after: Duration::from_millis(250)
            }
            .idle_after(),
            Some(Duration::from_millis(250))
        );
        // Every repertoire step is non-terminal.
        assert!(Decision::Read {
            path: "notes/a.md".into()
        }
        .idle_after()
        .is_none());
        assert!(Decision::List {
            path: "notes/".into()
        }
        .idle_after()
        .is_none());
        assert!(Decision::Search {
            query: "q".into(),
            path: None
        }
        .idle_after()
        .is_none());
        assert!(Decision::RewriteFs { ops: vec![] }.idle_after().is_none());
        assert!(Decision::CallTools { calls: vec![] }.idle_after().is_none());
    }

    // ---- FS-nav round trips ---------------------------------------------

    #[test]
    fn read_round_trip() {
        let d = Decision::Read {
            path: "outputs/abc.json".into(),
        };
        let s = serde_json::to_string(&d).unwrap();
        assert!(s.contains("\"type\":\"read\""), "wire shape: {s}");
        assert!(
            s.contains("\"path\":\"outputs/abc.json\""),
            "wire shape: {s}"
        );
        assert_eq!(d, serde_json::from_str::<Decision>(&s).unwrap());
    }

    #[test]
    fn list_round_trip() {
        let d = Decision::List {
            path: "notes/".into(),
        };
        let s = serde_json::to_string(&d).unwrap();
        assert!(s.contains("\"type\":\"list\""), "wire shape: {s}");
        assert_eq!(d, serde_json::from_str::<Decision>(&s).unwrap());
    }

    #[test]
    fn search_round_trip_with_and_without_path() {
        let scoped = Decision::Search {
            query: "tsmc".into(),
            path: Some("notes/".into()),
        };
        let s = serde_json::to_string(&scoped).unwrap();
        assert!(s.contains("\"type\":\"search\""), "wire shape: {s}");
        assert!(s.contains("\"path\":\"notes/\""), "wire shape: {s}");
        assert_eq!(scoped, serde_json::from_str::<Decision>(&s).unwrap());

        let unscoped = Decision::Search {
            query: "tsmc".into(),
            path: None,
        };
        let s2 = serde_json::to_string(&unscoped).unwrap();
        // `path` omitted from the wire when None (skip_serializing_if).
        assert!(!s2.contains("path"), "wire shape: {s2}");
        assert_eq!(unscoped, serde_json::from_str::<Decision>(&s2).unwrap());
    }

    // ---- Session / Seed / Observation -----------------------------------

    #[test]
    fn session_push_accumulates_steps_in_order() {
        let mut session = dummy_session();
        assert!(session.is_empty());
        session.push(
            Decision::Read {
                path: "notes/a.md".into(),
            },
            Observation::ok("file body"),
        );
        session.push(
            Decision::CallTools { calls: vec![] },
            Observation::err("tool blew up"),
        );
        assert_eq!(session.len(), 2);
        assert_eq!(session.steps[0].observation, Observation::ok("file body"));
        assert!(!session.steps[1].observation.ok);
    }

    #[test]
    fn session_round_trips_through_serde() {
        // The session is serialized as a `decide_step` activity input on the
        // Temporal path, so the round trip is load-bearing.
        let mut session = dummy_session();
        session.push(
            Decision::List {
                path: "notes/".into(),
            },
            Observation::ok("a.md\nb.md"),
        );
        let s = serde_json::to_string(&session).unwrap();
        let back: Session = serde_json::from_str(&s).unwrap();
        assert_eq!(session, back);
    }

    #[test]
    fn fs_index_defaults_to_empty_and_deserializes_from_empty_object() {
        let idx = FsIndex::default();
        assert!(idx.notes.is_empty() && idx.outputs.is_empty());
        let back: FsIndex = serde_json::from_str("{}").unwrap();
        assert_eq!(back, idx);
    }
}
