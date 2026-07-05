//! Activity surface for `AgentWorkflow`. Each activity body is a free
//! `async fn` taking `ActivityContext` and a typed input; the
//! `#[activities]` macro registers them on a value-typed
//! `AgentActivities`. Test-side decision injection lives in
//! [`set_decision_script`] — `decide_step` consults the static
//! script before reaching for the installed [`Decide`].

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use coral_node::agent_core;
use coral_node::agent_ref::{AgentId, GraphId};
use coral_node::conflict::ConflictRecord;
use coral_node::decision::{
    ConflictId, ConflictRecordIntent, Decide, Decision, FsOp, Observation, ReconcileSource, Seed,
    Session, ToolCall,
};
use coral_node::evidence::EvidenceRecord;
use coral_node::fs::{AgentFs, FsError, CANONICAL_OUTPUT};
use coral_node::mandate::{Mandate, OutputId};
use coral_node::model_client::ModelError;
use coral_node::storage::{AgentStorage, BlobSha, VersionedStorage};
use coral_node::trigger::{HumanOp, MandatePatch, Trigger};
use serde::{Deserialize, Serialize};
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use temporalio_sdk::ApplicationFailure;

use crate::worker::{
    agent_storage, agent_versioned_storage_opt, structural_db_store, StructuralDbStore,
};
use crate::workflow::{AgentConfig, FsHandle};

/// Input to [`AgentActivities::build_seed`]. Carries the per-cycle drained
/// signal buckets (`triggers`, `human_ops`, `mandate_patches`) plus the
/// resolved [`Mandate`] + FS handle so the activity can call into
/// [`coral_node::agent_core::build_seed`] for the thin orienting seed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildSeedInput {
    pub mandate: Mandate,
    pub fs_handle: FsHandle,
    pub triggers: Vec<Trigger>,
    /// Human overrides drained alongside `triggers`. Folded into the
    /// `Trigger::HumanOverride` taxonomy and appended after the regular
    /// triggers so ordering matches the in-process loop (one mpsc
    /// receiver, signals serialized in arrival order).
    pub human_ops: Vec<HumanOp>,
    /// Mandate patches drained from the workflow's
    /// `pending_mandate_patches` bucket. The activity records the count
    /// today; consumption is unwired.
    pub mandate_patches: Vec<MandatePatch>,
}

/// Output of [`AgentActivities::build_seed`]. Carries the thin orienting
/// [`Seed`] from `agent_core::build_seed`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BuildSeedOutput {
    pub seed: Seed,
}

/// Input to [`AgentActivities::decide_step`]. Wraps `LlmDecide::decide(&session)`
/// after consulting the test script. The session is rebuilt by the workflow
/// body from prior journaled activity results, so each step's decide sees the
/// full in-cycle history.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecideStepInput {
    pub session: Session,
}

/// Input to [`AgentActivities::read_fs`] — one read-only FS-navigation step
/// (`Read`/`List`/`Search`) against the agent's own filesystem.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadFsInput {
    pub fs_handle: FsHandle,
    pub op: FsNavOp,
}

/// The read-only navigation op a [`ReadFsInput`] carries. Mirrors the
/// `Read`/`List`/`Search` repertoire steps.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum FsNavOp {
    Read {
        path: String,
    },
    List {
        path: String,
    },
    Search {
        query: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
    },
}

impl FsNavOp {
    /// Rebuild the equivalent [`Decision`] so the activity can reuse
    /// [`agent_core::execute_step`]'s rendering for byte-identical
    /// observations with the in-process path.
    fn into_decision(self) -> Decision {
        match self {
            FsNavOp::Read { path } => Decision::Read { path },
            FsNavOp::List { path } => Decision::List { path },
            FsNavOp::Search { query, path } => Decision::Search { query, path },
        }
    }
}

/// Output of [`AgentActivities::read_fs`] — the observation the workflow
/// pushes into the session for the next step to reason over.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadFsOutput {
    pub observation: Observation,
}

/// Input to [`AgentActivities::execute_tool`]. One activity invocation
/// per `ToolCall`; the workflow body fans out via `workflows::join_all`
/// so a partial parallel batch survives a worker crash (only in-flight
/// calls re-execute on retry; completed ones already wrote their
/// outcome to workflow history).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecuteToolInput {
    pub cfg: AgentConfig,
    pub fs_handle: FsHandle,
    /// Graph the calling agent belongs to. Selects the per-graph
    /// [`ToolRegistry`] the dispatch resolves against.
    pub graph_id: GraphId,
    /// The calling agent's assigned tool def ids (`Mandate.tools`). Dispatch
    /// rejects a call whose advertised name resolves to no assigned def.
    pub allowed_tools: Vec<String>,
    pub call: ToolCall,
}

/// Result of a single `execute_tool` activity invocation. Successful
/// calls carry an `EvidenceId`; failed calls carry a structured
/// [`ToolCallFailure`] the workflow can fold into next-tick correction
/// context.
///
/// Mirrors `agent_core::ToolFailure`'s shape with serde derives so the
/// value crosses the workflow ↔ activity boundary via Temporal's
/// payload codec; the source type cannot derive serde directly because
/// `agent_core` is out of scope for this surface.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ToolCallOutcome {
    Success { evidence_path: String },
    Failure { failure: ToolCallFailure },
}

/// Mirror of `coral_node::agent_core::ToolFailure` with serde derives
/// so the value crosses the workflow ↔ activity boundary via Temporal's
/// payload codec.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolCallFailure {
    pub tool: String,
    pub args: serde_json::Value,
    pub error: String,
}

/// Input to [`AgentActivities::persist_output`]. The activity writes the
/// body to the canonical FS path and the version-pinned citation edges to
/// the DB reference graph, then returns the minted `OutputId`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistOutputInput {
    pub cfg: AgentConfig,
    pub fs_handle: FsHandle,
    /// This agent's structural-DB id — keys the `file_index` row and the
    /// `citing`/self-`cited` ends of every citation written for this output.
    pub agent_id: AgentId,
    pub body: String,
    /// `evidence/` paths the output cites — the handles the model copied from
    /// its tool-call and reconcile observations.
    pub citations: Vec<String>,
}

/// Input to [`AgentActivities::apply_fs_ops`].
///
/// Carries a `Mandate` because
/// [`coral_node::fs::AgentFs::new_with_storage`] requires one to reify
/// an `AgentFs` against the shared storage. The mandate is decorative
/// for this call path — `AgentFs::new_with_storage` only writes
/// `mandate.md` when absent, and `apply_fs_ops` runs only against
/// agents that have already gone through `build_seed` at least
/// once (so `mandate.md` already exists on disk). Carrying the real
/// mandate rather than fishing it out of disk keeps the activity body
/// single-storage-roundtrip.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplyFsOpsInput {
    pub fs_handle: FsHandle,
    pub mandate: Mandate,
    pub ops: Vec<FsOp>,
}

/// Input to [`AgentActivities::persist_retirement`]. Carries the reason so
/// retirement is auditable on disk.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistRetirementInput {
    pub fs_handle: FsHandle,
    pub reason: String,
}

/// Input to [`AgentActivities::append_decision_log`]. One entry per
/// tick, written to `<prefix>/decisions/<tick>.jsonl`. Called after
/// [`decide`](crate::workflow) returns a `Decision` so output
/// decisions, retirements, and idle ticks all land in the same
/// artifact stream.
///
/// `decision_summary` is the human-readable rendering of the
/// `Decision` enum variant. The full structured payload is captured
/// by Temporal workflow history; the on-disk log is a host-agnostic,
/// FS-readable, replay-stable summary the TUI consumes without
/// talking to Temporal.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendDecisionLogInput {
    pub fs_handle: FsHandle,
    pub tick: u64,
    /// Step index within the cycle (a cycle takes multiple inner-loop
    /// steps). Each step lands at its own `decisions/<tick>-<step>.jsonl`.
    pub step: u64,
    pub decision_summary: String,
}

/// Input to [`AgentActivities::commit_tick`]. The cycle-boundary commit that
/// snapshots the agent's FS as one tick. `tick` is the completed cycle's
/// counter — it both names the commit (`"tick {tick}"`) and is the only
/// non-deterministic-free field, since it comes from durable workflow state,
/// not wall-clock.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitTickInput {
    pub fs_handle: FsHandle,
    pub tick: u64,
}

/// Input for the `register_child_in_structural_db` activity. Carries
/// the parent's `(graph_id, agent_id)` so the activity can write the
/// child's `agents` row (scoped to the parent's graph) and the
/// parent → child `edges` row in one transaction's worth of writes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterChildInStructuralDbInput {
    pub parent_graph_id: GraphId,
    pub parent_agent_id: AgentId,
    pub child_agent_name: String,
    /// The tool def ids the parent is granting the child (the child's
    /// `Mandate.tools`). Validated against the graph's defined tools before
    /// any row is written — a parent may grant only graph-defined tools.
    pub child_tools: Vec<String>,
}

/// Outcome of the `register_child_in_structural_db` activity.
///
/// A spawn that grants a tool the graph doesn't define is a model error,
/// not an infra failure: it surfaces as `RejectedUnknownTool` (data the
/// workflow folds into next-tick correction) rather than an `ActivityError`,
/// so the parent keeps running and the model can retry with a valid grant.
/// Genuine write failures still surface as `Err` from the activity.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum RegisterChildOutcome {
    /// Child row + parent→child edge written. Carries the freshly-allocated
    /// `AgentId` so the workflow body can construct the child workflow id
    /// (`graphs/<gid>/agents/<aid>`) and pass it to `ctx.child_workflow(..)`.
    Registered { child_agent_id: AgentId },
    /// The grant named a tool def id the graph does not define; nothing was
    /// written. Carries the offending id for the correction text.
    RejectedUnknownTool { tool: String },
}

/// Input to the `reconcile_children` activity. Carries the parent's
/// identity (so the activity can open the parent's FS and write the
/// synthetic evidence) plus the cited child outputs and the optional
/// conflict-record intent. Both `parent_graph_id` and every
/// `sources[i].child_ref` must live in the same graph — cross-graph
/// reads are out of scope.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconcileChildrenInput {
    pub parent_graph_id: GraphId,
    pub parent_agent_id: AgentId,
    pub sources: Vec<ReconcileSource>,
    /// `Some` iff the parent observed disagreement among the cited
    /// outputs. When `Some`, the activity persists a content-addressed
    /// `ConflictRecord` under the parent's `conflicts/<id>.json` and
    /// returns the id on `ReconcileChildrenOutput.conflict_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict: Option<ConflictRecordIntent>,
}

/// Output of the `reconcile_children` activity.
///
/// `synthetic_evidence[i]` is the `evidence/` path of the freshly-minted
/// record for the `sources[i]` cross-agent fold. The reconcile observation
/// surfaces these paths directly, so the parent cites them in a subsequent
/// `WriteOutput` without listing `evidence/` first.
///
/// `conflict_id` is `Some` iff `input.conflict.is_some()` and the
/// activity wrote the conflict record successfully.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconcileChildrenOutput {
    pub synthetic_evidence: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_id: Option<ConflictId>,
}

/// Typed reconciliation errors. The `reconcile_children` activity
/// wraps these as `ApplicationFailure::non_retryable` so Temporal's
/// outer retry loop doesn't churn through them; the workflow body
/// catches the failure and folds it into a session observation the
/// model adapts to on its next step.
#[derive(Debug, thiserror::Error)]
pub enum ReconciliationError {
    /// The named child has no canonical output (`outputs/output.md`) to
    /// fold. Carries the child agent id + the observed output id so the
    /// workflow body's correction text is precise enough for the LLM to
    /// fix on the next tick.
    #[error("reconcile: child output {output_id} not found for agent {agent_id}")]
    ChildOutputNotFound {
        agent_id: AgentId,
        output_id: OutputId,
    },
    /// The parent's `Decision::ReconcileChildren` carried a
    /// `ConflictRecordIntent` with fewer than two alternatives. A
    /// single-alternative "conflict" is meaningless and signals an
    /// LLM-level mistake; non-retryable because re-running with the
    /// same payload won't make a bad shape good.
    #[error("reconcile: conflict intent has only {count} alternatives (need >= 2)")]
    ConflictAlternativesTooFew { count: usize },
}

/// One JSONL entry written by [`AgentActivities::append_decision_log`].
///
/// Wire format (one per line, no trailing newline on the last):
///
/// ```json
/// {"tick": 0, "decision_summary": "Idle { 50ms }", "ts": "2026-05-25T12:00:00Z"}
/// ```
///
/// Pinned as a typed struct (not free-form JSON) so the TUI reader has
/// a stable shape. `#[non_exhaustive]` reserves room for per-tick
/// health / cost meters without a wire break.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct DecisionLogEntry {
    pub tick: u64,
    /// Step index within the cycle. A cycle takes multiple inner-loop steps;
    /// each lands at its own `decisions/<tick>-<step>.jsonl`.
    pub step: u64,
    pub decision_summary: String,
    pub ts: DateTime<Utc>,
}

impl DecisionLogEntry {
    /// Convenience constructor for the workflow body call site.
    pub fn new(tick: u64, step: u64, decision_summary: String, ts: DateTime<Utc>) -> Self {
        Self {
            tick,
            step,
            decision_summary,
            ts,
        }
    }
}

// Test-injectable decision script. Lives outside the impl block because
// activity bodies are free functions over a value-typed registered
// instance — external observation/control of the registered
// `AgentActivities` value isn't available, so a process-wide static is
// the SDK-blessed workaround.

static DECISION_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();

fn script_handle() -> &'static Mutex<VecDeque<Decision>> {
    DECISION_SCRIPT.get_or_init(|| Mutex::new(VecDeque::new()))
}

/// Install a script of decisions the [`AgentActivities::decide_step`]
/// stub returns, in order. Tests call this *before* starting the workflow.
///
/// When the script is empty, the activity falls back to its canned
/// `Decision::Idle { next_after: 1s }` so a misconfigured test doesn't
/// hang. To reset between tests, pass an empty `Vec`.
pub fn set_decision_script(script: Vec<Decision>) {
    let mut q = script_handle()
        .lock()
        .expect("DECISION_SCRIPT mutex poisoned");
    *q = script.into();
}

/// Pop the next scripted decision, or `None` if the script is empty.
fn pop_scripted_decision() -> Option<Decision> {
    script_handle()
        .lock()
        .expect("DECISION_SCRIPT mutex poisoned")
        .pop_front()
}

/// Substantive body of [`AgentActivities::apply_fs_ops`], factored out
/// so hermetic unit tests can drive it against a `MemoryStorage`
/// backend without the live-test-only `ActivityContext` indirection.
/// Returns `anyhow::Result<()>` so the activity-level `?` lifts the
/// error into `ActivityError::Application(...)` via the SDK's blanket
/// impl.
async fn apply_fs_ops_impl(
    storage: std::sync::Arc<dyn coral_node::storage::AgentStorage>,
    input: ApplyFsOpsInput,
) -> anyhow::Result<()> {
    let fs = AgentFs::new_with_storage(storage, &input.fs_handle.prefix, &input.mandate).await?;
    fs.apply_ops(input.ops).await?;
    Ok(())
}

// Free functions extracted from the activity bodies so hermetic tests
// can exercise the FS-touching logic without constructing an
// `ActivityContext` or installing the process-wide
// `OnceLock<AgentStorage>`. The activity body is a 3-line wrapper
// around these helpers; the helpers carry the real shape.

/// Append a single [`DecisionLogEntry`] to the per-tick JSONL file at
/// `<prefix>/decisions/<tick>.jsonl`. Each tick gets its own file with
/// exactly one line. This keeps Temporal-retry idempotency trivial: a
/// retry with the same `(tick, decision_summary, ts)` triple PUTs
/// byte-identical bytes via [`AgentStorage::put`]. The TUI reader
/// concatenates files in tick order.
pub(crate) async fn append_decision_log_impl(
    storage: Arc<dyn AgentStorage>,
    prefix: &str,
    entry: &DecisionLogEntry,
) -> anyhow::Result<()> {
    let fs = AgentFs::attach(storage, prefix);
    let prefix = fs.prefix();
    let key = format!(
        "{prefix}decisions/{tick}-{step}.jsonl",
        tick = entry.tick,
        step = entry.step
    );
    let line = serde_json::to_string(entry)?;
    fs.storage()
        .put(&key, bytes::Bytes::from(line.into_bytes()))
        .await?;
    Ok(())
}

/// Commit one agent's working tree as a single tick via the versioned
/// backend. The message is `"tick {tick}"` — deterministic (sourced from
/// durable workflow state, never wall-clock) so a Temporal retry commits an
/// identical message, and idempotent because a clean tree is a git no-op.
pub(crate) async fn commit_tick_impl(
    storage: Arc<dyn VersionedStorage>,
    prefix: &str,
    tick: u64,
) -> anyhow::Result<()> {
    storage.commit(prefix, &format!("tick {tick}")).await?;
    Ok(())
}

pub(crate) async fn persist_output_impl(
    storage: Arc<dyn AgentStorage>,
    db: Arc<dyn StructuralDbStore>,
    agent_id: AgentId,
    prefix: &str,
    body: &str,
    citations: &[String],
) -> anyhow::Result<OutputId> {
    // Placeholder mandate: `AgentFs` only writes `mandate.md` when
    // absent, so the real mandate persisted by an earlier
    // `build_seed` (or prior agent boot) is not clobbered.
    let mandate = Mandate::new("", Duration::ZERO, None);
    let fs = AgentFs::new_with_storage(storage, prefix, &mandate).await?;
    // FS first: writes the body and enforces that every citation resolves
    // to an evidence record on disk (the output-level provenance gate).
    let output_id = fs.persist_output(body, citations).await?;

    // DB second: the version-pinned reference graph. Both stores are
    // idempotent, so a retried activity converges. The blob sha is computed
    // from the same bytes git would commit, so a reference pinned here stays
    // resolvable against `file_index` (and, once git-backed, the blob).
    let output_sha = BlobSha::of_bytes(body.as_bytes());
    db.set_file_version(agent_id, CANONICAL_OUTPUT, &output_sha)
        .await?;
    for cited in citations {
        let (cited_agent, cited_path, cited_sha) = resolve_cited(&fs, agent_id, cited).await?;
        db.add_citation(
            agent_id,
            CANONICAL_OUTPUT,
            &output_sha,
            cited_agent,
            &cited_path,
            &cited_sha,
        )
        .await?;
    }
    Ok(output_id)
}

/// Resolve a citation's *cited* end. A `reconcile` evidence record cites the
/// CHILD's output at the version folded — a cross-agent edge pinned to the
/// sha captured at fold time (see [`reconcile_children_impl`]). Any other
/// evidence is the agent's own tool-call observation, cited at the evidence
/// file's own blob sha (a within-agent edge). Evidence is immutable, so its
/// pin can never go stale; only the cross-agent output pin can.
async fn resolve_cited(
    fs: &AgentFs,
    self_agent_id: AgentId,
    cited_path: &str,
) -> anyhow::Result<(AgentId, String, BlobSha)> {
    let bytes = fs.read_evidence_bytes(cited_path).await?;
    let record: EvidenceRecord = serde_json::from_slice(&bytes)?;
    if record.tool == "reconcile" {
        let child_agent_id: AgentId = serde_json::from_value(
            record
                .args
                .get("child_agent_id")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("reconcile evidence missing child_agent_id"))?,
        )?;
        let child_sha = record
            .args
            .get("child_output_blob_sha")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("reconcile evidence missing child_output_blob_sha"))?;
        Ok((
            child_agent_id,
            CANONICAL_OUTPUT.to_string(),
            BlobSha::from_hex(child_sha),
        ))
    } else {
        Ok((
            self_agent_id,
            cited_path.to_string(),
            BlobSha::of_bytes(&bytes),
        ))
    }
}

/// Activity bundle registered on the worker. The `#[activities]` macro
/// impls `ActivityImplementer` for the bare type; `register_activities`
/// wraps in `Arc` internally (passing `Arc<AgentActivities>` is a type
/// error).
pub struct AgentActivities;

#[activities]
impl AgentActivities {
    /// Build a per-cycle [`AgentFs`] over the worker-shared `AgentStorage`
    /// at the input's prefix, fold drained `human_ops` into the
    /// `Trigger::HumanOverride` taxonomy, then delegate to
    /// [`agent_core::build_seed`] for the thin orienting seed (mandate +
    /// triggers + pointers-only FS index).
    ///
    /// FS open is idempotent — `AgentFs::new_with_storage` only writes
    /// `mandate.md` when absent, so passing the workflow's mandate
    /// through on every cycle is correct. The cost is one storage `get`
    /// per cycle + a one-time put on first open per agent.
    #[activity]
    pub async fn build_seed(
        _ctx: ActivityContext,
        input: BuildSeedInput,
    ) -> Result<BuildSeedOutput, ActivityError> {
        let storage = crate::worker::agent_storage();
        let fs = AgentFs::new_with_storage(storage, input.fs_handle.prefix.clone(), &input.mandate)
            .await?;

        // Human overrides appended after regular triggers so ordering
        // matches the in-process loop (one mpsc receiver, arrival order).
        let mut triggers = input.triggers;
        triggers.extend(
            input
                .human_ops
                .into_iter()
                .map(|op| Trigger::HumanOverride { op }),
        );

        if !input.mandate_patches.is_empty() {
            tracing::debug!(
                count = input.mandate_patches.len(),
                "build_seed: dropping mandate_patches (unwired)"
            );
        }

        let seed = agent_core::build_seed(&fs, triggers, &input.mandate).await?;
        Ok(BuildSeedOutput { seed })
    }

    /// Wrap the process-wide [`Decide`] impl installed via
    /// [`crate::worker::install_decide`] (typically an `LlmDecide` over
    /// a vendor `ModelClient`).
    ///
    /// Script-first: the activity consults the test-injected
    /// [`DECISION_SCRIPT`] *before* reaching for the installed
    /// implementation, so tests that script every decision never touch
    /// a real LLM.
    ///
    /// Error classification — downcasts the `anyhow::Error` to
    /// `&ModelError`:
    ///
    /// - `ModelError::Transport` / `ModelError::RateLimit` →
    ///   **retryable**. Temporal reschedules per the workflow-side
    ///   `ActivityOptions::retry_policy`.
    /// - `ModelError::Auth` / `ModelError::Parse` /
    ///   `ModelError::Other` → **non-retryable**. Bad credentials,
    ///   malformed responses, vendor-specific 4xxs don't improve on
    ///   retry.
    /// - Downcast fails (e.g. validation-exhaustion `anyhow!`) →
    ///   **non-retryable**. Validation failures bubble as activity
    ///   failures so the workflow can stage a correction context on
    ///   the next tick rather than retry a broken decision in place.
    ///
    /// Heartbeats are omitted: the 30s activity timeout
    /// (`workflow::ACTIVITY_TIMEOUT`) comfortably brackets a normal
    /// LLM call, and the batch-shape `ModelClient` doesn't stream.
    #[activity]
    pub async fn decide_step(
        _ctx: ActivityContext,
        input: DecideStepInput,
    ) -> Result<Decision, ActivityError> {
        // Script-first: scripted decisions short-circuit the installed
        // `Decide` so tests never hit a real LLM.
        if let Some(d) = pop_scripted_decision() {
            return Ok(d);
        }

        let decide = crate::worker::decide_impl();
        decide_with(decide.as_ref(), input)
            .await
            .map_err(classify_decide_error)
    }

    /// Execute one read-only FS-navigation step (`Read`/`List`/`Search`)
    /// against the agent's own filesystem and return the observation the
    /// workflow pushes into the session.
    ///
    /// Reuses [`agent_core::execute_step`] so the observation rendering is
    /// byte-identical to the in-process path. The empty `ToolRegistry` is
    /// never consulted — FS-nav variants don't dispatch tools. `attach`
    /// (not `new_with_storage`) because read-only nav needs neither the
    /// `mandate.md` write nor the tail reconcile.
    ///
    /// A `Read` of a missing file comes back as a failure `Observation`
    /// (the model adapts in-cycle), not an `ActivityError`.
    #[activity]
    pub async fn read_fs(
        _ctx: ActivityContext,
        input: ReadFsInput,
    ) -> Result<ReadFsOutput, ActivityError> {
        let storage = crate::worker::agent_storage();
        let fs = AgentFs::attach(storage, input.fs_handle.prefix.clone());
        let registry = coral_node::tools::ToolRegistry::new();
        let outcome = agent_core::execute_step(&fs, &registry, &input.op.into_decision()).await?;
        Ok(ReadFsOutput {
            observation: outcome.observation,
        })
    }

    /// Dispatch one `ToolCall` through the process-wide
    /// [`ToolRegistry`] (installed via
    /// [`crate::worker::install_tool_registry`]) and, on success,
    /// persist the resulting `EvidenceRecord` via the per-agent
    /// `AgentFs` facade backed by [`crate::worker::agent_storage`].
    ///
    /// One activity invocation per `ToolCall`; the workflow body fans
    /// out N calls via `workflows::join_all` and summarizes the batch
    /// into a session observation (a failure observation when any
    /// surface as `Failure`) the model adapts to on its next step.
    ///
    /// Retry layering: tool calls are dispatched single-shot from this
    /// activity — `McpTool` already runs its own `RetryPolicy` loop
    /// inside `Tool::call`. Wrapping another retry here would compound
    /// them multiplicatively. The outer Temporal retry on activity
    /// errors stays safe because evidence is content-addressed: a
    /// retried invocation with the same `(tool, args, result)` triple
    /// resolves to the same `EvidenceId` and
    /// `AgentFs::record_evidence` is idempotent via `put_if_absent`.
    ///
    /// Tool errors return `Ok(ToolCallOutcome::Failure { failure })`
    /// rather than `ActivityError`: the inner retry already gave up,
    /// and surfacing as `ActivityError` would trip Temporal's outer
    /// retry pointlessly. The workflow body folds the failure into a
    /// session observation the model adapts to. Unknown tool names take
    /// the same path — at per-call granularity they're
    /// observationally identical to any call-time error.
    ///
    /// Heartbeats are deferred: today's tools (`EchoTool` in
    /// microseconds, `McpTool` sub-second under default retry policy)
    /// don't approach the 30s start-to-close timeout. Add a heartbeat
    /// loop when a tool's expected duration approaches it.
    #[activity]
    pub async fn execute_tool(
        _ctx: ActivityContext,
        input: ExecuteToolInput,
    ) -> Result<ToolCallOutcome, ActivityError> {
        // Resolve the calling graph's registry. A build failure (e.g. an
        // MCP server that won't spawn) folds into a tool-call `Failure`,
        // not an `ActivityError` — same path as a call-time error, so the
        // workflow surfaces it as next-tick correction rather than tripping
        // Temporal's outer retry.
        let registry = match crate::worker::tool_registry_provider()
            .registry_for_graph(input.graph_id)
            .await
        {
            Ok(registry) => registry,
            Err(e) => {
                return Ok(ToolCallOutcome::Failure {
                    failure: ToolCallFailure {
                        tool: input.call.name.clone(),
                        args: input.call.args.clone(),
                        error: format!("tool registry unavailable for graph: {e:#}"),
                    },
                });
            }
        };
        // Per-agent scoping: the model calls a tool by its advertised name;
        // allow it only if some def that advertises that name is in the
        // caller's assigned set. A rejection folds into next-tick correction
        // (a `Failure`, not an `ActivityError`) — same path as a call error —
        // so the model sees why and can pick an assigned tool instead.
        if !registry.is_call_allowed(&input.call.name, &input.allowed_tools) {
            return Ok(ToolCallOutcome::Failure {
                failure: ToolCallFailure {
                    tool: input.call.name.clone(),
                    args: input.call.args.clone(),
                    error: format!("tool {:?} is not assigned to this agent", input.call.name),
                },
            });
        }
        // One-shot dispatch — the tool implementation owns its retry
        // policy; another retry layer here would compound them.
        let call_result = registry
            .call(&input.call.name, input.call.args.clone())
            .await;
        match call_result {
            Ok(record) => {
                // Placeholder mandate is safe: `AgentFs::new_with_storage`
                // is read-then-PUT-only-if-absent, so the real mandate
                // written elsewhere is never overwritten.
                let storage = crate::worker::agent_storage();
                let placeholder_mandate = Mandate::new("", Duration::ZERO, None);
                let fs = AgentFs::new_with_storage(
                    storage,
                    input.fs_handle.prefix.clone(),
                    &placeholder_mandate,
                )
                .await
                .map_err(|e| ActivityError::from(anyhow::anyhow!("agent_fs open failed: {e:#}")))?;
                let evidence_path = fs
                    .record_evidence(record, &input.call.claim_seed.0)
                    .await
                    .map_err(|e| {
                        ActivityError::from(anyhow::anyhow!("record_evidence failed: {e:#}"))
                    })?;
                Ok(ToolCallOutcome::Success { evidence_path })
            }
            Err(e) => Ok(ToolCallOutcome::Failure {
                failure: ToolCallFailure {
                    tool: input.call.name.clone(),
                    args: input.call.args.clone(),
                    error: format!("{e:#}"),
                },
            }),
        }
    }

    /// Persist an output across both stores in one activity: the body to
    /// the canonical FS path (enforcing that every cited `EvidenceId`
    /// resolves to a file in `evidence/`), then the version-pinned citation
    /// edges + `file_index` row to the structural DB.
    ///
    /// Idempotency: both writes are idempotent — the FS `put` overwrites the
    /// stable path with byte-identical bytes, and `set_file_version` /
    /// `add_citation` upsert — so a Temporal retry of a successful write +
    /// failed activity ack converges without duplicating a file or an edge.
    #[activity]
    pub async fn persist_output(
        _ctx: ActivityContext,
        input: PersistOutputInput,
    ) -> Result<OutputId, ActivityError> {
        let id = persist_output_impl(
            agent_storage(),
            structural_db_store(),
            input.agent_id,
            &input.fs_handle.prefix,
            &input.body,
            &input.citations,
        )
        .await?;
        Ok(id)
    }

    /// Reify an [`AgentFs`] over the worker-shared storage at the
    /// agent's prefix and apply the batch of [`FsOp`]s. Path
    /// validation (no traversal, must live under `notes/`) is enforced
    /// inside [`AgentFs::apply_ops`].
    ///
    /// Idempotency: `FsOp` is deterministic state — replaying the same
    /// set of writes/deletes against the same prefix produces the same
    /// file state. Mutable, not content-addressed; effectively
    /// idempotent for the Temporal-retry case.
    ///
    /// Error mapping: typed `FsError::PathTraversal` /
    /// `FsError::PathOutsideNotes` / `FsError::Storage` all surface as
    /// `ActivityError::Application(...)` via the SDK's blanket
    /// `From<E> for ActivityError`.
    #[activity]
    pub async fn apply_fs_ops(
        _ctx: ActivityContext,
        input: ApplyFsOpsInput,
    ) -> Result<(), ActivityError> {
        apply_fs_ops_impl(crate::worker::agent_storage(), input).await?;
        Ok(())
    }

    /// Write `retirement.json` via [`AgentFs::persist_retirement`]
    /// using a deterministic timestamp drawn from
    /// `ctx.info().scheduled_time`.
    ///
    /// # Why `AgentFs::attach` (not `new_with_storage`)
    ///
    /// `new_with_storage` reads-or-writes `mandate.md` to confirm
    /// the per-agent FS is initialized. At the retirement-signal
    /// short-circuit no `Mandate` is in scope — the workflow body
    /// never loaded one. `attach` is the strictly weaker constructor
    /// that skips the mandate write and the tail-index reconciliation.
    /// The retirement path writes exactly one key (`retirement.json`)
    /// and exits, so neither side effect is required.
    ///
    /// # Why `scheduled_time` (not `Utc::now()`)
    ///
    /// `Utc::now()` inside an activity body is wall-clock time at
    /// execution. If Temporal retries the activity, the retry's
    /// `Utc::now()` differs from the first attempt's — two attempts
    /// reaching the `put` would write different bytes to
    /// `retirement.json`, defeating workflow-replay byte-identicality.
    /// `ctx.info().scheduled_time` is stamped from workflow history,
    /// so it is stable across retries.
    ///
    /// Fallback: if `scheduled_time` is `None` (test harnesses or an
    /// SDK that hasn't filled it in), synthesize `Utc::now()` so the
    /// body still completes. Costs the replay-determinism property in
    /// that edge case.
    #[activity]
    pub async fn persist_retirement(
        ctx: ActivityContext,
        input: PersistRetirementInput,
    ) -> Result<(), ActivityError> {
        persist_retirement_inner(&input, ctx.info().scheduled_time).await?;
        Ok(())
    }

    /// Append a one-line JSONL entry describing the decision the
    /// workflow just took to `<prefix>/decisions/<tick>.jsonl`. One
    /// activity invocation per tick, called from the workflow body
    /// after `decide_step` returns. Idempotent: `<tick>.jsonl`
    /// is a per-tick file containing exactly one line, and the
    /// timestamp is sourced from `ctx.info().scheduled_time` so
    /// retries PUT byte-identical bytes.
    ///
    /// Fallback: if `scheduled_time` is `None`, synthesize `Utc::now()`
    /// — costs replay-determinism in that edge case; live production
    /// workers always have `scheduled_time` filled in.
    #[activity]
    pub async fn append_decision_log(
        ctx: ActivityContext,
        input: AppendDecisionLogInput,
    ) -> Result<(), ActivityError> {
        let ts: DateTime<Utc> = ctx
            .info()
            .scheduled_time
            .map(DateTime::<Utc>::from)
            .unwrap_or_else(Utc::now);
        let entry = DecisionLogEntry::new(input.tick, input.step, input.decision_summary, ts);
        append_decision_log_impl(agent_storage(), &input.fs_handle.prefix, &entry).await?;
        Ok(())
    }

    /// Commit the agent's FS as one tick at the cycle boundary ("commit =
    /// cycle"). No-ops when no [`VersionedStorage`] backend is installed
    /// (e.g. a hermetic test on a plain `MemoryStorage`), so the workflow can
    /// schedule it unconditionally. Idempotent on retry: a clean tree is a git
    /// no-op, and the `"tick {tick}"` message is deterministic.
    #[activity]
    pub async fn commit_tick(
        _ctx: ActivityContext,
        input: CommitTickInput,
    ) -> Result<(), ActivityError> {
        if let Some(storage) = agent_versioned_storage_opt() {
            commit_tick_impl(storage, &input.fs_handle.prefix, input.tick).await?;
        }
        Ok(())
    }

    /// Write the child's `agents` row + the parent → child `edges`
    /// row into the structural DB, returning the freshly-allocated
    /// `AgentId`. Routed through the worker-shared
    /// [`crate::worker::StructuralDbStore`] trait object (installed
    /// via [`crate::worker::install_structural_db_store`]).
    ///
    /// The activity does **not** write `mandate.md` to the child's
    /// FS — that's the child workflow's first-run `build_seed`
    /// job. Scope is structural state only.
    ///
    /// Idempotency: not provided. Both writes are FK-bound — a
    /// retried activity invocation with a re-allocated child UUID
    /// would create a duplicate child row + duplicate edge. The
    /// schema deliberately doesn't enforce per-graph name uniqueness
    /// (operators may want two children with the same name). Temporal's
    /// at-most-once activity completion keeps duplication rare in
    /// practice.
    #[activity]
    pub async fn register_child_in_structural_db(
        _ctx: ActivityContext,
        input: RegisterChildInStructuralDbInput,
    ) -> Result<RegisterChildOutcome, ActivityError> {
        let store = structural_db_store();
        let out = register_child_in_structural_db_impl(store, input).await?;
        Ok(out)
    }

    /// Fold N cited child outputs into the parent's `evidence/`
    /// directory as synthetic evidence records. One activity
    /// invocation per `Decision::ReconcileChildren`; the workflow body
    /// does NOT push the resulting evidence into any workflow-state
    /// slot — the parent pulls the synthetic records on a later step via
    /// `List`/`Read` of `evidence/`.
    ///
    /// When `input.conflict.is_some()`, the activity persists a
    /// `ConflictRecord` under the parent's `conflicts/<id>.json` and
    /// returns the resulting `ConflictId` in `output.conflict_id`. A
    /// malformed intent (`alternatives.len() < 2`) surfaces as
    /// [`ReconciliationError::ConflictAlternativesTooFew`] via the
    /// same non-retryable path as `ChildOutputNotFound`.
    ///
    /// Error mapping: only typed [`ReconciliationError`]s surface as
    /// `ActivityError::Application(non_retryable)` — the workflow body
    /// catches the failure and folds it into a session observation the
    /// model adapts to. Non-retryable because `ChildOutputNotFound` is
    /// structural; re-running with the same id won't make it resolve.
    ///
    /// Every other error (storage, serde, `record_evidence` write
    /// failures) is surfaced as a *retryable* `ApplicationFailure` so
    /// Temporal's default retry policy gets a chance — a transient
    /// infra blip shouldn't be misreported to the LLM as a provenance
    /// miss.
    ///
    /// `now` is sourced from `ctx.info().scheduled_time` so a retry
    /// PUTs byte-identical bytes under the same content-addressed
    /// `EvidenceId`.
    #[activity]
    pub async fn reconcile_children(
        ctx: ActivityContext,
        input: ReconcileChildrenInput,
    ) -> Result<ReconcileChildrenOutput, ActivityError> {
        let now: DateTime<Utc> = ctx
            .info()
            .scheduled_time
            .map(DateTime::<Utc>::from)
            .unwrap_or_else(Utc::now);
        let storage = agent_storage();
        match reconcile_children_impl(storage, input, now).await {
            Ok(out) => Ok(out),
            Err(e) => {
                let failure = if e.downcast_ref::<ReconciliationError>().is_some() {
                    ApplicationFailure::non_retryable(e)
                } else {
                    // Retryable: synthetic evidence is content-addressed
                    // via `record_evidence`'s `put_if_absent`, so a retry
                    // after a partial completion doesn't duplicate records.
                    ApplicationFailure::new(e)
                };
                Err(ActivityError::application(failure))
            }
        }
    }
}

/// Substantive body of
/// [`AgentActivities::register_child_in_structural_db`], factored out
/// so hermetic / DB-backed integration tests can drive it against any
/// [`crate::worker::StructuralDbStore`] without an `ActivityContext`
/// or the process-wide `OnceLock` install.
pub async fn register_child_in_structural_db_impl(
    store: std::sync::Arc<dyn crate::worker::StructuralDbStore>,
    input: RegisterChildInStructuralDbInput,
) -> anyhow::Result<RegisterChildOutcome> {
    // Validate the grant before writing any rows: a parent may grant only
    // tools the graph defines. A bad grant is a model error, not infra
    // failure — return it as data so the workflow folds it into next-tick
    // correction instead of terminating the parent. Dispatch enforces the
    // same boundary again on the child's own calls.
    if !input.child_tools.is_empty() {
        let defined = store
            .list_tool_def_ids_for_graph(input.parent_graph_id)
            .await?;
        if let Some(tool) = input.child_tools.iter().find(|t| !defined.contains(t)) {
            return Ok(RegisterChildOutcome::RejectedUnknownTool { tool: tool.clone() });
        }
    }
    let child_agent_id = store
        .add_agent(input.parent_graph_id, &input.child_agent_name)
        .await?;
    store
        .add_edge(input.parent_agent_id, child_agent_id)
        .await?;
    Ok(RegisterChildOutcome::Registered { child_agent_id })
}

/// Substantive body of [`AgentActivities::reconcile_children`],
/// factored out for hermetic unit testing against a `MemoryStorage`
/// backend.
///
/// Per-source loop:
///
/// 1. Open the child's FS read-only via [`AgentFs::open_for_agent`].
/// 2. Read the cited [`Output`](coral_node::mandate::Output) via
///    [`AgentFs::read_output`]. On miss, return
///    [`ReconciliationError::ChildOutputNotFound`].
/// 3. Build a synthetic [`EvidenceRecord`] with `tool = "reconcile"`,
///    the `(child_agent_id, child_workflow_id, source_output_id)`
///    triple as `args`, and the serialized child `Output` as
///    `result`. `EvidenceId` is content-addressed over
///    `(tool, args, result)` so the parent's existing provenance
///    contract keeps working with zero extensions.
/// 4. Write the synthetic record to the **parent's** `evidence/`
///    directory via [`AgentFs::record_evidence`].
///
/// Conflict-record write: if `input.conflict.is_some()`, persist a
/// `ConflictRecord` to the parent's `conflicts/<id>.json` via
/// `AgentFs::write_conflict`. The returned `ConflictId` lands in
/// `ReconcileChildrenOutput.conflict_id`. `alternatives.len() < 2`
/// returns `ReconciliationError::ConflictAlternativesTooFew`.
///
/// Error discipline: only a genuine `FsError::OutputNotFound` is
/// wrapped as the typed [`ReconciliationError::ChildOutputNotFound`]
/// (LLM-level mistake, non-retryable). Every other failure (storage,
/// serde, write errors) propagates as a plain `anyhow::Error` for
/// the activity wrapper to mark retryable. Conflating the two would
/// either lie to the LLM about provenance OR skip Temporal's retry.
///
/// Pre-validation pass: every source is read before any
/// `record_evidence` write so a single bad source doesn't leave a
/// partial trail of synthetic evidence on the parent's FS. Atomicity
/// is load-bearing — a partial trail would confuse both the LLM and
/// human reviewers about a reconciliation that never completed.
///
/// `now` is supplied by the caller so the activity sources it from
/// `ctx.info().scheduled_time` — deterministic across retries because
/// the synthetic record's `created_at` is part of the on-disk bytes
/// and a retried activity must PUT byte-identical content under the
/// same content-addressed id.
pub async fn reconcile_children_impl(
    storage: std::sync::Arc<dyn coral_node::storage::AgentStorage>,
    input: ReconcileChildrenInput,
    now: DateTime<Utc>,
) -> anyhow::Result<ReconcileChildrenOutput> {
    // Parent FS — write target. `open_for_agent` uses `attach`
    // semantics (no mandate read, no tail reconcile); the parent's
    // `build_seed` has already written `mandate.md` on its
    // first tick, and `record_evidence` doesn't need it.
    let parent_fs = AgentFs::open_for_agent(
        storage.clone(),
        input.parent_graph_id,
        input.parent_agent_id,
    );

    // Phase 1: read every child output up-front. Only
    // `FsError::OutputNotFound` becomes the typed reconcile error;
    // storage / serde failures bubble as anyhow and stay retryable
    // at the activity layer.
    let mut child_outputs = Vec::with_capacity(input.sources.len());
    for source in &input.sources {
        // Cross-agent read: both agents share `parent_graph_id`
        // (cross-graph reads are out of scope).
        let child_fs = AgentFs::open_for_agent(
            storage.clone(),
            input.parent_graph_id,
            source.child_ref.agent_id,
        );
        // Read the child's single canonical Output (current version). The
        // `source.output_id` the parent named is the version it observed at
        // decide time; the current body may be newer (kept-current
        // semantics) — reconciling the latest is correct, and V6 re-wakes
        // the parent if it drifts again.
        let child_output = child_fs.read_output().await.map_err(|e| {
            if matches!(e.downcast_ref::<FsError>(), Some(FsError::OutputNotFound)) {
                anyhow::Error::new(ReconciliationError::ChildOutputNotFound {
                    agent_id: source.child_ref.agent_id,
                    output_id: source.output_id.clone(),
                })
            } else {
                // Bubble verbatim so the activity wrapper marks it
                // retryable.
                e
            }
        })?;
        child_outputs.push(child_output);
    }

    // Phase 2: write one synthetic evidence record per source.
    let mut synthetic_evidence = Vec::with_capacity(input.sources.len());
    for (source, child_output) in input.sources.iter().zip(child_outputs.iter()) {
        // `tool = "reconcile"` is the wire-locked discriminator; do
        // NOT introduce a new EvidenceKind / sub-tool taxonomy.
        //
        // Pin the blob sha of the body actually *read* here, not the
        // `source.output_id` the parent signaled (the two can differ under
        // kept-current semantics). `persist_output` turns this into the
        // cross-agent citation's `cited_blob_sha`, so the pin records the
        // exact child version this reconcile folded — pin-what-you-read.
        let args = serde_json::json!({
            "child_agent_id": source.child_ref.agent_id,
            "child_workflow_id": source.child_ref.workflow_id,
            "source_output_id": source.output_id,
            "child_output_blob_sha": BlobSha::of_bytes(child_output.as_bytes()).as_str(),
        });
        let result = serde_json::to_value(child_output)?;
        let record = EvidenceRecord::new("reconcile", args, result, now);
        let slug_seed = format!("reconcile {}", source.child_ref.workflow_id);
        let ev_path = parent_fs.record_evidence(record, &slug_seed).await?;
        synthetic_evidence.push(ev_path);
    }

    // Persist the conflict record (if any). The record is
    // content-addressed over `(alternatives, resolution)` so a retried
    // activity PUTs byte-identical bytes under the same key
    // (`AgentFs::write_conflict` rides `put_if_absent`).
    //
    // Pre-check `alternatives.len() < 2` so the typed
    // `ReconciliationError::ConflictAlternativesTooFew` reaches the
    // workflow body — defence in depth, mirroring how
    // `ChildOutputNotFound` is mapped at the cross-agent read site.
    //
    // `kind` is not set here — `ConflictRecord::new` derives it from
    // `resolution.is_some()`.
    let conflict_id = if let Some(intent) = input.conflict {
        if intent.alternatives.len() < 2 {
            return Err(ReconciliationError::ConflictAlternativesTooFew {
                count: intent.alternatives.len(),
            }
            .into());
        }
        let record = ConflictRecord::new(now, intent.alternatives, intent.resolution);
        Some(parent_fs.write_conflict(&record).await?)
    } else {
        None
    };

    Ok(ReconcileChildrenOutput {
        synthetic_evidence,
        conflict_id,
    })
}

/// Body of [`AgentActivities::persist_retirement`], factored out so
/// hermetic tests can call it without constructing an
/// `ActivityContext`. Sources the storage backend from
/// [`agent_storage`] and the timestamp from `scheduled_time` — both
/// load-bearing for the activity contract. See the activity doc for
/// why `scheduled_time` and not `Utc::now()`.
pub async fn persist_retirement_inner(
    input: &PersistRetirementInput,
    scheduled_time: Option<std::time::SystemTime>,
) -> anyhow::Result<()> {
    let storage = agent_storage();
    let fs = AgentFs::attach(storage, &input.fs_handle.prefix);
    let retired_at: DateTime<Utc> = scheduled_time
        .map(DateTime::<Utc>::from)
        .unwrap_or_else(Utc::now);
    fs.persist_retirement(&input.reason, retired_at).await?;
    Ok(())
}

/// Call the supplied [`Decide`] with the activity's input. Separated
/// from [`AgentActivities::decide_step`] so hermetic tests can
/// inject an arbitrary `Decide` without going through the
/// `worker::decide_impl()` static.
async fn decide_with(decide: &dyn Decide, input: DecideStepInput) -> anyhow::Result<Decision> {
    decide.decide(&input.session).await
}

/// Map an `anyhow::Error` from `Decide::decide` to a Temporal
/// [`ActivityError`] with retryability flagged per the categorization
/// rules in [`AgentActivities::decide_step`].
///
/// Downcasts to `&ModelError` to extract the category; `LlmDecide`
/// wraps typed `ModelError` via `anyhow::Error::new` so the source
/// chain preserves it. Non-`ModelError` causes fall through to
/// non-retryable: validation failures don't retry at the activity
/// layer, they become correction contexts in the next workflow tick.
fn classify_decide_error(err: anyhow::Error) -> ActivityError {
    let retryable = matches!(
        err.downcast_ref::<ModelError>(),
        Some(ModelError::Transport(_)) | Some(ModelError::RateLimit(_))
    );
    let failure = if retryable {
        ApplicationFailure::new(err)
    } else {
        ApplicationFailure::non_retryable(err)
    };
    ActivityError::application(failure)
}

// Compile-time witness that `crate::worker::decide_impl()` returns
// exactly `Arc<dyn Decide>`. The activity body passes the result
// through `Arc::as_ref`, which only works if the function returns an
// `Arc`-shaped trait object. Never invoked — calling `decide_impl()`
// here would panic when no `Decide` is installed.
const _: fn() = || {
    fn assert_arc_dyn_decide() -> Arc<dyn Decide> {
        crate::worker::decide_impl()
    }
    let _ = assert_arc_dyn_decide;
};

#[cfg(test)]
mod tests {
    //! Hermetic unit coverage for the activity surface. Live tests in
    //! `tests/workflow_loop.rs` exercise the activities through the
    //! real workflow against a Temporal Server.

    use super::*;
    use coral_node::decision::MockDecide;
    use serde_json::json;

    // Serializes the two tests below that mutate the process-wide
    // `DECISION_SCRIPT` static. Without this they race under cargo's
    // default parallel runner.
    static SCRIPT_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Build an empty `Session` for tests that exercise the activity
    /// body. `Mandate::new("", Duration::ZERO, None)` is the cheapest
    /// valid construction.
    fn empty_session() -> Session {
        Session::new(coral_node::decision::Seed::new(
            Mandate::new("", Duration::ZERO, None),
            Vec::new(),
            coral_node::decision::FsIndex::default(),
        ))
    }

    #[test]
    fn decision_script_round_trips_in_order() {
        let _g = SCRIPT_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        // Reset (subsequent tests inherit process state).
        set_decision_script(vec![]);

        set_decision_script(vec![
            Decision::Idle {
                next_after: Duration::from_millis(100),
            },
            Decision::WriteOutput {
                body: "test".into(),
                citations: vec![],
            },
        ]);
        let first = pop_scripted_decision();
        assert!(matches!(
            first,
            Some(Decision::Idle {
                next_after,
            }) if next_after == Duration::from_millis(100)
        ));
        let second = pop_scripted_decision();
        assert!(matches!(second, Some(Decision::WriteOutput { body, .. }) if body == "test"));
        // Drained — falls back to None.
        assert!(pop_scripted_decision().is_none());
    }

    #[test]
    fn decision_script_resets_between_tests() {
        let _g = SCRIPT_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        set_decision_script(vec![Decision::WriteOutput {
            body: "first".into(),
            citations: vec![],
        }]);
        set_decision_script(vec![Decision::Idle {
            next_after: Duration::from_secs(5),
        }]);
        // Second `set_decision_script` replaces, not appends.
        let got = pop_scripted_decision();
        assert!(
            matches!(got, Some(Decision::Idle { next_after }) if next_after == Duration::from_secs(5))
        );
        assert!(pop_scripted_decision().is_none());
    }

    #[test]
    fn build_seed_input_empty_buckets_pin_shape() {
        // No `Default` derive (real `Mandate` has none). Explicit
        // construction so future non-`Default` fields force the same
        // bucket-init discipline.
        let i = BuildSeedInput {
            mandate: Mandate::new("", Duration::ZERO, None),
            fs_handle: FsHandle::default(),
            triggers: Vec::new(),
            human_ops: Vec::new(),
            mandate_patches: Vec::new(),
        };
        assert!(i.triggers.is_empty());
        assert!(i.human_ops.is_empty());
        assert!(i.mandate_patches.is_empty());
    }

    #[test]
    fn build_seed_input_round_trips_through_json() {
        let i = BuildSeedInput {
            mandate: Mandate::new("test", Duration::from_millis(100), Some(4)),
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            triggers: vec![Trigger::ScheduledWake],
            human_ops: vec![HumanOp::new(json!({"action": "pause"}))],
            mandate_patches: vec![MandatePatch::new(json!({"model": "x"}))],
        };
        let s = serde_json::to_string(&i).unwrap();
        let back: BuildSeedInput = serde_json::from_str(&s).unwrap();
        assert_eq!(i, back);
    }

    #[test]
    fn read_fs_input_round_trips_through_json() {
        let i = ReadFsInput {
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            op: FsNavOp::Search {
                query: "tsmc".into(),
                path: Some("notes/".into()),
            },
        };
        let s = serde_json::to_string(&i).unwrap();
        let back: ReadFsInput = serde_json::from_str(&s).unwrap();
        assert_eq!(i.op, back.op);
    }

    #[test]
    fn tool_call_outcome_round_trips_through_json() {
        let oc = ToolCallOutcome::Success {
            evidence_path: "evidence/t-aabbccdd.json".to_string(),
        };
        let s = serde_json::to_string(&oc).unwrap();
        assert!(s.contains("\"outcome\":\"success\""), "wire shape: {s}");
        let _back: ToolCallOutcome = serde_json::from_str(&s).unwrap();

        let f = ToolCallFailure {
            tool: "errbomb".into(),
            args: json!({"x": 1}),
            error: "boom".into(),
        };
        let oc2 = ToolCallOutcome::Failure { failure: f };
        let s2 = serde_json::to_string(&oc2).unwrap();
        assert!(s2.contains("\"outcome\":\"failure\""), "wire shape: {s2}");
        let _back2: ToolCallOutcome = serde_json::from_str(&s2).unwrap();
    }

    #[test]
    fn execute_tool_input_round_trips_through_json() {
        use coral_node::decision::ClaimSeed;
        let graph_id = GraphId::new(uuid::Uuid::from_u128(0x9c));
        let i = ExecuteToolInput {
            cfg: AgentConfig::default(),
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            graph_id,
            allowed_tools: vec!["echo-tool".into()],
            call: ToolCall::new("echo", json!({"msg": "hi"}), ClaimSeed::new("s")),
        };
        let s = serde_json::to_string(&i).unwrap();
        let back: ExecuteToolInput = serde_json::from_str(&s).unwrap();
        assert_eq!(back.graph_id, graph_id);
        assert_eq!(back.allowed_tools, vec!["echo-tool".to_string()]);
    }

    #[test]
    fn persist_output_input_round_trips_through_json() {
        let i = PersistOutputInput {
            cfg: AgentConfig::default(),
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            agent_id: AgentId::new(uuid::Uuid::from_u128(0xa1)),
            body: "claim".into(),
            citations: vec!["evidence/t-aabbccdd.json".to_string()],
        };
        let s = serde_json::to_string(&i).unwrap();
        let _back: PersistOutputInput = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn apply_fs_ops_input_round_trips_through_json() {
        let i = ApplyFsOpsInput {
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            mandate: Mandate::new("test", Duration::from_millis(100), None),
            ops: vec![FsOp::WriteFile {
                path: "n/x.md".into(),
                content: "hi".into(),
            }],
        };
        let s = serde_json::to_string(&i).unwrap();
        let _back: ApplyFsOpsInput = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn persist_retirement_input_round_trips_through_json() {
        let i = PersistRetirementInput {
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            reason: "done".into(),
        };
        let s = serde_json::to_string(&i).unwrap();
        let _back: PersistRetirementInput = serde_json::from_str(&s).unwrap();
    }

    /// Bespoke `Decide` impl that returns the supplied error verbatim
    /// on every `decide` call. Drives the activity body's error
    /// classification path without standing up a full `LlmDecide`.
    struct ErrDecide {
        make_err: fn() -> anyhow::Error,
    }

    #[async_trait::async_trait]
    impl Decide for ErrDecide {
        async fn decide(&self, _session: &Session) -> anyhow::Result<Decision> {
            Err((self.make_err)())
        }
    }

    /// Happy path: `decide_with` forwards the bundle to the trait
    /// method and returns the trait's decision verbatim.
    #[tokio::test]
    async fn decide_with_returns_trait_decision_on_success() {
        let want = Decision::Idle {
            next_after: Duration::from_millis(250),
        };
        let decide: Arc<dyn Decide> = Arc::new(MockDecide::new(vec![want.clone()]));
        let input = DecideStepInput {
            session: empty_session(),
        };
        let got = decide_with(decide.as_ref(), input).await.unwrap();
        assert_eq!(got, want);
    }

    /// Transport failures classify as retryable. The activity body
    /// surfaces this via `ActivityError::Application(_)` carrying an
    /// `ApplicationFailure` with `is_non_retryable() == false`, so
    /// Temporal's default retry policy will reschedule.
    #[test]
    fn classify_decide_error_transport_is_retryable() {
        let err = anyhow::Error::new(ModelError::Transport("DNS failure".into()));
        let activity_err = classify_decide_error(err);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(
            !failure.is_non_retryable(),
            "Transport errors must be retryable"
        );
    }

    /// Rate-limit failures classify as retryable. Vendor-side backoff
    /// handling lives outside the activity.
    #[test]
    fn classify_decide_error_rate_limit_is_retryable() {
        let err = anyhow::Error::new(ModelError::RateLimit("slow down".into()));
        let activity_err = classify_decide_error(err);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(
            !failure.is_non_retryable(),
            "RateLimit errors must be retryable"
        );
    }

    /// Auth failures classify as non-retryable. Bad credentials don't
    /// fix themselves; surface to the workflow body as a terminal
    /// activity failure on the first attempt.
    #[test]
    fn classify_decide_error_auth_is_non_retryable() {
        let err = anyhow::Error::new(ModelError::Auth("ANTHROPIC_API_KEY missing".into()));
        let activity_err = classify_decide_error(err);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(
            failure.is_non_retryable(),
            "Auth errors must be non-retryable"
        );
    }

    /// `Parse` and `Other` failures classify as non-retryable. Bad
    /// response shapes and vendor-specific 4xxs don't get better by
    /// retrying; the workflow body's next-tick correction is the right
    /// place to surface them.
    #[test]
    fn classify_decide_error_parse_and_other_are_non_retryable() {
        for err in [
            anyhow::Error::new(ModelError::Parse("bad JSON".into())),
            anyhow::Error::new(ModelError::Other("4xx".into())),
        ] {
            let activity_err = classify_decide_error(err);
            let ActivityError::Application(failure) = activity_err else {
                panic!("expected ActivityError::Application");
            };
            assert!(
                failure.is_non_retryable(),
                "Parse/Other errors must be non-retryable"
            );
        }
    }

    // Exercise the substantive `apply_fs_ops_impl` body against a
    // `MemoryStorage` backend. Bypasses `worker::agent_storage()` and
    // the `ActivityContext` (unconstructable without `Arc<CoreWorker>`).
    // `FsError` and `MemoryStorage` are imported further down.

    fn fresh_storage_and_input(ops: Vec<FsOp>) -> (Arc<dyn AgentStorage>, ApplyFsOpsInput) {
        let storage: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
        let input = ApplyFsOpsInput {
            fs_handle: FsHandle {
                prefix: "graphs/g/agents/a".into(),
            },
            mandate: Mandate::new("hermetic", Duration::from_millis(100), None),
            ops,
        };
        (storage, input)
    }

    #[tokio::test]
    async fn apply_fs_ops_writes_both_notes_files_under_prefix() {
        let (storage, input) = fresh_storage_and_input(vec![
            FsOp::WriteFile {
                path: "notes/a.md".into(),
                content: "alpha".into(),
            },
            FsOp::WriteFile {
                path: "notes/sub/b.md".into(),
                content: "bravo".into(),
            },
        ]);

        apply_fs_ops_impl(storage.clone(), input)
            .await
            .expect("apply_fs_ops_impl");

        // Hit the backend directly so the assertion doesn't couple to
        // `AgentFs` read methods.
        let a = storage
            .get("graphs/g/agents/a/notes/a.md")
            .await
            .expect("get a")
            .expect("a present");
        assert_eq!(a.as_ref(), b"alpha");
        let b = storage
            .get("graphs/g/agents/a/notes/sub/b.md")
            .await
            .expect("get b")
            .expect("b present");
        assert_eq!(b.as_ref(), b"bravo");
    }

    #[tokio::test]
    async fn apply_fs_ops_rejects_traversal_and_leaves_fs_untouched() {
        // First write a known-good note so we can prove the second
        // batch's traversal op didn't clobber it.
        let (storage, seed_input) = fresh_storage_and_input(vec![FsOp::WriteFile {
            path: "notes/keep.md".into(),
            content: "preserved".into(),
        }]);
        apply_fs_ops_impl(storage.clone(), seed_input)
            .await
            .expect("seed apply_fs_ops_impl");

        let traversal_input = ApplyFsOpsInput {
            fs_handle: FsHandle {
                prefix: "graphs/g/agents/a".into(),
            },
            mandate: Mandate::new("hermetic", Duration::from_millis(100), None),
            ops: vec![FsOp::WriteFile {
                path: "../outside.md".into(),
                content: "escape".into(),
            }],
        };
        let err = apply_fs_ops_impl(storage.clone(), traversal_input)
            .await
            .expect_err("traversal op must reject");
        let downcast = err.downcast_ref::<FsError>().expect("typed FsError");
        assert!(
            matches!(downcast, FsError::PathTraversal(_)),
            "expected PathTraversal, got {downcast:?}"
        );

        // Original file unchanged.
        let still_there = storage
            .get("graphs/g/agents/a/notes/keep.md")
            .await
            .expect("get keep")
            .expect("keep present");
        assert_eq!(still_there.as_ref(), b"preserved");

        // No `../outside.md`-shaped key landed under the agent prefix
        // (or at the root). Scan via `list` because escape keys could
        // appear anywhere.
        let all = storage
            .list("", None, usize::MAX)
            .await
            .expect("list all keys");
        for key in &all.keys {
            assert!(
                !key.contains("outside"),
                "traversal write leaked to backend: {key}"
            );
        }
    }

    /// Non-`ModelError` causes (e.g. `LlmDecide`'s parse-exhaustion
    /// `anyhow!(...)`) classify as non-retryable; validation failures
    /// don't retry at the activity layer, they become correction
    /// contexts on the next workflow tick.
    #[test]
    fn classify_decide_error_non_model_error_is_non_retryable() {
        let err = anyhow::anyhow!("LlmDecide: parse failed on all 2 attempt(s)");
        let activity_err = classify_decide_error(err);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(
            failure.is_non_retryable(),
            "non-ModelError causes must be non-retryable"
        );
    }

    /// End-to-end of the `decide_with` + `classify_decide_error` pair:
    /// when the trait returns a transport-flavored error, the call
    /// site lands on a retryable `ApplicationFailure`. Closes the loop
    /// with the same shape the `#[activity]` body uses.
    #[tokio::test]
    async fn decide_with_then_classify_transport_yields_retryable_failure() {
        let decide: Arc<dyn Decide> = Arc::new(ErrDecide {
            make_err: || anyhow::Error::new(ModelError::Transport("downstream 503".into())),
        });
        let input = DecideStepInput {
            session: empty_session(),
        };
        let raw = decide_with(decide.as_ref(), input).await.unwrap_err();
        let activity_err = classify_decide_error(raw);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(!failure.is_non_retryable());
    }

    /// End-to-end with a parse-exhaustion `anyhow!` (the canonical
    /// `LlmDecide` validation-failure shape). The activity layer must
    /// surface this as non-retryable so the workflow body's correction
    /// path takes over on the next tick.
    #[tokio::test]
    async fn decide_with_then_classify_validation_yields_non_retryable_failure() {
        let decide: Arc<dyn Decide> = Arc::new(ErrDecide {
            make_err: || anyhow::anyhow!("LlmDecide: parse failed on all 2 attempts"),
        });
        let input = DecideStepInput {
            session: empty_session(),
        };
        let raw = decide_with(decide.as_ref(), input).await.unwrap_err();
        let activity_err = classify_decide_error(raw);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(failure.is_non_retryable());
    }

    // `persist_output_impl` hermetic coverage — exercises the
    // extracted free helper without an `ActivityContext` or the
    // process-wide `OnceLock<AgentStorage>` install. Each test creates
    // its own `MemoryStorage` and exercises the storage-prefix shape
    // `<graph_id>/<agent_id>/`.

    use chrono::Utc;
    use coral_node::evidence::EvidenceRecord;
    use coral_node::fs::FsError;
    use coral_node::storage::MemoryStorage;

    /// Plant an evidence record under `prefix` so a subsequent
    /// `persist_output_impl` referencing the returned id passes the
    /// provenance check. Shared between the happy-path test and the
    /// failure tests so the planting shape doesn't drift between them.
    async fn plant_evidence(
        storage: Arc<dyn coral_node::storage::AgentStorage>,
        prefix: &str,
        tool: &str,
        args: serde_json::Value,
        result: serde_json::Value,
    ) -> String {
        // Same storage Arc + prefix the activity will open against —
        // `MemoryStorage` is in-process state, not a connected backend,
        // so a separate instance would not share evidence. Returns the
        // `evidence/` path — the handle a citation names.
        let mandate = Mandate::new("plant", Duration::from_millis(0), None);
        let fs = AgentFs::new_with_storage(storage, prefix, &mandate)
            .await
            .expect("open planting AgentFs");
        let rec = EvidenceRecord::new(tool, args, result, Utc::now());
        fs.record_evidence(rec, tool).await.expect("plant evidence")
    }

    #[tokio::test]
    async fn persist_output_impl_writes_output_with_resolved_evidence() {
        let storage: Arc<dyn coral_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let prefix = "graphs/g1/agents/a1/";

        // Plant two evidence records the output will cite.
        let id_a = plant_evidence(
            storage.clone(),
            prefix,
            "tool_a",
            json!({"q": "alpha"}),
            json!({"r": 1}),
        )
        .await;
        let id_b = plant_evidence(
            storage.clone(),
            prefix,
            "tool_b",
            json!({"q": "beta"}),
            json!({"r": 2}),
        )
        .await;
        assert_ne!(id_a, id_b);

        let db = Arc::new(MemoryStructuralDbStore::new());
        let agent_id = AgentId::new(uuid::Uuid::from_u128(0xa1));
        let out_id = persist_output_impl(
            storage.clone(),
            db.clone(),
            agent_id,
            prefix,
            "claim X",
            &[id_a.clone(), id_b.clone()],
        )
        .await
        .expect("persist_output_impl ok");

        // Inspect via a fresh `AgentFs` view: the single canonical
        // output holds the body, and the returned id fingerprints it.
        let mandate = Mandate::new("inspect", Duration::from_millis(0), None);
        let fs = AgentFs::new_with_storage(storage, prefix, &mandate)
            .await
            .unwrap();
        assert_eq!(fs.read_output().await.expect("read output"), "claim X");
        assert_eq!(
            out_id,
            OutputId::new("claim X"),
            "returned id must fingerprint the body"
        );

        // DB reference graph: `file_index` pins the output's blob sha, and
        // each cited evidence becomes a within-agent citation edge pinned to
        // the evidence file's own blob sha.
        let output_sha = BlobSha::of_bytes(b"claim X");
        assert_eq!(
            db.current_sha(agent_id, CANONICAL_OUTPUT).as_ref(),
            Some(&output_sha),
            "output version pinned in file_index"
        );
        let cites = db.citations();
        assert_eq!(cites.len(), 2, "one citation per cited evidence");
        for (cited_path, cite) in [&id_a, &id_b].iter().zip(cites.iter()) {
            assert_eq!(cite.citing_agent, agent_id);
            assert_eq!(cite.citing_path, CANONICAL_OUTPUT);
            assert_eq!(cite.citing_sha, output_sha);
            assert_eq!(
                cite.cited_agent, agent_id,
                "own tool-call evidence is a within-agent edge"
            );
            assert_eq!(&cite.cited_path, *cited_path);
            let ev_bytes = fs.read_evidence_bytes(cited_path).await.unwrap();
            assert_eq!(
                cite.cited_sha,
                BlobSha::of_bytes(&ev_bytes),
                "evidence pinned at its own blob sha"
            );
        }
    }

    /// FS↔DB consistency (14c): an interrupted write leaves a recoverable
    /// orphan (never a dangling pointer), and Temporal's at-least-once retry
    /// *is* the consistency mechanism. The first attempt fails on the citation
    /// insert *after* the FS write + `file_index` upsert — a crash between the
    /// two DB halves — so the output is on the FS and version-pinned but has no
    /// citation. Re-running the (idempotent) activity completes the citation
    /// and converges, duplicating neither the `file_index` nor the citation.
    #[tokio::test]
    async fn persist_output_impl_converges_after_crash_between_halves() {
        let storage: Arc<dyn coral_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let prefix = "graphs/g1/agents/a-retry/";
        let cited = plant_evidence(
            storage.clone(),
            prefix,
            "tool_c",
            json!({"q": "gamma"}),
            json!({"r": 3}),
        )
        .await;

        // Fail only the first citation insert; the retry succeeds.
        let db = Arc::new(MemoryStructuralDbStore::with_citation_failures(1));
        let agent_id = AgentId::new(uuid::Uuid::from_u128(0xa3));
        let output_sha = BlobSha::of_bytes(b"claim R");

        let err = persist_output_impl(
            storage.clone(),
            db.clone(),
            agent_id,
            prefix,
            "claim R",
            &[cited.clone()],
        )
        .await
        .expect_err("first attempt fails on the citation half");
        assert!(
            err.to_string().contains("simulated add_citation failure"),
            "unexpected error: {err}"
        );

        // Recoverable state: the FS holds the output and `file_index` pinned
        // its version, but no citation landed — an orphan, not a dangling
        // pointer (FS-first / DB-second).
        {
            let mandate = Mandate::new("inspect", Duration::from_millis(0), None);
            let fs = AgentFs::new_with_storage(storage.clone(), prefix, &mandate)
                .await
                .unwrap();
            assert_eq!(fs.read_output().await.expect("read output"), "claim R");
        }
        assert_eq!(
            db.current_sha(agent_id, CANONICAL_OUTPUT).as_ref(),
            Some(&output_sha),
            "file_index pinned even though the citation half failed"
        );
        assert!(
            db.citations().is_empty(),
            "no citation recorded from the failed attempt"
        );

        // Retry (Temporal reschedules the activity) → converges.
        persist_output_impl(
            storage.clone(),
            db.clone(),
            agent_id,
            prefix,
            "claim R",
            &[cited.clone()],
        )
        .await
        .expect("retry converges");

        assert_eq!(
            db.file_index_len(),
            1,
            "retry's file_index upsert must not duplicate the row"
        );
        let cites = db.citations();
        assert_eq!(cites.len(), 1, "exactly one citation after retry — no dupe");
        assert_eq!(cites[0].citing_sha, output_sha);
        assert_eq!(cites[0].cited_path, cited);
    }

    #[tokio::test]
    async fn persist_output_impl_rejects_unresolved_evidence_id() {
        let storage: Arc<dyn coral_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let prefix = "graphs/g1/agents/a-missing/";

        // Reference an evidence path that was never planted — the
        // `AgentFs::persist_output` provenance check fires.
        let bogus = "evidence/tool-x-deadbeef.json".to_string();
        let db = Arc::new(MemoryStructuralDbStore::new());
        let agent_id = AgentId::new(uuid::Uuid::from_u128(0xa2));
        let err = persist_output_impl(
            storage.clone(),
            db.clone(),
            agent_id,
            prefix,
            "claim Y",
            &[bogus.clone()],
        )
        .await
        .expect_err("must fail on unresolved evidence path");
        let typed = err.downcast_ref::<FsError>().expect("typed FsError");
        match typed {
            FsError::EvidenceNotFound(missing) => assert_eq!(missing, &bogus),
            other => panic!("expected EvidenceNotFound, got {other:?}"),
        }

        // No output written.
        let mandate = Mandate::new("inspect", Duration::from_millis(0), None);
        let fs = AgentFs::new_with_storage(storage, prefix, &mandate)
            .await
            .unwrap();
        let err = fs
            .read_output()
            .await
            .expect_err("no output should have been written");
        assert!(matches!(
            err.downcast_ref::<FsError>(),
            Some(FsError::OutputNotFound)
        ));

        // The FS provenance gate runs before the DB write, so a rejected
        // output leaves the reference graph untouched.
        assert!(
            db.citations().is_empty(),
            "rejected output writes no citation edges"
        );
        assert!(
            db.current_sha(agent_id, CANONICAL_OUTPUT).is_none(),
            "rejected output writes no file_index row"
        );
    }

    #[tokio::test]
    async fn persist_output_impl_rejects_empty_evidence_list() {
        // Provenance contract: an output with no evidence is rejected
        // before the file write.
        let storage: Arc<dyn coral_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let prefix = "graphs/g1/agents/a-empty/";

        let db = Arc::new(MemoryStructuralDbStore::new());
        let agent_id = AgentId::new(uuid::Uuid::from_u128(0xa3));
        let err = persist_output_impl(storage, db, agent_id, prefix, "claim Z", &[])
            .await
            .expect_err("must fail on empty evidence");
        let typed = err.downcast_ref::<FsError>().expect("typed FsError");
        assert!(matches!(typed, FsError::EmptyEvidence));
    }

    #[tokio::test]
    async fn apply_fs_ops_rejects_path_outside_notes() {
        let (storage, input) = fresh_storage_and_input(vec![FsOp::WriteFile {
            path: "outputs/x.json".into(),
            content: "wrong dir".into(),
        }]);
        let err = apply_fs_ops_impl(storage.clone(), input)
            .await
            .expect_err("non-notes path must reject");
        let downcast = err.downcast_ref::<FsError>().expect("typed FsError");
        assert!(
            matches!(downcast, FsError::PathOutsideNotes(_)),
            "expected PathOutsideNotes, got {downcast:?}"
        );
    }

    #[tokio::test]
    async fn apply_fs_ops_replay_is_idempotent_for_writes() {
        // Models the Temporal retry path: same input, applied twice,
        // must leave file state identical.
        let (storage, _) = fresh_storage_and_input(vec![]);
        let ops_a = vec![
            FsOp::WriteFile {
                path: "notes/a.md".into(),
                content: "v1".into(),
            },
            FsOp::WriteFile {
                path: "notes/b.md".into(),
                content: "v1".into(),
            },
        ];
        let input_a = ApplyFsOpsInput {
            fs_handle: FsHandle {
                prefix: "graphs/g/agents/a".into(),
            },
            mandate: Mandate::new("hermetic", Duration::from_millis(100), None),
            ops: ops_a,
        };
        apply_fs_ops_impl(storage.clone(), input_a.clone())
            .await
            .unwrap();
        apply_fs_ops_impl(storage.clone(), input_a).await.unwrap();

        let a = storage
            .get("graphs/g/agents/a/notes/a.md")
            .await
            .unwrap()
            .unwrap();
        let b = storage
            .get("graphs/g/agents/a/notes/b.md")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.as_ref(), b"v1");
        assert_eq!(b.as_ref(), b"v1");
    }

    /// `append_decision_log_impl` writes exactly
    /// `<prefix>decisions/<tick>.jsonl` containing one JSON line that
    /// deserializes back to the same entry.
    #[tokio::test]
    async fn append_decision_log_impl_writes_per_tick_jsonl() {
        let storage: Arc<dyn coral_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let prefix = "graphs/g/agents/a";
        let ts = DateTime::parse_from_rfc3339("2026-05-25T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let entry = DecisionLogEntry::new(7, 2, "Idle { 50ms }".into(), ts);
        append_decision_log_impl(storage.clone(), prefix, &entry)
            .await
            .expect("append_decision_log_impl ok");

        // File lands at `<prefix>/decisions/<tick>-<step>.jsonl` with the
        // single JSON line we wrote.
        let key = "graphs/g/agents/a/decisions/7-2.jsonl";
        let bytes = storage
            .get(key)
            .await
            .expect("storage.get ok")
            .unwrap_or_else(|| panic!("expected key {key}"));
        let line = std::str::from_utf8(bytes.as_ref()).unwrap();
        // No trailing newline; one JSONL line.
        assert!(
            !line.ends_with('\n'),
            "no trailing newline in single-line file, got: {line:?}"
        );
        let parsed: DecisionLogEntry = serde_json::from_str(line).unwrap();
        assert_eq!(parsed, entry);
    }

    /// Temporal-retry idempotency: re-running the helper with the
    /// same `(tick, decision_summary, ts)` triple writes byte-identical
    /// bytes. Load-bearing because the activity sources `ts` from
    /// `ctx.info().scheduled_time` (stable across retries).
    #[tokio::test]
    async fn append_decision_log_impl_is_idempotent_on_replay() {
        let storage: Arc<dyn coral_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let prefix = "graphs/g/agents/replay";
        let ts = DateTime::parse_from_rfc3339("2026-05-25T13:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let entry = DecisionLogEntry::new(0, 0, "Retire { 'done' }".into(), ts);
        append_decision_log_impl(storage.clone(), prefix, &entry)
            .await
            .unwrap();
        let first = storage
            .get("graphs/g/agents/replay/decisions/0-0.jsonl")
            .await
            .unwrap()
            .unwrap();
        append_decision_log_impl(storage.clone(), prefix, &entry)
            .await
            .unwrap();
        let second = storage
            .get("graphs/g/agents/replay/decisions/0-0.jsonl")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.as_ref(), second.as_ref());
    }

    /// Spy [`VersionedStorage`] recording every `commit(prefix, message)` so a
    /// hermetic test can assert the cycle-boundary commit's deterministic
    /// message without a git backend. The data plane delegates to an inner
    /// `MemoryStorage` — `commit_tick_impl` only commits, so it stays untouched.
    struct CommitSpy {
        inner: MemoryStorage,
        commits: std::sync::Mutex<Vec<(String, String)>>,
    }

    impl CommitSpy {
        fn new() -> Self {
            Self {
                inner: MemoryStorage::new(),
                commits: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl AgentStorage for CommitSpy {
        async fn put(
            &self,
            key: &str,
            value: bytes::Bytes,
        ) -> coral_node::storage::StorageResult<()> {
            self.inner.put(key, value).await
        }
        async fn put_if_absent(
            &self,
            key: &str,
            value: bytes::Bytes,
        ) -> coral_node::storage::StorageResult<coral_node::storage::PutOutcome> {
            self.inner.put_if_absent(key, value).await
        }
        async fn get(&self, key: &str) -> coral_node::storage::StorageResult<Option<bytes::Bytes>> {
            self.inner.get(key).await
        }
        async fn get_many(
            &self,
            keys: &[&str],
        ) -> coral_node::storage::StorageResult<Vec<Option<bytes::Bytes>>> {
            self.inner.get_many(keys).await
        }
        async fn delete(&self, key: &str) -> coral_node::storage::StorageResult<()> {
            self.inner.delete(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            after: Option<&str>,
            limit: usize,
        ) -> coral_node::storage::StorageResult<coral_node::storage::ListPage> {
            self.inner.list(prefix, after, limit).await
        }
    }

    #[async_trait::async_trait]
    impl VersionedStorage for CommitSpy {
        async fn commit(
            &self,
            agent_prefix: &str,
            message: &str,
        ) -> coral_node::storage::StorageResult<Vec<(String, BlobSha)>> {
            self.commits
                .lock()
                .unwrap()
                .push((agent_prefix.to_string(), message.to_string()));
            Ok(Vec::new())
        }
        async fn read_at(
            &self,
            _agent_prefix: &str,
            _sha: &BlobSha,
        ) -> coral_node::storage::StorageResult<Option<bytes::Bytes>> {
            Ok(None)
        }
    }

    /// `commit_tick_impl` commits the agent's prefix with the deterministic
    /// `"tick {tick}"` message — and a retry (Temporal at-least-once) replays
    /// the identical message, the property that keeps commit-per-tick safe.
    #[tokio::test]
    async fn commit_tick_impl_commits_with_deterministic_tick_message() {
        let spy = Arc::new(CommitSpy::new());
        let storage: Arc<dyn VersionedStorage> = spy.clone();
        let prefix = "graphs/g/agents/a";

        commit_tick_impl(storage.clone(), prefix, 7).await.unwrap();
        commit_tick_impl(storage, prefix, 7).await.unwrap();

        let commits = spy.commits.lock().unwrap();
        assert_eq!(
            *commits,
            vec![
                (prefix.to_string(), "tick 7".to_string()),
                (prefix.to_string(), "tick 7".to_string()),
            ],
            "commit_tick_impl must commit (prefix, \"tick {{tick}}\") deterministically across retries"
        );
    }

    /// The full provenance-retention chain over the production git backend:
    /// `persist_output_impl` (FS write + DB version-pin) → `commit_tick_impl`
    /// (cycle-boundary commit) → refresh the canonical output next cycle →
    /// the sha pinned at the first cycle still resolves to the original bytes
    /// via `read_at`, even though HEAD now holds the refreshed output. The
    /// individual links each have a unit test; this is the only hermetic
    /// coverage of them composed on a real `PerAgentGitStorage` (the worker
    /// install seam is exercised only by the live persistent-monitor run).
    #[tokio::test]
    async fn pinned_output_resolves_after_refresh_over_git_backend() {
        let tmp = tempfile::TempDir::new().unwrap();
        let git = Arc::new(coral_node::storage::PerAgentGitStorage::new(tmp.path()).unwrap());
        let storage: Arc<dyn coral_node::storage::AgentStorage> = git.clone();
        let versioned: Arc<dyn VersionedStorage> = git.clone();
        let prefix = "graphs/g1/agents/a1/";

        // One immutable evidence record both cycles cite — the provenance
        // gate must pass for either output to persist.
        let cite = plant_evidence(
            storage.clone(),
            prefix,
            "tool_a",
            json!({"q": "alpha"}),
            json!({"r": 1}),
        )
        .await;

        let db = Arc::new(MemoryStructuralDbStore::new());
        let agent_id = AgentId::new(uuid::Uuid::from_u128(0xa1));

        // Cycle 0: write the first output, then commit the cycle. The sha
        // pinned in `file_index` is exactly what a parent citing this output
        // would pin — same bytes, same blob sha.
        let body_v1 = "first conclusion";
        persist_output_impl(
            storage.clone(),
            db.clone(),
            agent_id,
            prefix,
            body_v1,
            &[cite.clone()],
        )
        .await
        .unwrap();
        let pinned = db
            .current_sha(agent_id, CANONICAL_OUTPUT)
            .expect("output pinned in file_index");
        commit_tick_impl(versioned.clone(), prefix, 0)
            .await
            .unwrap();

        // Cycle 1: refresh the canonical output (same path, new bytes), commit.
        let body_v2 = "revised conclusion";
        persist_output_impl(
            storage.clone(),
            db.clone(),
            agent_id,
            prefix,
            body_v2,
            &[cite.clone()],
        )
        .await
        .unwrap();
        commit_tick_impl(versioned.clone(), prefix, 1)
            .await
            .unwrap();

        // The refresh moved the canonical path's current version, so the
        // earlier pin is no longer current — the detectable staleness that
        // drives re-reconciliation.
        assert_eq!(
            db.current_sha(agent_id, CANONICAL_OUTPUT),
            Some(BlobSha::of_bytes(body_v2.as_bytes())),
            "file_index now points at the refreshed version"
        );
        assert_ne!(
            db.current_sha(agent_id, CANONICAL_OUTPUT),
            Some(pinned.clone()),
            "the pinned version is no longer current"
        );

        // HEAD reflects the refresh...
        let mandate = Mandate::new("inspect", Duration::from_millis(0), None);
        let fs = AgentFs::new_with_storage(storage, prefix, &mandate)
            .await
            .unwrap();
        assert_eq!(
            fs.read_output().await.unwrap(),
            body_v2,
            "HEAD is the refresh"
        );

        // ...yet the version pinned at cycle 0 still resolves to its original
        // bytes: git retained the blob the citation points at across the
        // overwrite. This is the time-scrubbable provenance the reference
        // graph promises.
        let resolved = versioned
            .read_at(prefix, &pinned)
            .await
            .unwrap()
            .expect("pinned version still resolves after refresh");
        assert_eq!(resolved.as_ref(), body_v1.as_bytes());
    }

    /// In-memory `StructuralDbStore` fake. Records every `add_agent`
    /// / `add_edge` call so hermetic tests can assert without Postgres.
    /// Extracted to a struct (rather than a tuple) to keep clippy's
    /// `type_complexity` lint happy and give the assertions readable
    /// field names.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RecordedAgent {
        graph_id: GraphId,
        name: String,
        allocated_id: AgentId,
    }

    /// One recorded `add_citation` call — a version-pinned edge.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RecordedCitation {
        citing_agent: AgentId,
        citing_path: String,
        citing_sha: BlobSha,
        cited_agent: AgentId,
        cited_path: String,
        cited_sha: BlobSha,
    }

    struct MemoryStructuralDbStore {
        agents: std::sync::Mutex<Vec<RecordedAgent>>,
        edges: std::sync::Mutex<Vec<(AgentId, AgentId)>>,
        file_index: std::sync::Mutex<Vec<(AgentId, String, BlobSha)>>,
        citations: std::sync::Mutex<Vec<RecordedCitation>>,
        defined_tools: Vec<String>,
        /// Number of upcoming `add_citation` calls to fail before recording,
        /// simulating a crash between the two DB halves so a retry can prove
        /// convergence. `0` (the default) means never fail.
        citation_failures: std::sync::atomic::AtomicUsize,
    }

    impl MemoryStructuralDbStore {
        fn new() -> Self {
            Self::with_tools(Vec::new())
        }

        fn with_tools(defined_tools: Vec<String>) -> Self {
            Self {
                agents: std::sync::Mutex::new(Vec::new()),
                edges: std::sync::Mutex::new(Vec::new()),
                file_index: std::sync::Mutex::new(Vec::new()),
                citations: std::sync::Mutex::new(Vec::new()),
                defined_tools,
                citation_failures: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        /// Fail the next `n` `add_citation` calls before recording, then
        /// succeed — models an interrupted write (crash between the FS write /
        /// `file_index` upsert and the citation insert).
        fn with_citation_failures(n: usize) -> Self {
            let s = Self::new();
            s.citation_failures
                .store(n, std::sync::atomic::Ordering::SeqCst);
            s
        }

        /// Current sha bound to `(agent, path)` — last write wins, mirroring
        /// the real upsert. `None` if the path was never written.
        fn current_sha(&self, agent: AgentId, path: &str) -> Option<BlobSha> {
            self.file_index
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find(|(a, p, _)| *a == agent && p == path)
                .map(|(_, _, sha)| sha.clone())
        }

        fn file_index_len(&self) -> usize {
            self.file_index.lock().unwrap().len()
        }

        fn citations(&self) -> Vec<RecordedCitation> {
            self.citations.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl crate::worker::StructuralDbStore for MemoryStructuralDbStore {
        async fn add_agent(&self, graph_id: GraphId, name: &str) -> anyhow::Result<AgentId> {
            let id = AgentId::new(uuid::Uuid::new_v4());
            self.agents.lock().unwrap().push(RecordedAgent {
                graph_id,
                name: name.to_string(),
                allocated_id: id,
            });
            Ok(id)
        }

        async fn add_edge(
            &self,
            parent_agent_id: AgentId,
            child_agent_id: AgentId,
        ) -> anyhow::Result<()> {
            self.edges
                .lock()
                .unwrap()
                .push((parent_agent_id, child_agent_id));
            Ok(())
        }

        async fn list_tool_def_ids_for_graph(
            &self,
            _graph_id: GraphId,
        ) -> anyhow::Result<Vec<String>> {
            Ok(self.defined_tools.clone())
        }

        async fn set_file_version(
            &self,
            agent_id: AgentId,
            filepath: &str,
            blob_sha: &BlobSha,
        ) -> anyhow::Result<()> {
            // Upsert on `(agent, path)`, mirroring the real store's
            // `INSERT … ON CONFLICT (agent_id, filepath) DO UPDATE` — a retry
            // re-pins in place rather than duplicating the row.
            let mut fi = self.file_index.lock().unwrap();
            fi.retain(|(a, p, _)| !(*a == agent_id && p == filepath));
            fi.push((agent_id, filepath.to_string(), blob_sha.clone()));
            Ok(())
        }

        #[allow(clippy::too_many_arguments)]
        async fn add_citation(
            &self,
            citing_agent_id: AgentId,
            citing_filepath: &str,
            citing_blob_sha: &BlobSha,
            cited_agent_id: AgentId,
            cited_filepath: &str,
            cited_blob_sha: &BlobSha,
        ) -> anyhow::Result<()> {
            if self
                .citation_failures
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0
            {
                self.citation_failures
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                anyhow::bail!("simulated add_citation failure (crash between halves)");
            }
            let row = RecordedCitation {
                citing_agent: citing_agent_id,
                citing_path: citing_filepath.to_string(),
                citing_sha: citing_blob_sha.clone(),
                cited_agent: cited_agent_id,
                cited_path: cited_filepath.to_string(),
                cited_sha: cited_blob_sha.clone(),
            };
            // Dedup on the full 6-column edge, mirroring the real store's
            // `UNIQUE` idempotent insert — a retry converges to one row.
            let mut cites = self.citations.lock().unwrap();
            let present = cites.iter().any(|c| {
                c.citing_agent == row.citing_agent
                    && c.citing_path == row.citing_path
                    && c.citing_sha == row.citing_sha
                    && c.cited_agent == row.cited_agent
                    && c.cited_path == row.cited_path
                    && c.cited_sha == row.cited_sha
            });
            if !present {
                cites.push(row);
            }
            Ok(())
        }
    }

    /// Activity-body hermetic coverage: the helper writes one agent
    /// row + one parent → child edge with the right endpoints, and
    /// the returned child id matches the recorded agent row's id.
    #[tokio::test]
    async fn register_child_in_structural_db_impl_records_agent_name_and_edge_endpoints() {
        let fake = std::sync::Arc::new(MemoryStructuralDbStore::new());
        let store: std::sync::Arc<dyn crate::worker::StructuralDbStore> = fake.clone();

        let parent_graph_id = GraphId::new(uuid::Uuid::new_v4());
        let parent_agent_id = AgentId::new(uuid::Uuid::new_v4());

        let out = register_child_in_structural_db_impl(
            store,
            RegisterChildInStructuralDbInput {
                parent_graph_id,
                parent_agent_id,
                child_agent_name: "fetcher".into(),
                child_tools: Vec::new(),
            },
        )
        .await
        .expect("activity body ok");

        let RegisterChildOutcome::Registered { child_agent_id } = out else {
            panic!("expected Registered, got {out:?}");
        };
        let agents = fake.agents.lock().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].graph_id, parent_graph_id);
        assert_eq!(agents[0].name, "fetcher");
        assert_eq!(agents[0].allocated_id, child_agent_id);

        let edges = fake.edges.lock().unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].0, parent_agent_id);
        assert_eq!(edges[0].1, child_agent_id);
    }

    /// A granted tool the graph defines is accepted; one it doesn't is
    /// rejected as data (no rows written) so the workflow can fold it into
    /// next-tick correction rather than terminating the parent.
    #[tokio::test]
    async fn register_child_validates_granted_tools_against_graph_defs() {
        let parent_graph_id = GraphId::new(uuid::Uuid::new_v4());
        let parent_agent_id = AgentId::new(uuid::Uuid::new_v4());

        // Grant a subset of the graph's defs → registered.
        let fake = std::sync::Arc::new(MemoryStructuralDbStore::with_tools(vec![
            "web-search".into(),
            "x-search".into(),
        ]));
        let store: std::sync::Arc<dyn crate::worker::StructuralDbStore> = fake.clone();
        let out = register_child_in_structural_db_impl(
            store,
            RegisterChildInStructuralDbInput {
                parent_graph_id,
                parent_agent_id,
                child_agent_name: "fetcher".into(),
                child_tools: vec!["web-search".into()],
            },
        )
        .await
        .expect("subset grant ok");
        assert!(matches!(out, RegisterChildOutcome::Registered { .. }));
        assert_eq!(fake.agents.lock().unwrap().len(), 1);

        // Grant a tool the graph doesn't define → rejected, nothing written.
        let fake2 = std::sync::Arc::new(MemoryStructuralDbStore::with_tools(vec![
            "web-search".into()
        ]));
        let store2: std::sync::Arc<dyn crate::worker::StructuralDbStore> = fake2.clone();
        let out = register_child_in_structural_db_impl(
            store2,
            RegisterChildInStructuralDbInput {
                parent_graph_id,
                parent_agent_id,
                child_agent_name: "rogue".into(),
                child_tools: vec!["web-search".into(), "rm-rf".into()],
            },
        )
        .await
        .expect("validation surfaces as data, not error");
        assert!(matches!(
            out,
            RegisterChildOutcome::RejectedUnknownTool { ref tool } if tool == "rm-rf"
        ));
        assert!(fake2.agents.lock().unwrap().is_empty());
        assert!(fake2.edges.lock().unwrap().is_empty());
    }

    /// Pin the wire shape of the activity's input/output types so a
    /// future field addition shows up as a test miss. Live coverage of
    /// the activity body lives in the integration test gated on
    /// `TEMPORAL_LIVE_TEST=1` + `DATABASE_URL`.
    #[test]
    fn register_child_input_round_trips_through_json() {
        use uuid::Uuid;
        let i = RegisterChildInStructuralDbInput {
            parent_graph_id: GraphId::new(
                Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
            ),
            parent_agent_id: AgentId::new(
                Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap(),
            ),
            child_agent_name: "fetcher".into(),
            child_tools: vec!["web-search".into()],
        };
        let s = serde_json::to_string(&i).unwrap();
        let back: RegisterChildInStructuralDbInput = serde_json::from_str(&s).unwrap();
        assert_eq!(i, back);
        assert!(
            s.contains("\"child_agent_name\":\"fetcher\""),
            "wire shape: {s}"
        );
    }

    #[test]
    fn register_child_output_round_trips_through_json() {
        use uuid::Uuid;
        let registered = RegisterChildOutcome::Registered {
            child_agent_id: AgentId::new(
                Uuid::parse_str("bbbbbbbb-cccc-dddd-eeee-ffffffffffff").unwrap(),
            ),
        };
        let s = serde_json::to_string(&registered).unwrap();
        assert_eq!(
            serde_json::from_str::<RegisterChildOutcome>(&s).unwrap(),
            registered
        );

        let rejected = RegisterChildOutcome::RejectedUnknownTool {
            tool: "rm-rf".into(),
        };
        let s = serde_json::to_string(&rejected).unwrap();
        assert_eq!(
            serde_json::from_str::<RegisterChildOutcome>(&s).unwrap(),
            rejected
        );
    }

    /// Wire-shape check for `AppendDecisionLogInput` + `DecisionLogEntry`.
    /// Both cross the workflow ↔ activity boundary via Temporal's payload
    /// codec (serde-backed), so a round-trip through `serde_json` is a
    /// cheap proxy.
    #[test]
    fn decision_log_types_round_trip_through_json() {
        let i = AppendDecisionLogInput {
            fs_handle: FsHandle {
                prefix: "g/a".into(),
            },
            tick: 42,
            step: 1,
            decision_summary: "CallTools { 3 calls }".into(),
        };
        let s = serde_json::to_string(&i).unwrap();
        let back: AppendDecisionLogInput = serde_json::from_str(&s).unwrap();
        assert_eq!(i, back);

        let ts = DateTime::parse_from_rfc3339("2026-05-25T14:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let e = DecisionLogEntry::new(42, 1, "WriteOutput { citations: 1 }".into(), ts);
        let s2 = serde_json::to_string(&e).unwrap();
        let back2: DecisionLogEntry = serde_json::from_str(&s2).unwrap();
        assert_eq!(e, back2);
    }

    /// Deterministic timestamp for the synthetic-evidence records the
    /// reconcile activity writes. `EvidenceId` hashes
    /// `(tool, args, result)`, NOT `created_at`, but the on-disk JSON
    /// bytes do include the timestamp.
    fn fixed_now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-05-25T15:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    /// Plant a child agent's FS root with one persisted output (citing
    /// one planted evidence record) and return the (`child_agent_id`,
    /// child workflow id string, child `OutputId`, planted evidence
    /// id) tuple. Shared between the happy + failure-mode tests.
    async fn plant_child_output(
        storage: Arc<dyn coral_node::storage::AgentStorage>,
        graph_id: GraphId,
        child_agent_id: AgentId,
        content: &str,
    ) -> (String, OutputId, String) {
        let child_prefix = format!("graphs/{graph_id}/agents/{child_agent_id}/");
        let mandate = Mandate::new("child", Duration::from_millis(0), None);
        let fs = AgentFs::new_with_storage(storage.clone(), &child_prefix, &mandate)
            .await
            .expect("open child FS");
        let ev = fs
            .record_evidence(
                EvidenceRecord::new(
                    "echo",
                    json!({"q": content}),
                    json!({"r": "child result"}),
                    fixed_now(),
                ),
                "echo",
            )
            .await
            .expect("plant child evidence");
        let out = fs
            .persist_output(content, &[ev.clone()])
            .await
            .expect("plant child output");
        // Canonical scheme minted by `FsHandle::for_agent`.
        let child_workflow_id = format!("graphs/{graph_id}/agents/{child_agent_id}");
        (child_workflow_id, out, ev)
    }

    #[tokio::test]
    async fn reconcile_children_impl_writes_one_synthetic_evidence_per_source() {
        use coral_node::agent_ref::AgentRef;
        use uuid::Uuid;

        let storage: Arc<dyn coral_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let graph_id = GraphId::new(Uuid::new_v4());
        let parent_agent_id = AgentId::new(Uuid::new_v4());
        let child_a_id = AgentId::new(Uuid::new_v4());
        let child_b_id = AgentId::new(Uuid::new_v4());

        // Two distinct child outputs the parent will fold in.
        let (child_a_wf, child_a_out, _ev_a) =
            plant_child_output(storage.clone(), graph_id, child_a_id, "claim from A").await;
        let (child_b_wf, child_b_out, _ev_b) =
            plant_child_output(storage.clone(), graph_id, child_b_id, "claim from B").await;

        let input = ReconcileChildrenInput {
            parent_graph_id: graph_id,
            parent_agent_id,
            sources: vec![
                ReconcileSource {
                    child_ref: AgentRef::new(child_a_wf.clone(), child_a_id),
                    output_id: child_a_out.clone(),
                },
                ReconcileSource {
                    child_ref: AgentRef::new(child_b_wf.clone(), child_b_id),
                    output_id: child_b_out.clone(),
                },
            ],
            conflict: None,
        };

        let out = reconcile_children_impl(storage.clone(), input, fixed_now())
            .await
            .expect("reconcile_children_impl ok");

        assert_eq!(out.synthetic_evidence.len(), 2);
        assert!(
            out.conflict_id.is_none(),
            "no conflict intent: conflict_id must be None",
        );

        // Verify both synthetic evidence records landed under the
        // parent's prefix with the right `tool` + `args` shape.
        let parent_view = AgentFs::open_for_agent(storage, graph_id, parent_agent_id);
        let evs = parent_view
            .list_recent_evidence(8)
            .await
            .expect("list parent evidence");
        assert_eq!(
            evs.len(),
            2,
            "expected exactly two synthetic evidence records under parent's prefix"
        );
        for ev in &evs {
            assert_eq!(
                ev.tool, "reconcile",
                "synthetic record's tool must lock to \"reconcile\""
            );
            // `args` carries the (child_agent_id, child_workflow_id,
            // source_output_id) triple. The serde wire form is a JSON
            // object; pull the fields out for spot-checks rather than
            // pinning a full string match (canonicalization sorts keys).
            let args = ev.args.as_object().expect("args is a JSON object");
            assert!(args.contains_key("child_agent_id"));
            assert!(args.contains_key("child_workflow_id"));
            assert!(args.contains_key("source_output_id"));
        }
        // Each returned EvidenceId resolves on disk under parent's prefix.
        for id in &out.synthetic_evidence {
            parent_view
                .evidence_must_exist(id)
                .await
                .unwrap_or_else(|e| panic!("synthetic evidence {id} not on disk: {e:#}"));
        }
    }

    /// A persistent parent folds successive outputs from the *same* child
    /// over time: reconciling a newer output writes a second, distinct
    /// synthetic record (the parent's report can refresh), and re-citing an
    /// already-folded output is idempotent (`record_evidence` rides
    /// `put_if_absent`) — so a re-seen `output_id` refreshes without looping
    /// or duplicating evidence. This is why CM-4 needs no runtime
    /// already-seen guard.
    #[tokio::test]
    async fn reconcile_children_impl_folds_newer_output_and_is_idempotent_on_reseen() {
        use coral_node::agent_ref::AgentRef;
        use uuid::Uuid;

        let storage: Arc<dyn coral_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let graph_id = GraphId::new(Uuid::new_v4());
        let parent_agent_id = AgentId::new(Uuid::new_v4());
        let child_id = AgentId::new(Uuid::new_v4());

        // The same child emits two outputs over time (same workflow id,
        // distinct output ids).
        let (child_wf, out1, _) =
            plant_child_output(storage.clone(), graph_id, child_id, "first report").await;
        let (child_wf2, out2, _) =
            plant_child_output(storage.clone(), graph_id, child_id, "second report").await;
        assert_eq!(child_wf, child_wf2, "same child ⇒ same workflow id");
        assert_ne!(out1, out2, "two emits ⇒ distinct output ids");

        let reconcile_one = |output_id: OutputId| ReconcileChildrenInput {
            parent_graph_id: graph_id,
            parent_agent_id,
            sources: vec![ReconcileSource {
                child_ref: AgentRef::new(child_wf.clone(), child_id),
                output_id,
            }],
            conflict: None,
        };

        // Tick 1: fold the first output.
        let r1 = reconcile_children_impl(storage.clone(), reconcile_one(out1.clone()), fixed_now())
            .await
            .expect("reconcile out1");
        assert_eq!(r1.synthetic_evidence.len(), 1);

        // Later tick: fold the newer output → a second, distinct record so
        // the parent's refreshed report can cite it.
        let r2 = reconcile_children_impl(storage.clone(), reconcile_one(out2.clone()), fixed_now())
            .await
            .expect("reconcile out2");
        assert_eq!(r2.synthetic_evidence.len(), 1);
        assert_ne!(
            r1.synthetic_evidence[0], r2.synthetic_evidence[0],
            "a newer output must produce distinct synthetic evidence"
        );

        let parent_view = AgentFs::open_for_agent(storage.clone(), graph_id, parent_agent_id);
        let after_two = parent_view
            .list_recent_evidence(8)
            .await
            .expect("list parent evidence");
        assert_eq!(after_two.len(), 2, "two folded outputs ⇒ two records");

        // The citation half of the contract: each reconcile is followed by a
        // refreshed parent Output citing the synthetic evidence it just
        // produced. The kept-current Output is overwritten, so the two emits
        // mint distinct ids while the canonical body reflects the latest. The
        // provenance check inside `persist_output` passes because reconcile
        // wrote that evidence under the parent's own prefix.
        let parent_prefix = format!("graphs/{graph_id}/agents/{parent_agent_id}");
        let db = Arc::new(MemoryStructuralDbStore::new());
        let out_v1 = persist_output_impl(
            storage.clone(),
            db.clone(),
            parent_agent_id,
            &parent_prefix,
            "consolidated report (folds A)",
            &[r1.synthetic_evidence[0].clone()],
        )
        .await
        .expect("parent emits refreshed report v1");
        let out_v2 = persist_output_impl(
            storage.clone(),
            db.clone(),
            parent_agent_id,
            &parent_prefix,
            "consolidated report (folds A + newer B)",
            &[r2.synthetic_evidence[0].clone()],
        )
        .await
        .expect("parent emits refreshed report v2");
        assert_ne!(out_v1, out_v2, "two refreshed reports ⇒ distinct outputs");

        // Kept-current semantics: the canonical Output reflects the latest
        // refresh (v2), not an accumulation.
        assert_eq!(
            parent_view.read_output().await.expect("read parent output"),
            "consolidated report (folds A + newer B)",
            "parent's canonical output must be the latest refreshed report"
        );

        // Re-cite the already-folded first output: idempotent. Same
        // content-addressed id, no new record — the parent never loops.
        let r1_again =
            reconcile_children_impl(storage.clone(), reconcile_one(out1.clone()), fixed_now())
                .await
                .expect("re-reconcile out1");
        assert_eq!(
            r1_again.synthetic_evidence, r1.synthetic_evidence,
            "re-reconciling an already-folded output yields the same evidence id"
        );
        let after_reseen = parent_view
            .list_recent_evidence(8)
            .await
            .expect("list parent evidence after re-seen");
        assert_eq!(
            after_reseen.len(),
            2,
            "re-seen output must not add a synthetic record"
        );
    }

    /// The cross-agent reference edge and the staleness-detection prerequisite.
    /// A child writes its output (pinning its own `file_index` sha); the parent
    /// folds it via reconcile and refreshes its Output citing the synthetic
    /// evidence. The resulting citation must be a CROSS-AGENT edge to the
    /// child's canonical output, pinned to the exact version folded — and that
    /// pinned sha must equal the child's own `file_index` sha (the same bytes
    /// flow through the reconcile read and the child write). If that equality
    /// drifts, the staleness reactor silently never fires. When the child later
    /// refreshes, its current sha advances while the parent's pin is retained —
    /// that gap between pin and current sha is the detectable staleness.
    #[tokio::test]
    async fn reconcile_then_persist_writes_cross_agent_edge_pinned_to_child_version() {
        use coral_node::agent_ref::AgentRef;
        use uuid::Uuid;

        let storage: Arc<dyn coral_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let db = Arc::new(MemoryStructuralDbStore::new());
        let graph_id = GraphId::new(Uuid::new_v4());
        let parent_agent_id = AgentId::new(Uuid::new_v4());
        let child_id = AgentId::new(Uuid::new_v4());

        // Child writes its output the way production does, so `set_file_version`
        // records its current blob sha.
        let child_prefix = format!("graphs/{graph_id}/agents/{child_id}");
        let child_ev = plant_evidence(
            storage.clone(),
            &child_prefix,
            "scan",
            json!({"q": "fab"}),
            json!({"util": 91}),
        )
        .await;
        let child_body = "child finding: utilization 91%";
        persist_output_impl(
            storage.clone(),
            db.clone(),
            child_id,
            &child_prefix,
            child_body,
            &[child_ev],
        )
        .await
        .expect("child persists output");

        // Parent folds the child's current output, then refreshes its own.
        let input = ReconcileChildrenInput {
            parent_graph_id: graph_id,
            parent_agent_id,
            sources: vec![ReconcileSource {
                child_ref: AgentRef::new(child_prefix.clone(), child_id),
                output_id: OutputId::new(child_body),
            }],
            conflict: None,
        };
        let r = reconcile_children_impl(storage.clone(), input, fixed_now())
            .await
            .expect("reconcile child");
        let parent_prefix = format!("graphs/{graph_id}/agents/{parent_agent_id}");
        persist_output_impl(
            storage.clone(),
            db.clone(),
            parent_agent_id,
            &parent_prefix,
            "consolidated finding",
            &[r.synthetic_evidence[0].clone()],
        )
        .await
        .expect("parent persists consolidated output");

        // Exactly one parent → child edge, citing the child's canonical output.
        let edge = db
            .citations()
            .into_iter()
            .find(|c| c.citing_agent == parent_agent_id && c.cited_agent == child_id)
            .expect("a cross-agent citation to the child");
        assert_eq!(edge.citing_path, CANONICAL_OUTPUT);
        assert_eq!(
            edge.cited_path, CANONICAL_OUTPUT,
            "cross-agent edge cites the child's canonical output, not parent self-evidence"
        );

        // Staleness-detection prerequisite: the pinned sha equals the child's
        // current file_index sha equals of_bytes(child_body) — one version,
        // three paths. Drift here would make staleness silently undetectable.
        let child_current = db
            .current_sha(child_id, CANONICAL_OUTPUT)
            .expect("child file_index set by its own write");
        assert_eq!(
            edge.cited_sha, child_current,
            "the parent's pin must equal the child's file_index sha"
        );
        assert_eq!(edge.cited_sha, BlobSha::of_bytes(child_body.as_bytes()));

        // Child refreshes → its current sha advances, but the parent's pin is
        // retained at the folded version. That difference between current and
        // pinned sha is the detectable staleness; resolving the pinned bytes
        // back is a separate concern (it needs a retained-history backend).
        let child_ev2 = plant_evidence(
            storage.clone(),
            &child_prefix,
            "scan",
            json!({"q": "fab"}),
            json!({"util": 95}),
        )
        .await;
        persist_output_impl(
            storage.clone(),
            db.clone(),
            child_id,
            &child_prefix,
            "child finding: utilization 95%",
            &[child_ev2],
        )
        .await
        .expect("child refreshes output");
        let child_after = db
            .current_sha(child_id, CANONICAL_OUTPUT)
            .expect("child file_index after refresh");
        assert_ne!(
            child_after, edge.cited_sha,
            "child's current sha must advance past the parent's pin"
        );
        assert!(
            db.citations()
                .iter()
                .any(|c| c.citing_agent == parent_agent_id
                    && c.cited_sha == BlobSha::of_bytes(child_body.as_bytes())),
            "the parent's earlier pin is retained, not auto-followed to latest"
        );
    }

    #[tokio::test]
    async fn reconcile_children_impl_returns_typed_error_for_missing_child_output() {
        use coral_node::agent_ref::AgentRef;
        use uuid::Uuid;

        let storage: Arc<dyn coral_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let graph_id = GraphId::new(Uuid::new_v4());
        let parent_agent_id = AgentId::new(Uuid::new_v4());
        let child_agent_id = AgentId::new(Uuid::new_v4());

        // No child output planted. Synthesize an `OutputId` for a body
        // we never persisted — the cross-agent read will miss.
        let bogus = OutputId::new("never-written");
        let child_workflow_id = format!("graphs/{graph_id}/agents/{child_agent_id}");
        let input = ReconcileChildrenInput {
            parent_graph_id: graph_id,
            parent_agent_id,
            sources: vec![ReconcileSource {
                child_ref: AgentRef::new(child_workflow_id, child_agent_id),
                output_id: bogus.clone(),
            }],
            conflict: None,
        };

        let err = reconcile_children_impl(storage.clone(), input, fixed_now())
            .await
            .expect_err("missing child output must surface typed error");
        // The helper returns `anyhow::Result`; downcast to the typed
        // `ReconciliationError` variant the activity wrapper uses for
        // the non-retryable classification.
        let typed = err
            .downcast_ref::<ReconciliationError>()
            .expect("typed ReconciliationError");
        match typed {
            ReconciliationError::ChildOutputNotFound {
                agent_id,
                output_id,
            } => {
                assert_eq!(*agent_id, child_agent_id);
                assert_eq!(*output_id, bogus);
            }
            other => panic!("expected ChildOutputNotFound, got {other:?}"),
        }

        // No synthetic evidence landed on the parent's FS — the
        // helper's two-phase pre-validation pass reads every cited
        // child output before any record_evidence write fires, so a
        // single bad source short-circuits the activity without
        // leaving a partial provenance trail.
        let parent_view = AgentFs::open_for_agent(storage, graph_id, parent_agent_id);
        let evs = parent_view
            .list_recent_evidence(8)
            .await
            .expect("list parent evidence after failure");
        assert!(
            evs.is_empty(),
            "pre-validation pass: no synthetic evidence should have landed on parent after typed failure"
        );
    }

    #[tokio::test]
    async fn reconcile_children_impl_atomicity_no_partial_writes_with_good_and_bad_sources() {
        use coral_node::agent_ref::AgentRef;
        use uuid::Uuid;

        let storage: Arc<dyn coral_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let graph_id = GraphId::new(Uuid::new_v4());
        let parent_agent_id = AgentId::new(Uuid::new_v4());
        let good_child_id = AgentId::new(Uuid::new_v4());
        let bad_child_id = AgentId::new(Uuid::new_v4());
        let (good_wf, good_out, _ev) =
            plant_child_output(storage.clone(), graph_id, good_child_id, "good claim").await;
        let bad_output_id = OutputId::from_hex("de".repeat(32));
        let bad_wf = format!("graphs/{graph_id}/agents/{bad_child_id}");

        let input = ReconcileChildrenInput {
            parent_graph_id: graph_id,
            parent_agent_id,
            sources: vec![
                ReconcileSource {
                    child_ref: AgentRef::new(good_wf, good_child_id),
                    output_id: good_out,
                },
                ReconcileSource {
                    child_ref: AgentRef::new(bad_wf, bad_child_id),
                    output_id: bad_output_id.clone(),
                },
            ],
            conflict: None,
        };

        let err = reconcile_children_impl(storage.clone(), input, fixed_now())
            .await
            .expect_err("a single bad source must fail the whole activity");
        let typed = err
            .downcast_ref::<ReconciliationError>()
            .expect("typed ReconciliationError");
        assert!(
            matches!(
                typed,
                ReconciliationError::ChildOutputNotFound { agent_id, .. } if *agent_id == bad_child_id
            ),
            "typed error must point at the bad child"
        );

        // Atomicity property: the good source's synthetic evidence
        // record did NOT land on the parent (phase-1 pre-validation
        // saw the bad source's miss before any phase-2 write fired).
        let parent_view = AgentFs::open_for_agent(storage, graph_id, parent_agent_id);
        let evs = parent_view
            .list_recent_evidence(8)
            .await
            .expect("list parent evidence after partial-failure attempt");
        assert!(
            evs.is_empty(),
            "atomicity: good_source must not have left a record after bad_source missed"
        );
    }

    #[tokio::test]
    async fn reconcile_children_impl_with_held_open_conflict_writes_record_and_returns_id() {
        // With `resolution: None` the activity must persist a
        // `HeldOpen` conflict record under the parent's
        // `conflicts/<id>.json` and return the id.
        use coral_node::agent_ref::AgentRef;
        use coral_node::conflict::ConflictKind;
        use coral_node::decision::{ConflictAlternative, ConflictRecordIntent};
        use uuid::Uuid;

        let storage: Arc<dyn coral_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let graph_id = GraphId::new(Uuid::new_v4());
        let parent_agent_id = AgentId::new(Uuid::new_v4());
        let child_agent_id = AgentId::new(Uuid::new_v4());
        let (child_wf, child_out, _ev) =
            plant_child_output(storage.clone(), graph_id, child_agent_id, "single claim").await;

        let alt_x = ConflictAlternative {
            source_child: AgentRef::new(child_wf.clone(), child_agent_id),
            source_output_id: child_out.clone(),
            claim: "value is X".into(),
        };
        let alt_y = ConflictAlternative {
            source_child: AgentRef::new(child_wf.clone(), child_agent_id),
            source_output_id: child_out.clone(),
            claim: "value is Y".into(),
        };
        let conflict = ConflictRecordIntent {
            alternatives: vec![alt_x.clone(), alt_y.clone()],
            resolution: None,
        };
        let input = ReconcileChildrenInput {
            parent_graph_id: graph_id,
            parent_agent_id,
            sources: vec![ReconcileSource {
                child_ref: AgentRef::new(child_wf, child_agent_id),
                output_id: child_out,
            }],
            conflict: Some(conflict),
        };
        let out = reconcile_children_impl(storage.clone(), input, fixed_now())
            .await
            .expect("reconcile with held-open conflict ok");
        assert_eq!(out.synthetic_evidence.len(), 1);
        let conflict_id = out
            .conflict_id
            .expect("conflict_id is Some when input.conflict is Some");

        // The record landed in the parent's FS and round-trips with
        // the right shape.
        let parent_view = AgentFs::open_for_agent(storage, graph_id, parent_agent_id);
        let listed = parent_view.list_conflicts().await.unwrap();
        assert_eq!(listed.len(), 1, "expected one conflict record");
        let record = &listed[0];
        assert_eq!(record.id, conflict_id);
        assert_eq!(record.kind, ConflictKind::HeldOpen);
        assert_eq!(record.alternatives, vec![alt_x, alt_y]);
        assert!(record.resolution.is_none());
    }

    #[tokio::test]
    async fn reconcile_children_impl_with_resolved_conflict_writes_resolution() {
        use coral_node::agent_ref::AgentRef;
        use coral_node::conflict::ConflictKind;
        use coral_node::decision::{ConflictAlternative, ConflictRecordIntent, ConflictResolution};
        use uuid::Uuid;

        let storage: Arc<dyn coral_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let graph_id = GraphId::new(Uuid::new_v4());
        let parent_agent_id = AgentId::new(Uuid::new_v4());
        let child_agent_id = AgentId::new(Uuid::new_v4());
        let (child_wf, child_out, _ev) =
            plant_child_output(storage.clone(), graph_id, child_agent_id, "claim").await;

        let resolution = ConflictResolution {
            chosen_alternative_idx: 1,
            reasoning: "second alternative cites more recent evidence".into(),
        };
        let conflict = ConflictRecordIntent {
            alternatives: vec![
                ConflictAlternative {
                    source_child: AgentRef::new(child_wf.clone(), child_agent_id),
                    source_output_id: child_out.clone(),
                    claim: "value is X".into(),
                },
                ConflictAlternative {
                    source_child: AgentRef::new(child_wf.clone(), child_agent_id),
                    source_output_id: child_out.clone(),
                    claim: "value is Y".into(),
                },
            ],
            resolution: Some(resolution.clone()),
        };
        let input = ReconcileChildrenInput {
            parent_graph_id: graph_id,
            parent_agent_id,
            sources: vec![ReconcileSource {
                child_ref: AgentRef::new(child_wf, child_agent_id),
                output_id: child_out,
            }],
            conflict: Some(conflict),
        };
        let out = reconcile_children_impl(storage.clone(), input, fixed_now())
            .await
            .expect("reconcile with resolved conflict ok");
        let conflict_id = out.conflict_id.expect("conflict_id is Some");

        let parent_view = AgentFs::open_for_agent(storage, graph_id, parent_agent_id);
        let record = parent_view
            .read_conflict(&conflict_id)
            .await
            .unwrap()
            .expect("conflict record present");
        assert_eq!(record.kind, ConflictKind::Resolved);
        assert_eq!(record.resolution.as_ref().unwrap(), &resolution);
    }

    #[tokio::test]
    async fn reconcile_children_impl_rejects_fewer_than_two_alternatives() {
        // A `ConflictRecordIntent` with one alternative is a
        // structural error; the activity returns the typed
        // ReconciliationError::ConflictAlternativesTooFew, which the
        // wrapper maps to non-retryable so the workflow body's
        // correction-context path takes over.
        use coral_node::agent_ref::AgentRef;
        use coral_node::decision::{ConflictAlternative, ConflictRecordIntent};
        use uuid::Uuid;

        let storage: Arc<dyn coral_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let graph_id = GraphId::new(Uuid::new_v4());
        let parent_agent_id = AgentId::new(Uuid::new_v4());
        let child_agent_id = AgentId::new(Uuid::new_v4());
        let (child_wf, child_out, _ev) =
            plant_child_output(storage.clone(), graph_id, child_agent_id, "claim").await;

        let conflict = ConflictRecordIntent {
            alternatives: vec![ConflictAlternative {
                source_child: AgentRef::new(child_wf.clone(), child_agent_id),
                source_output_id: child_out.clone(),
                claim: "the only side".into(),
            }],
            resolution: None,
        };
        let input = ReconcileChildrenInput {
            parent_graph_id: graph_id,
            parent_agent_id,
            sources: vec![ReconcileSource {
                child_ref: AgentRef::new(child_wf, child_agent_id),
                output_id: child_out,
            }],
            conflict: Some(conflict),
        };
        let err = reconcile_children_impl(storage.clone(), input, fixed_now())
            .await
            .expect_err("expected ConflictAlternativesTooFew");
        match err.downcast_ref::<ReconciliationError>() {
            Some(ReconciliationError::ConflictAlternativesTooFew { count }) => {
                assert_eq!(*count, 1)
            }
            other => panic!("expected ConflictAlternativesTooFew, got {other:?}"),
        }
        // No conflict file landed in the parent's FS.
        let parent_view = AgentFs::open_for_agent(storage, graph_id, parent_agent_id);
        let listed = parent_view.list_conflicts().await.unwrap();
        assert!(
            listed.is_empty(),
            "no conflict file should land for malformed intent; got {listed:?}"
        );
    }

    #[test]
    fn reconcile_children_input_round_trips_through_json_with_no_conflict() {
        use coral_node::agent_ref::AgentRef;
        use uuid::Uuid;
        let i = ReconcileChildrenInput {
            parent_graph_id: GraphId::new(Uuid::nil()),
            parent_agent_id: AgentId::new(Uuid::nil()),
            sources: vec![ReconcileSource {
                child_ref: AgentRef::new("graphs/g/agents/c", AgentId::new(Uuid::nil())),
                output_id: OutputId::from_hex("ab".repeat(32)),
            }],
            conflict: None,
        };
        let s = serde_json::to_string(&i).unwrap();
        let back: ReconcileChildrenInput = serde_json::from_str(&s).unwrap();
        assert_eq!(i, back);
        // `skip_serializing_if` keeps the wire lean when conflict is None.
        assert!(!s.contains("conflict"), "wire shape: {s}");
    }

    #[test]
    fn reconcile_children_output_round_trips_through_json() {
        let o = ReconcileChildrenOutput {
            synthetic_evidence: vec!["evidence/reconcile-aabbccdd.json".to_string()],
            conflict_id: None,
        };
        let s = serde_json::to_string(&o).unwrap();
        let back: ReconcileChildrenOutput = serde_json::from_str(&s).unwrap();
        assert_eq!(o, back);
        assert!(!s.contains("conflict_id"), "wire shape: {s}");
    }
}
