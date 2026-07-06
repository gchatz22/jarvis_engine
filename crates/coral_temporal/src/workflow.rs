//! `AgentWorkflow` — workflow type, input/output shapes, signal/update surface,
//! and the per-tick loop that orchestrates the activities in [`crate::activities`].
//!
//! SDK constraints: concurrency primitives must come from
//! `temporalio_sdk::workflows` (`select!`, `join_all`) — `tokio::*` wake-ups
//! fail the workflow task. Workflow state mutation is via
//! `ctx.state_mut(|s| ...)`, not a bare `&mut self` receiver. The workflow
//! body does no I/O and consults no clocks — wall-clock arrives via
//! `ctx.timer(...)` and FS reads/writes live in activities, so the loop is
//! fully replayable against workflow history.
//!
//! Cross-workflow signaling uses a two-step SDK chain
//! `ctx.external_workflow(workflow_id, None).signal(SignalDef, payload).await`
//! — there is no single `signal_external_workflow(..)` method in this SDK
//! version. Signal failures are logged and swallowed (best-effort): the
//! sender's data is durable on its own FS regardless of whether the
//! recipient observed the signal. [`ParentRef::signal`] is informational
//! at this version — the dispatch site uses the compile-time
//! [`AgentWorkflow::external_signal`] marker regardless of the field's
//! value.
//!
//! Synthetic evidence: the parent's `reconcile_children` activity reads each
//! cited child output cross-agent and writes one synthetic
//! [`EvidenceRecord`](coral_node::evidence::EvidenceRecord) (`tool =
//! "reconcile"`) into the parent's `evidence/`. This preserves
//! [`AgentFs::persist_output`](coral_node::fs::AgentFs::persist_output)'s
//! provenance check unchanged — cross-agent provenance becomes a normal
//! evidence trail.

use std::time::Duration;

use coral_node::agent_core::CYCLE_RUNAWAY_FUSE;
use coral_node::agent_ref::{AgentId, AgentRef, GraphId};
use coral_node::decision::{Decision, Observation, Seed, Session, ToolCall};
use coral_node::mandate::{Mandate, OutputId, INTERIM_STEP_CAP};
use coral_node::scheduler::arm_self_wake;
use coral_node::trigger::{HumanOp, MandatePatch, Trigger};
use serde::{Deserialize, Serialize};
use temporalio_common::protos::temporal::api::enums::v1::ParentClosePolicy;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ChildWorkflowOptions, ContinueAsNewOptions, SyncWorkflowContext,
    WorkflowContext, WorkflowResult,
};

use crate::activities::{
    AgentActivities, AppendDecisionLogInput, ApplyFsOpsInput, BuildSeedInput, CommitTickInput,
    DecideStepInput, ExecuteToolInput, FsNavOp, PersistOutputInput, PersistRetirementInput,
    ReadFsInput, ReconcileChildrenInput, RegisterChildInStructuralDbInput, RegisterChildOutcome,
    ToolCallFailure, ToolCallOutcome,
};
use coral_node::decision::{ConflictRecordIntent, ReconcileSource};

/// Resolved agent configuration handed to the workflow at start.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConfig {}

/// Storage handle scoping the agent to its `<graph_id>/<agent_id>` prefix.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsHandle {
    /// `<graph_id>/<agent_id>` — the prefix the storage trait scopes to.
    pub prefix: String,
}

impl FsHandle {
    /// Construct an [`FsHandle`] for a `(graph_id, agent_id)` pair using the
    /// canonical workflow-id prefix layout (`graphs/<graph_id>/agents/<agent_id>`).
    pub fn for_agent(graph_id: GraphId, agent_id: AgentId) -> Self {
        Self {
            prefix: agent_workflow_id(&graph_id.to_string(), &agent_id.to_string()),
        }
    }
}

/// Parent workflow reference for cross-workflow signal routing.
///
/// Populated by [`build_child_input`] so a child workflow can call
/// `WorkflowContext::external_workflow(parent_handle.workflow_id, None).signal(parent_handle.signal, ..)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentRef {
    /// Temporal workflow id of the parent — load-bearing for
    /// `external_workflow(workflow_id, None)` lookups. Flat
    /// `graphs/<gid>/agents/<aid>` form so reparenting doesn't rewrite ids.
    pub workflow_id: String,
    /// Signal name on the parent the child fires `Trigger`s through.
    /// Defaults to [`Self::DEFAULT_SIGNAL`].
    pub signal: String,
}

impl ParentRef {
    /// Default signal name routed to [`AgentWorkflow::external_signal`].
    pub const DEFAULT_SIGNAL: &'static str = "external_signal";
}

impl Default for ParentRef {
    /// Empty `workflow_id` is *not* a valid signal target — callers
    /// constructing a `ParentRef` for live use must populate `workflow_id`.
    /// The `Default` exists for serde compat and the test surface that
    /// constructs `AgentInput` with `parent_handle: None`.
    fn default() -> Self {
        Self {
            workflow_id: String::new(),
            signal: Self::DEFAULT_SIGNAL.to_string(),
        }
    }
}

/// Scheduler-state subset of the [`Carryover`].
///
/// Wraps `next_wake` in a struct (rather than a bare `Option<Duration>` on
/// `Carryover`) so future per-mandate cursor state can slot in without
/// renaming a field on the wire.
///
/// Deliberately no `last_tick_at` timestamp: a wall-clock timestamp on the
/// carryover would only be observed at encode time on the post-CAN run's
/// replay and adds zero scheduling value over `next_wake` alone — the new
/// run pins its own first-tick wake the same way the very first run does,
/// defaulting to [`INITIAL_NEXT_WAKE`] when `next_wake` is `None`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerCursor {
    /// `next_wake` cadence the previous run had pinned. `None` means the
    /// previous run never saw a `Decision::Idle` (so the new run defaults
    /// to the [`INITIAL_NEXT_WAKE`] floor on its first tick).
    pub next_wake: Option<Duration>,
}

/// Typed continue-as-new carryover.
///
/// A small, typed, deterministically-rebuildable subset of in-workflow state
/// that would otherwise be lost when `ctx.continue_as_new(...)` terminates
/// the current run. Not conversation history or tool results — those survive
/// via the per-agent FS, which is external to Temporal history.
///
/// Most fields map to a workflow-state field that the run loop observes or
/// mutates. The mapping is:
///
/// | Carryover field | Workflow-state field | Lifecycle |
/// |---|---|---|
/// | `pending_triggers` | [`AgentWorkflow::pending_triggers`] | Drained at top of each tick |
/// | `pending_human_ops` | [`AgentWorkflow::pending_human_ops`] | Drained at top of each tick |
/// | `pending_mandate_patches` | [`AgentWorkflow::pending_mandate_patches`] | Drained at top of each tick |
/// | `retirement_request` | [`AgentWorkflow::retirement_request`] | Drained at top of each tick (short-circuits) |
/// | `scheduler_cursor` | [`AgentWorkflow::next_wake`] | Honored by the wake gate |
/// | `last_output_id` | [`AgentWorkflow::last_output_id`] | Latest persisted `WriteOutput` id |
/// | `cumulative_*_observed` | matching `AgentWorkflow::cumulative_*_observed` | Observability across CAN boundary |
/// | `child_handles` | [`AgentWorkflow::child_handles`] | Spawned-child handles across CAN |
///
/// `cumulative_*_observed` must survive CAN — without them, a snapshot taken
/// on the post-CAN run would report `cumulative_triggers_observed == 0` even
/// though the workflow lifetime had observed N signals on the pre-CAN run.
///
/// `in_flight` is the exception to "maps to workflow state": it is the
/// in-flight [`Session`] of a cycle a *mid-cycle* continue-as-new suspended.
/// The run loop reads it once at entry to resume that exact cycle (rehydrate
/// the session, continue the inner ReAct loop) rather than building a fresh
/// seed; it never lives on the workflow struct, so `encode_carryover` always
/// emits `None` and only the mid-cycle CAN site sets it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Carryover {
    pub pending_triggers: Vec<Trigger>,
    pub pending_human_ops: Vec<HumanOp>,
    pub pending_mandate_patches: Vec<MandatePatch>,
    pub retirement_request: Option<String>,
    pub scheduler_cursor: SchedulerCursor,
    /// Handles to spawned child agents the parent retains across
    /// continue-as-new. Each entry is an [`AgentRef`] populated by the
    /// `Decision::SpawnChild` arm of [`AgentWorkflow::run`].
    pub child_handles: Vec<AgentRef>,
    pub last_output_id: Option<OutputId>,
    /// Cumulative count of `Trigger`s observed via `external_signal` across
    /// the workflow's lifetime (including all prior CAN runs). Without
    /// this, a post-CAN snapshot would only reflect signals received on the
    /// current run, not the lifetime view.
    pub cumulative_triggers_observed: u64,
    pub cumulative_human_ops_observed: u64,
    pub cumulative_mandate_patches_observed: u64,
    /// Monotonically increasing tick counter the workflow body stamps onto
    /// every `<prefix>/decisions/<tick>.jsonl` artifact. Survives CAN so the
    /// post-CAN run continues numbering rather than clobbering pre-CAN
    /// files at `decisions/0.jsonl`.
    pub tick: u64,
    /// In-flight [`Session`] of a cycle a *mid-cycle* continue-as-new
    /// suspended. `Some` ⇒ the post-CAN run resumes this exact cycle
    /// (rehydrate the session, continue the inner ReAct loop) instead of
    /// building a fresh seed; `None` ⇒ a normal between-cycles start. Set
    /// only at the mid-cycle CAN site in [`AgentWorkflow::run`];
    /// [`AgentWorkflow::encode_carryover`] always emits `None` because the
    /// session lives in a run-loop local, not on the workflow struct.
    #[serde(default)]
    pub in_flight: Option<Session>,
}

/// Input handed to `AgentWorkflow::run` at start (and at every continue-as-new).
///
/// `carryover` is load-bearing: on hydrate the workflow body decodes it via
/// [`AgentWorkflow::hydrate_from_carryover`] back onto workflow state so
/// pending signal queues, retirement requests, `next_wake`, and the
/// cumulative observability counters all survive a CAN boundary. `None`
/// means "first run of this workflow" — the workflow starts from `Default`
/// state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentInput {
    pub cfg: AgentConfig,
    pub fs_handle: FsHandle,
    pub parent_handle: Option<ParentRef>,
    pub carryover: Option<Carryover>,
    /// Resolved [`Mandate`] for this agent. The workflow body passes it into
    /// every `build_seed` activity invocation so the LLM sees the real
    /// mandate text + cadence.
    pub mandate: Mandate,
    /// Graph this agent belongs to. Carried on `AgentInput` (rather than
    /// parsed from `ctx.workflow_id()` at activity-time) so the workflow
    /// body isn't tied to the id-scheme string format.
    pub graph_id: GraphId,
    /// This agent's structural-DB id. The `Decision::SpawnChild` arm needs
    /// the parent's `AgentId` to write the parent → child edge.
    pub agent_id: AgentId,
    /// Operator-authored agent name (the `agents[].id` from the YAML),
    /// distinct from the structural `agent_id` UUID. Used by the child →
    /// parent signal renderer for the `ChildOutput { child_name }` field.
    pub agent_name: String,
}

impl AgentInput {
    /// Test-only constructor with first-run defaults for every non-identity
    /// field (`cfg: Default`, `fs_handle: Default`, `parent_handle: None`,
    /// `carryover: None`, `mandate: Mandate::new("", ZERO, None)`), requiring
    /// the caller to supply the identity triple explicitly.
    ///
    /// Production constructors ([`build_child_input`] /
    /// [`build_root_input`]) carry a real mandate + fs_handle.
    pub fn new_for_test(
        graph_id: GraphId,
        agent_id: AgentId,
        agent_name: impl Into<String>,
    ) -> Self {
        Self {
            cfg: AgentConfig::default(),
            fs_handle: FsHandle::default(),
            parent_handle: None,
            carryover: None,
            mandate: Mandate::new("", Duration::ZERO, None),
            graph_id,
            agent_id,
            agent_name: agent_name.into(),
        }
    }
}

/// Result returned by `AgentWorkflow::run` when the workflow exits cleanly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum AgentResult {
    /// The workflow's loop body completed because the agent retired (the
    /// `retire` signal fired or the interim `step_cap` runaway backstop was
    /// hit). The agent never self-terminates — termination is a kernel/human
    /// op.
    Retired { reason: String },
}

impl Default for AgentResult {
    fn default() -> Self {
        Self::Retired {
            reason: String::new(),
        }
    }
}

/// Snapshot of the workflow's signal-bucket counts + retirement flag,
/// returned by the [`AgentWorkflow::inspect_state`] update.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentSnapshot {
    /// Count of `Trigger`s currently queued in `pending_triggers`. The loop
    /// drains this at the top of every tick, so `0` here doesn't mean "the
    /// signal didn't land" — see `cumulative_triggers_observed` for the
    /// persistent "did-it-arrive?" view.
    pub pending_triggers_count: usize,
    pub pending_human_ops_count: usize,
    pub pending_mandate_patches_count: usize,
    pub retirement_request: Option<String>,
    pub recent_output_ids: Vec<String>,
    /// Cumulative count of `Trigger`s observed via `external_signal` since
    /// the workflow started (or its last continue-as-new). Bumped in the
    /// signal handler at receipt time so an inspect taken between signal
    /// arrival and the next loop drain still reflects the arrival.
    #[serde(default)]
    pub cumulative_triggers_observed: u64,
    #[serde(default)]
    pub cumulative_human_ops_observed: u64,
    #[serde(default)]
    pub cumulative_mandate_patches_observed: u64,
}

impl AgentSnapshot {
    fn from_state(workflow: &AgentWorkflow) -> Self {
        Self {
            pending_triggers_count: workflow.pending_triggers.len(),
            pending_human_ops_count: workflow.pending_human_ops.len(),
            pending_mandate_patches_count: workflow.pending_mandate_patches.len(),
            retirement_request: workflow.retirement_request.clone(),
            recent_output_ids: Vec::new(),
            cumulative_triggers_observed: workflow.cumulative_triggers_observed,
            cumulative_human_ops_observed: workflow.cumulative_human_ops_observed,
            cumulative_mandate_patches_observed: workflow.cumulative_mandate_patches_observed,
        }
    }
}

/// Build the workflow ID for an agent: `graphs/<graph_id>/agents/<agent_id>`.
pub fn agent_workflow_id(graph_id: &str, agent_id: &str) -> String {
    format!("graphs/{graph_id}/agents/{agent_id}")
}

/// `next_wake` value when the workflow state hasn't been told a specific
/// idle period yet (first tick of a run, or first tick after CAN).
/// Deliberately tiny — the first iteration's wake gate must fire
/// immediately so the workflow doesn't sit idle waiting for nothing.
const INITIAL_NEXT_WAKE: Duration = Duration::from_millis(1);

/// Hard ceiling for the serialized [`AgentInput`] a *mid-cycle* continue-as-new
/// would send when carrying an in-flight session. Temporal rejects any single
/// payload above `limit.blobSize.error` (default 2 MiB) and a CAN input is one
/// payload, so a too-large carry would fail the CAN command and wedge the
/// workflow on retry. We measure the *whole* candidate input (carryover +
/// session + mandate + …) against this cap, set below the server default with
/// margin so the carry can never trip the limit; a session that would exceed it
/// force-idles the cycle and rebuilds from the durable FS instead.
const CAN_PAYLOAD_HARD_BYTES: usize = 1_572_864; // 1.5 MiB

/// Soft warning threshold (kept under Temporal's `limit.blobSize.warn`, default
/// 512 KiB). Crossing it logs but still carries the session.
const CAN_PAYLOAD_WARN_BYTES: usize = 262_144; // 256 KiB

/// Per-activity start-to-close timeout. Generous so a stub activity and a
/// real activity (LLM calls, FS writes) both fit; the workflow loop's own
/// deadlines come from `next_wake` and the retirement signal.
const ACTIVITY_TIMEOUT: Duration = Duration::from_secs(30);

/// The agent workflow.
///
/// `#[derive(Default)]` is required by the SDK's `#[workflow]` macro.
#[workflow]
#[derive(Default)]
pub struct AgentWorkflow {
    /// `external_signal` queue. Pushed by the signal handler; drained at
    /// the top of every loop iteration.
    pending_triggers: Vec<Trigger>,
    /// `human_override` queue. Drained alongside `pending_triggers` and
    /// passed to `build_seed` as a separate field.
    pending_human_ops: Vec<HumanOp>,
    /// `mandate_update` queue. Drained alongside `pending_triggers` and
    /// passed to `build_seed` as a separate field.
    pending_mandate_patches: Vec<MandatePatch>,
    /// `retire` request. Checked at the top of every loop iteration; a set
    /// value short-circuits the tick to the retirement path.
    retirement_request: Option<String>,
    /// Wall-clock the next idle `ctx.timer(...)` waits for. Updated by
    /// `Decision::Idle { next_after }`. `None` on the very first tick of a
    /// run (the loop starts with [`INITIAL_NEXT_WAKE`] = 1ms so the first
    /// tick fires immediately). A continue-as-new preserves the prior run's
    /// `next_wake` via [`Carryover::scheduler_cursor`].
    next_wake: Option<Duration>,
    /// Cumulative count of `Trigger`s observed via `external_signal` since
    /// the workflow started (or last continue-as-new). Bumped inside the
    /// signal handler so a snapshot taken between signal arrival and the
    /// next loop drain still reflects the arrival.
    cumulative_triggers_observed: u64,
    cumulative_human_ops_observed: u64,
    cumulative_mandate_patches_observed: u64,
    /// Last `persist_output` `OutputId` observed by this run. The
    /// `persist_output` activity does not echo the id back into workflow
    /// state today (the field stays `None`); the slot exists so the
    /// [`Carryover`] round-trip is structurally complete.
    last_output_id: Option<OutputId>,
    /// Per-tick counter bumped at the bottom of each loop iteration.
    /// Stamped onto each `<prefix>/decisions/<tick>.jsonl` artifact via the
    /// `append_decision_log` activity. Hydrated from [`Carryover::tick`] on
    /// post-CAN runs so the artifact stream stays monotonic across the
    /// boundary.
    tick: u64,
    /// Handles to child agents this workflow has spawned via
    /// `Decision::SpawnChild`. Pushed by the spawn arm after
    /// `register_child_in_structural_db` returns the child's id and
    /// `ctx.child_workflow(..)` has dispatched the child run. Round-trips
    /// across continue-as-new via [`Carryover::child_handles`].
    child_handles: Vec<AgentRef>,
}

#[workflow_methods]
impl AgentWorkflow {
    /// `external_signal` — push a typed [`Trigger`] onto the per-tick queue.
    /// The loop drains the queue at the top of each iteration.
    ///
    /// Side-bookkeeps `cumulative_triggers_observed` at receipt time (not
    /// drain time) so the snapshot's cumulative view reflects every signal
    /// regardless of inspect timing relative to the loop.
    #[signal]
    pub fn external_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, trigger: Trigger) {
        self.pending_triggers.push(trigger);
        self.cumulative_triggers_observed = self.cumulative_triggers_observed.saturating_add(1);
    }

    /// `human_override` — push a typed [`HumanOp`] onto the override queue.
    #[signal]
    pub fn human_override(&mut self, _ctx: &mut SyncWorkflowContext<Self>, op: HumanOp) {
        self.pending_human_ops.push(op);
        self.cumulative_human_ops_observed = self.cumulative_human_ops_observed.saturating_add(1);
    }

    /// `mandate_update` — push a typed [`MandatePatch`] onto the patch queue.
    #[signal]
    pub fn mandate_update(&mut self, _ctx: &mut SyncWorkflowContext<Self>, patch: MandatePatch) {
        self.pending_mandate_patches.push(patch);
        self.cumulative_mandate_patches_observed =
            self.cumulative_mandate_patches_observed.saturating_add(1);
    }

    /// `retire` — record a retirement reason. The loop body observes
    /// `retirement_request.is_some()` at the top of every iteration and
    /// short-circuits to `persist_retirement` + return.
    #[signal]
    pub fn retire(&mut self, _ctx: &mut SyncWorkflowContext<Self>, reason: String) {
        self.retirement_request = Some(reason);
    }

    /// `inspect_state` — return a typed [`AgentSnapshot`] of the workflow's
    /// signal-bucket counts + retirement flag.
    #[update]
    pub fn inspect_state(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        _input: (),
    ) -> AgentSnapshot {
        AgentSnapshot::from_state(self)
    }

    /// Workflow entry point — the per-tick loop body.
    ///
    /// Reads top-to-bottom: hydrate carryover (if any) → loop {wake → drain
    /// → assemble → decide → dispatch → (maybe) continue-as-new}. Every
    /// external action (FS read/write, LLM call, tool dispatch) lives in an
    /// activity; the workflow body is pure orchestration.
    ///
    /// Continue-as-new fires at two points, both gated on
    /// [`temporalio_sdk::WorkflowContext::continue_as_new_suggested`]:
    ///
    /// 1. **Cycle boundary** (after a cycle's inner loop ends, only on
    ///    non-retirement ticks): the workflow state is encoded into a fresh
    ///    [`Carryover`] via [`encode_carryover`] (`in_flight: None`) and
    ///    passed to `ctx.continue_as_new(&next_input, opts)`. The next run
    ///    starts a fresh cycle.
    /// 2. **Mid-cycle** (between two inner steps, *after* a step's observation
    ///    has been pushed): the in-flight [`Session`] is attached to the
    ///    carryover (`in_flight: Some(session)`) so the next run resumes the
    ///    *same* cycle rather than rebuilding a seed. This is what lets one
    ///    unit of mandate work span many runs / outgrow a single history.
    ///
    /// On entry, if `input.carryover.in_flight.is_some()` the run resumes the
    /// suspended cycle (skip the wake gate / drain / `build_seed`); otherwise
    /// it hydrates the carryover onto workflow state via
    /// [`hydrate_from_carryover`] and runs the normal wake→drain→seed path.
    ///
    /// Retirement structurally cannot trigger CAN: every retirement path (the
    /// interim `step_cap` backstop, the `drained.retirement` short-circuit,
    /// and the resume-path retirement peek) returns before any CAN check.
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: AgentInput,
    ) -> WorkflowResult<AgentResult> {
        // A mid-cycle continue-as-new attaches the in-flight session here; if
        // present, this run resumes that suspended cycle. Read it out before
        // hydrating the rest of the carryover (which deliberately ignores
        // `in_flight`).
        let mut resume = input.carryover.as_ref().and_then(|c| c.in_flight.clone());
        if let Some(c) = input.carryover.clone() {
            ctx.state_mut(|s| s.hydrate_from_carryover(c));
        }
        // `never` cadence (`idle_period == None`): self-wake only the first
        // cycle, then wait on signals alone (the wake gate stops arming the
        // idle timer).
        let never = input.mandate.is_never();
        loop {
            // The interim `step_cap` runaway backstop. Checked before
            // `wait_for_tick` so an over-budget agent retires without waking
            // or burning a decide call, mirroring the in-process loop. `tick`
            // is the hydrated state value, so the cap spans a
            // continue-as-new. On a mid-cycle resume `tick` is unchanged from
            // when the cycle started (it bumps only at a true cycle end), so
            // this check passes through harmlessly.
            let tick = ctx.state(|s| s.tick);
            if let Some(reason) = step_cap_retire_reason(tick, input.mandate.step_cap) {
                return retire(ctx, &input, reason).await;
            }

            // Resume a mid-cycle-CAN-suspended session, or start a fresh
            // cycle. The `session` is a LOCAL value rebuilt only from
            // journaled activity results (or, on resume, from the immutable
            // workflow input) — never from a live FS read in the workflow
            // body — so replay stays deterministic.
            let mut session = if let Some(session) = resume.take() {
                // Resuming: we are mid-cycle, not at a wake boundary, so skip
                // the wake gate / drain / `build_seed`. Still honor a `retire`
                // request — the one signal that preempts a long cycle at a
                // rollover. Other pending signals (triggers / human ops /
                // mandate patches) stay queued for the next fresh cycle's
                // seed, matching the cycle-granularity drain model.
                if let Some(reason) = ctx.state_mut(|s| s.retirement_request.take()) {
                    return retire(ctx, &input, reason).await;
                }
                session
            } else {
                wait_for_tick(ctx, never).await;

                // Retirement short-circuit fires before any activity
                // invocation and before any CAN check, so a `retire` signal
                // can never trigger a continue-as-new.
                let mut drained = ctx.state_mut(drain_buckets);
                if let Some(reason) = drained.retirement {
                    return retire(ctx, &input, reason).await;
                }
                synthesize_scheduled_wake(&mut drained);

                // Build the thin orienting seed (activity) and start a fresh
                // session for the inner ReAct loop: decide_step → execute →
                // observe, until the model chooses `Idle` (the sole terminal).
                let seed = build_seed(ctx, &input, drained).await?;
                Session::new(seed)
            };
            // `step` is the decision-log index. It equals `session.len()` at
            // the top of every inner iteration, so deriving it here keeps the
            // `decisions/<tick>-<step>.jsonl` stream monotonic across a
            // mid-cycle CAN (fresh → 0; resume → where the cycle left off)
            // with no clobber and no extra carried counter.
            let mut step = session.len() as u64;
            loop {
                let action = decide_step(ctx, &session).await?;
                // Append `<prefix>/decisions/<tick>-<step>.jsonl` BEFORE the
                // action's activity runs so the artifact lands even if a
                // downstream activity errors out. The activity sources its
                // timestamp from `ctx.info().scheduled_time` so Temporal
                // retries write byte-identical bytes.
                log_decision(ctx, &input.fs_handle, tick, step, &action).await?;
                step = step.saturating_add(1);

                if let Some(next_after) = action.idle_after() {
                    // `Idle` is the sole terminal: pin the next cadence and
                    // end the cycle.
                    //
                    // Status-note telemetry, guarded by `is_replaying` so a
                    // workflow-task replay (cache eviction / worker restart)
                    // does not double-count this per-cycle metric.
                    if !ctx.is_replaying() {
                        tracing::info!(
                            steps = session.len(),
                            status_note_written =
                                coral_node::agent_core::status_note_written(&session),
                            "cycle complete: status-note telemetry"
                        );
                    }
                    ctx.state_mut(|s| s.next_wake = Some(next_after));
                    break;
                }

                let mut observation = execute_action(ctx, &input, &action).await?;
                if let Some(nudge) = coral_node::agent_core::wander_nudge(&session, &action) {
                    observation.content.push_str(&nudge);
                }
                session.push(action, observation);

                if session.len() >= CYCLE_RUNAWAY_FUSE {
                    tracing::error!(
                        steps = session.len(),
                        "cycle hit runaway fuse; forcing idle — this mandate never converges, decompose it"
                    );
                    break;
                }

                // Mid-cycle continue-as-new. This check MUST stay strictly
                // after `session.push`: that ordering is what keeps `step ==
                // session.len()` on resume, so the post-CAN run re-enters the
                // inner loop at exactly the next decision index with the
                // just-executed step's observation already in the carried
                // session — nothing re-executes and the decision log never
                // clobbers. Moving it earlier would resume on a
                // logged-but-not-executed decision.
                if ctx.continue_as_new_suggested() {
                    let candidate = mid_cycle_input(
                        &input,
                        ctx.state(|s| s.encode_carryover()),
                        session.clone(),
                    );
                    let payload_bytes = serde_json::to_vec(&candidate)
                        .map(|v| v.len())
                        .unwrap_or(usize::MAX);
                    match carry_decision(payload_bytes) {
                        CarryDecision::ForceIdle => {
                            tracing::error!(
                                payload_bytes,
                                hard = CAN_PAYLOAD_HARD_BYTES,
                                "in-flight session too large to carry across continue-as-new; \
                                 force-idling this cycle — it will rebuild from the durable FS on \
                                 the next wake"
                            );
                            // Wake promptly so the dropped in-cycle work
                            // resumes from the FS regardless of cadence (a
                            // `never` agent must not sleep forever here).
                            ctx.state_mut(|s| s.next_wake = Some(INITIAL_NEXT_WAKE));
                            break;
                        }
                        CarryDecision::Carry { warn } => {
                            if warn {
                                tracing::warn!(
                                    payload_bytes,
                                    warn = CAN_PAYLOAD_WARN_BYTES,
                                    "in-flight session carry size approaching the \
                                     continue-as-new payload limit"
                                );
                            }
                            ctx.continue_as_new(&candidate, ContinueAsNewOptions::default())?;
                            unreachable!("continue_as_new should have terminated this run");
                        }
                    }
                }
            }
            // Commit the agent's FS as this completed cycle ("commit =
            // cycle"). Sits after every inner-loop `break` (Idle / runaway /
            // force-idle) but before the tick bump, so it commits under the
            // tick the cycle ran as. The mid-cycle continue-as-new returns
            // before reaching here, so a cycle that spanned a CAN still gets
            // exactly one commit — at its true completion on the resumed run.
            // The retire path bypasses this entirely (it returns above): any
            // prior cycle's work was already committed at its own boundary.
            commit_tick(ctx, &input.fs_handle, tick).await?;

            // Bump the tick (cycle counter) after the cycle completes so the
            // next cycle's decisions land under `decisions/<tick+1>-*.jsonl`.
            // The retire path above intentionally bypasses this — the
            // retirement log is the final entry for the workflow.
            ctx.state_mut(|s| s.tick = s.tick.saturating_add(1));

            // `continue_as_new_suggested` is server-driven, surfaced on
            // each `WorkflowActivation`. `ContinueAsNewOptions` exposes no
            // client-side knob to lower the suggested-CAN threshold; the
            // dev-server threshold is undocumented and substantially larger
            // than the 4096 figure some SDK docs cite (empirically, 175
            // idle ticks producing 3001 history events did not flip the
            // suggestion). Forcing a natural CAN under a unit-test
            // wall-clock budget is therefore not feasible — the hermetic
            // tests below cover the encode + JSON + hydrate wire path.
            if ctx.continue_as_new_suggested() {
                let carryover = ctx.state(|s| s.encode_carryover());
                let next = AgentInput {
                    carryover: Some(carryover),
                    ..input
                };
                ctx.continue_as_new(&next, ContinueAsNewOptions::default())?;
                unreachable!("continue_as_new should have terminated this run");
            }
        }
    }
}

impl AgentWorkflow {
    /// Encode the workflow's per-tick state into a [`Carryover`] for
    /// transmission across a `continue_as_new` boundary.
    ///
    /// `&self` (not `&mut self`) — the encode is observation-only; the live
    /// workflow run will terminate immediately after `ctx.continue_as_new(...)`
    /// returns, so there is no value in clearing local state.
    pub(crate) fn encode_carryover(&self) -> Carryover {
        Carryover {
            pending_triggers: self.pending_triggers.clone(),
            pending_human_ops: self.pending_human_ops.clone(),
            pending_mandate_patches: self.pending_mandate_patches.clone(),
            retirement_request: self.retirement_request.clone(),
            scheduler_cursor: SchedulerCursor {
                next_wake: self.next_wake,
            },
            child_handles: self.child_handles.clone(),
            last_output_id: self.last_output_id.clone(),
            // `in_flight` is never workflow state — only the mid-cycle CAN
            // site attaches a session. A boundary CAN carries no in-flight
            // cycle (the cycle just ended), so emit `None` here.
            in_flight: None,
            cumulative_triggers_observed: self.cumulative_triggers_observed,
            cumulative_human_ops_observed: self.cumulative_human_ops_observed,
            cumulative_mandate_patches_observed: self.cumulative_mandate_patches_observed,
            tick: self.tick,
        }
    }

    /// Decode a [`Carryover`] back onto the workflow's mutable state.
    ///
    /// Symmetric inverse of [`Self::encode_carryover`]. Called exactly once
    /// at the top of [`Self::run`] when `input.carryover.is_some()`.
    pub(crate) fn hydrate_from_carryover(&mut self, c: Carryover) {
        self.pending_triggers = c.pending_triggers;
        self.pending_human_ops = c.pending_human_ops;
        self.pending_mandate_patches = c.pending_mandate_patches;
        self.retirement_request = c.retirement_request;
        self.next_wake = c.scheduler_cursor.next_wake;
        self.last_output_id = c.last_output_id;
        // `c.in_flight` is intentionally not hydrated onto workflow state: the
        // run loop reads it directly from `input.carryover` to decide whether
        // to resume a suspended cycle (see [`AgentWorkflow::run`]).
        self.cumulative_triggers_observed = c.cumulative_triggers_observed;
        self.cumulative_human_ops_observed = c.cumulative_human_ops_observed;
        self.cumulative_mandate_patches_observed = c.cumulative_mandate_patches_observed;
        self.tick = c.tick;
        self.child_handles = c.child_handles;
    }
}

/// Retire reason when `tick` has reached the interim `step_cap` runaway
/// backstop (`None` falls back to [`INTERIM_STEP_CAP`]), else `None`. The
/// wording matches the in-process loop in `coral_node::agent` so
/// `retirement.json` reads identically on both paths.
fn step_cap_retire_reason(tick: u64, step_cap: Option<u64>) -> Option<String> {
    let cap = step_cap.unwrap_or(INTERIM_STEP_CAP);
    (tick >= cap).then(|| format!("step_cap ({cap}) reached"))
}

/// Outcome of sizing a candidate mid-cycle continue-as-new payload.
#[derive(Debug, PartialEq, Eq)]
enum CarryDecision {
    /// Carry the in-flight session across the CAN. `warn` flags a payload
    /// over the soft threshold (carried anyway, logged).
    Carry { warn: bool },
    /// The payload would risk Temporal's `blobSize.error` limit — force-idle
    /// the cycle and rebuild from the FS instead of carrying.
    ForceIdle,
}

/// Map a serialized candidate-input size to a [`CarryDecision`]. Pure so the
/// threshold logic is unit-testable at the boundary values without a live
/// continue-as-new (which can't be forced hermetically). Both thresholds are
/// strict `>` — a payload exactly at the cap still carries.
fn carry_decision(payload_bytes: usize) -> CarryDecision {
    if payload_bytes > CAN_PAYLOAD_HARD_BYTES {
        CarryDecision::ForceIdle
    } else {
        CarryDecision::Carry {
            warn: payload_bytes > CAN_PAYLOAD_WARN_BYTES,
        }
    }
}

/// Build the candidate [`AgentInput`] a mid-cycle continue-as-new would send:
/// the base carryover with the in-flight `session` attached. Cloning `input`
/// (rather than the boundary CAN's `..input` move) is deliberate — the
/// mid-cycle site may decide *not* to carry (the size-guard force-idle path),
/// in which case the caller keeps using the original `input`. The clone is
/// paid at most once per run (only when `continue_as_new_suggested` fires).
fn mid_cycle_input(input: &AgentInput, mut base: Carryover, session: Session) -> AgentInput {
    base.in_flight = Some(session);
    AgentInput {
        carryover: Some(base),
        ..input.clone()
    }
}

/// Give a pure idle-timer wake an explicit "you woke on schedule" signal.
///
/// When the drained tick carried no triggers, human ops, mandate patches,
/// pending correction, or retirement request, the agent woke because its
/// `idle_period` elapsed with nothing queued. Synthesize a `ScheduledWake`
/// so the model has a "why" to act on instead of an empty bundle — mirrors
/// the in-process loop, which pushes `ScheduledWake` only when the deadline
/// fires with an empty queue. No-op when any real work was drained.
fn synthesize_scheduled_wake(drained: &mut DrainedBuckets) {
    if drained.triggers.is_empty()
        && drained.human_ops.is_empty()
        && drained.mandate_patches.is_empty()
        && drained.retirement.is_none()
    {
        drained.triggers.push(Trigger::ScheduledWake);
    }
}

/// Wake gate for the loop body. Returns once any signal bucket is non-empty
/// (triggers, human ops, mandate patches, or retirement), or the per-tick
/// `next_wake` timer elapses. `workflows::select!` is the SDK's
/// deterministic race primitive — `tokio::select!` would break replay.
///
/// We wake on every non-retire signal bucket (not only `triggers_pending`)
/// so operator-sent overrides round-trip through the loop within one tick
/// rather than waiting up to `next_wake` for the next idle wake.
async fn wait_for_tick(ctx: &WorkflowContext<AgentWorkflow>, never: bool) {
    let (wake_after, is_first_wake) =
        ctx.state(|s| (s.next_wake.unwrap_or(INITIAL_NEXT_WAKE), s.tick == 0));
    let mut wait_signal = ctx.wait_condition(|s| {
        !s.pending_triggers.is_empty()
            || !s.pending_human_ops.is_empty()
            || !s.pending_mandate_patches.is_empty()
            || s.retirement_request.is_some()
    });
    if arm_self_wake(never, is_first_wake) {
        let mut wait_timer = ctx.timer(wake_after);
        temporalio_sdk::workflows::select! {
            _ = wait_signal => {},
            _ = wait_timer => {},
        };
    } else {
        // `never` cadence past the first cycle: no self-wake timer, so block
        // until a signal arrives.
        wait_signal.await;
    }
}

/// Invoke the `build_seed` activity with the per-cycle drained buckets,
/// returning the thin orienting [`Seed`] that starts the cycle's session.
async fn build_seed(
    ctx: &WorkflowContext<AgentWorkflow>,
    input: &AgentInput,
    drained: DrainedBuckets,
) -> WorkflowResult<Seed> {
    let out = ctx
        .start_activity(
            AgentActivities::build_seed,
            BuildSeedInput {
                mandate: input.mandate.clone(),
                fs_handle: input.fs_handle.clone(),
                graph_id: input.graph_id,
                triggers: drained.triggers,
                human_ops: drained.human_ops,
                mandate_patches: drained.mandate_patches,
            },
            activity_opts(),
        )
        .await?;
    Ok(out.seed)
}

/// Invoke the `decide_step` activity for the next inner-loop step, passing
/// the accumulating session so the model reasons over its in-cycle history.
async fn decide_step(
    ctx: &WorkflowContext<AgentWorkflow>,
    session: &Session,
) -> WorkflowResult<Decision> {
    Ok(ctx
        .start_activity(
            AgentActivities::decide_step,
            DecideStepInput {
                session: session.clone(),
            },
            activity_opts(),
        )
        .await?)
}

/// Execute one **repertoire** step against the activity surface and return
/// the [`Observation`] the workflow pushes into the session for the next
/// step. `Idle` is terminal and never reaches here.
///
/// Failures (tool errors, a missing file, a rejected spawn, a bad reconcile)
/// come back as a failure `Observation` the model adapts to within the same
/// cycle — there is no cross-cycle correction state on the Temporal path
/// either. Genuine infra errors still propagate via `?`.
async fn execute_action(
    ctx: &WorkflowContext<AgentWorkflow>,
    input: &AgentInput,
    action: &Decision,
) -> WorkflowResult<Observation> {
    match action {
        Decision::CallTools { calls } => dispatch_call_tools(ctx, input, calls.clone()).await,
        Decision::WriteOutput { body, citations } => {
            write_output(ctx, input, body.clone(), citations.clone()).await
        }
        Decision::RewriteFs { ops } => rewrite_fs(ctx, &input.fs_handle, ops.clone()).await,
        Decision::Read { .. } | Decision::List { .. } | Decision::Search { .. } => {
            read_fs(ctx, &input.fs_handle, action).await
        }
        Decision::SpawnChild {
            agent_name,
            mandate,
        } => spawn_child(ctx, input, agent_name.clone(), mandate.clone()).await,
        Decision::ReconcileChildren { sources, conflict } => {
            reconcile_children(ctx, input, sources.clone(), conflict.clone()).await
        }
        Decision::RetireChild { child_ref, reason } => {
            retire_child(ctx, child_ref, reason.clone()).await;
            Ok(Observation::ok(format!(
                "retired child {}",
                child_ref.agent_id
            )))
        }
        // Replacement is NOT in-place — the new child gets a fresh
        // `agent_id` + `workflow_id` + `edges` row. The old `edges` row stays
        // as an audit trail. If `spawn_child` errors after the old child has
        // been retire-signaled there is no rollback — the error propagates so
        // Temporal's activity-failure surface makes the partial state
        // operator-visible.
        Decision::ReplaceChild {
            child_ref,
            new_mandate,
        } => {
            let replacement_name = format!("replacement-of-{}", child_ref.agent_id);
            retire_child(ctx, child_ref, format!("replaced by {replacement_name}")).await;
            spawn_child(ctx, input, replacement_name, new_mandate.clone()).await
        }
        Decision::Idle { .. } => {
            unreachable!("Idle is terminal; the cycle loop handles it before execute_action")
        }
    }
}

/// Invoke the `read_fs` activity for a `Read`/`List`/`Search` step and return
/// the resulting observation (the file body, the listing, the matches, or a
/// recoverable "not found" failure observation).
async fn read_fs(
    ctx: &WorkflowContext<AgentWorkflow>,
    fs_handle: &FsHandle,
    action: &Decision,
) -> WorkflowResult<Observation> {
    let op = match action {
        Decision::Read { path } => FsNavOp::Read { path: path.clone() },
        Decision::List { path } => FsNavOp::List { path: path.clone() },
        Decision::Search { query, path } => FsNavOp::Search {
            query: query.clone(),
            path: path.clone(),
        },
        _ => unreachable!("read_fs only handles Read/List/Search"),
    };
    let out = ctx
        .start_activity(
            AgentActivities::read_fs,
            ReadFsInput {
                fs_handle: fs_handle.clone(),
                op,
            },
            activity_opts(),
        )
        .await?;
    Ok(out.observation)
}

/// Invoke the `persist_output` activity for a `Decision::WriteOutput`.
///
/// Returns the observation the model reasons over next. A model-authored
/// provenance mistake (empty citations, a citation that doesn't resolve)
/// comes back as a failure observation with no `output_id` — nothing
/// persisted, so the parent is not signaled and the model corrects in-cycle.
async fn write_output(
    ctx: &WorkflowContext<AgentWorkflow>,
    input: &AgentInput,
    body: String,
    citations: Vec<String>,
) -> WorkflowResult<Observation> {
    let out = ctx
        .start_activity(
            AgentActivities::persist_output,
            PersistOutputInput {
                cfg: input.cfg.clone(),
                fs_handle: input.fs_handle.clone(),
                agent_id: input.agent_id,
                body,
                citations,
            },
            activity_opts(),
        )
        .await?;
    if let (Some(output_id), Some(parent)) = (out.output_id, &input.parent_handle) {
        let trigger = Trigger::ChildOutput {
            child_ref: AgentRef::new(ctx.workflow_id().to_string(), input.agent_id),
            agent_name: input.agent_name.clone(),
            output_id,
        };
        signal_parent_with_trigger(ctx, parent, trigger).await;
    }
    Ok(out.observation)
}

/// Fire a [`Trigger`] payload at the parent workflow via the SDK's
/// `ExternalWorkflowHandle::signal`. Errors are logged + swallowed:
/// cross-workflow signaling is best-effort — the child's data is durable on
/// its own FS regardless of whether the parent observed the signal.
///
/// Building the typed [`Trigger`] is the caller's job — `ChildOutput` and
/// `ChildRetired` each carry distinct fields that depend on workflow-local
/// state (output id vs. retirement reason) the helper shouldn't abstract over.
async fn signal_parent_with_trigger(
    ctx: &WorkflowContext<AgentWorkflow>,
    parent: &ParentRef,
    trigger: Trigger,
) {
    // SDK two-step: handle = external_workflow(workflow_id, run_id), then
    // handle.signal(SignalDef, payload). `run_id = None` targets the latest
    // run (the parent's currently-active execution).
    let result = ctx
        .external_workflow(parent.workflow_id.clone(), None)
        .signal(AgentWorkflow::external_signal, trigger)
        .await;
    if let Err(failure) = result {
        tracing::warn!(
            parent_workflow_id = %parent.workflow_id,
            error = ?failure,
            "signal_external_workflow to parent failed; child continuing best-effort"
        );
    }
}

/// Invoke the `apply_fs_ops` activity for a `Decision::RewriteFs`.
///
/// `Mandate::new("", Duration::ZERO, None)` is decorative because
/// `AgentFs::new_with_storage` only writes `mandate.md` when absent, and
/// `apply_fs_ops` runs only against agents whose `mandate.md` already
/// exists on disk (build_seed wrote it on cycle 1). The activity body
/// never reads the mandate — it only forwards it to `new_with_storage`,
/// which short-circuits the write.
async fn rewrite_fs(
    ctx: &WorkflowContext<AgentWorkflow>,
    fs_handle: &FsHandle,
    ops: Vec<coral_node::decision::FsOp>,
) -> WorkflowResult<Observation> {
    let out = ctx
        .start_activity(
            AgentActivities::apply_fs_ops,
            ApplyFsOpsInput {
                fs_handle: fs_handle.clone(),
                mandate: Mandate::new("", Duration::ZERO, None),
                ops,
            },
            activity_opts(),
        )
        .await?;
    Ok(out.observation)
}

/// Invoke the `append_decision_log` activity for the current tick's
/// decision. Called by the loop body right after `decide(...)` returns and
/// before the dispatch arm — see the call site for the "artifact even on
/// dispatch error" rationale.
async fn log_decision(
    ctx: &WorkflowContext<AgentWorkflow>,
    fs_handle: &FsHandle,
    tick: u64,
    step: u64,
    decision: &Decision,
) -> WorkflowResult<()> {
    ctx.start_activity(
        AgentActivities::append_decision_log,
        AppendDecisionLogInput {
            fs_handle: fs_handle.clone(),
            tick,
            step,
            decision_summary: decision_summary(decision),
        },
        activity_opts(),
    )
    .await?;
    Ok(())
}

/// Invoke the `commit_tick` activity for the just-completed cycle. `tick` is
/// the cycle's counter (read before the post-cycle bump), so the commit lands
/// under the tick the cycle ran as and the message stays deterministic across
/// retries.
async fn commit_tick(
    ctx: &WorkflowContext<AgentWorkflow>,
    fs_handle: &FsHandle,
    tick: u64,
) -> WorkflowResult<()> {
    ctx.start_activity(
        AgentActivities::commit_tick,
        CommitTickInput {
            fs_handle: fs_handle.clone(),
            tick,
        },
        activity_opts(),
    )
    .await?;
    Ok(())
}

/// Render a one-line, human-readable summary of a [`Decision`] for the
/// decision log artifact. Format is not part of any wire contract — the TUI
/// parses the JSONL line's `decision_summary` string verbatim.
fn decision_summary(decision: &Decision) -> String {
    match decision {
        Decision::CallTools { calls } => format!("CallTools {{ count: {} }}", calls.len()),
        Decision::WriteOutput { citations, .. } => {
            format!("WriteOutput {{ citations: {} }}", citations.len())
        }
        Decision::RewriteFs { ops } => format!("RewriteFs {{ ops: {} }}", ops.len()),
        Decision::Read { path } => format!("Read {{ path: {path:?} }}"),
        Decision::List { path } => format!("List {{ path: {path:?} }}"),
        Decision::Search { query, .. } => format!("Search {{ query: {query:?} }}"),
        Decision::Idle { next_after } => {
            format!("Idle {{ next_after_ms: {} }}", next_after.as_millis())
        }
        Decision::SpawnChild { agent_name, .. } => {
            format!("SpawnChild {{ agent_name: {agent_name:?} }}")
        }
        Decision::ReconcileChildren { sources, conflict } => format!(
            "ReconcileChildren {{ sources: {}, conflict: {} }}",
            sources.len(),
            conflict.is_some(),
        ),
        Decision::RetireChild { child_ref, reason } => format!(
            "RetireChild {{ agent_id: {}, reason: {reason:?} }}",
            child_ref.agent_id,
        ),
        Decision::ReplaceChild { child_ref, .. } => {
            format!("ReplaceChild {{ agent_id: {} }}", child_ref.agent_id)
        }
    }
}

/// Invoke the `persist_retirement` activity and return the workflow result.
/// Shared between the retire-signal short-circuit and the `step_cap` cap.
///
/// After `persist_retirement` returns and before the workflow exits, if
/// `input.parent_handle.is_some()` the workflow body fires one final
/// `Trigger::ChildRetired` at the parent via [`signal_parent_with_trigger`].
/// The signal is best-effort: failure is logged but does NOT prevent the
/// child from exiting cleanly — `retirement.json` is durable on the child's
/// own FS regardless of whether the parent observed the signal. Orphan
/// children (`parent_handle.is_none()`) skip the signal step entirely.
async fn retire(
    ctx: &WorkflowContext<AgentWorkflow>,
    input: &AgentInput,
    reason: String,
) -> WorkflowResult<AgentResult> {
    ctx.start_activity(
        AgentActivities::persist_retirement,
        PersistRetirementInput {
            fs_handle: input.fs_handle.clone(),
            reason: reason.clone(),
        },
        activity_opts(),
    )
    .await?;
    if let Some(parent) = &input.parent_handle {
        let trigger = Trigger::ChildRetired {
            child_ref: AgentRef::new(ctx.workflow_id().to_string(), input.agent_id),
            agent_name: input.agent_name.clone(),
            reason: reason.clone(),
        };
        signal_parent_with_trigger(ctx, parent, trigger).await;
    }
    Ok(AgentResult::Retired { reason })
}

/// Owned payload produced by [`drain_buckets`] — the per-cycle view of every
/// signal-staged bucket. Kept distinct from `BuildSeedInput` because the
/// workflow body short-circuits on `retirement` before building a seed.
struct DrainedBuckets {
    triggers: Vec<Trigger>,
    human_ops: Vec<HumanOp>,
    mandate_patches: Vec<MandatePatch>,
    retirement: Option<String>,
}

/// Drain the signal-tracked fields out of workflow state into owned values.
///
/// `cumulative_*_observed` counters are bumped by the signal handlers at
/// receipt time (not here at drain time) so a snapshot taken between a
/// signal landing and the next loop tick still reflects the arrival.
fn drain_buckets(s: &mut AgentWorkflow) -> DrainedBuckets {
    DrainedBuckets {
        triggers: std::mem::take(&mut s.pending_triggers),
        human_ops: std::mem::take(&mut s.pending_human_ops),
        mandate_patches: std::mem::take(&mut s.pending_mandate_patches),
        retirement: s.retirement_request.take(),
    }
}

/// Build the standard activity options.
fn activity_opts() -> ActivityOptions {
    ActivityOptions::start_to_close_timeout(ACTIVITY_TIMEOUT)
}

/// Fan out N `execute_tool` activity invocations via the SDK's deterministic
/// `workflows::join_all`, then summarize the batch into an [`Observation`]
/// the inner loop pushes into the session. On failure the observation
/// carries the per-call failure text the model adapts to on its next step —
/// the Temporal path does not ape `agent_core`'s budget state machine.
async fn dispatch_call_tools(
    ctx: &WorkflowContext<AgentWorkflow>,
    input: &AgentInput,
    calls: Vec<ToolCall>,
) -> WorkflowResult<Observation> {
    let futures = calls.into_iter().map(|call| {
        ctx.start_activity(
            AgentActivities::execute_tool,
            ExecuteToolInput {
                cfg: input.cfg.clone(),
                fs_handle: input.fs_handle.clone(),
                graph_id: input.graph_id,
                allowed_tools: input.mandate.tools.clone(),
                call,
            },
            activity_opts(),
        )
    });
    let results = temporalio_sdk::workflows::join_all(futures).await;

    let mut failures: Vec<ToolCallFailure> = Vec::new();
    let mut recorded: Vec<String> = Vec::new();
    for r in results {
        match r? {
            ToolCallOutcome::Success { evidence_path } => recorded.push(evidence_path),
            ToolCallOutcome::Failure { failure } => failures.push(failure),
        }
    }
    if failures.is_empty() {
        Ok(Observation::ok(format!(
            "{} tool call(s) succeeded; cite this evidence: {}",
            recorded.len(),
            recorded.join(", ")
        )))
    } else {
        Ok(Observation::err(format_correction(&failures)))
    }
}

/// Construct the [`AgentInput`] for a freshly-spawned child workflow.
/// Shared between the `Decision::SpawnChild` arm of [`AgentWorkflow::run`]
/// and the `coral apply` walker so the two surfaces cannot drift on
/// `parent_handle` shape, FS prefix layout, or inherited cfg.
///
/// The child shares the parent's `graph_id` rather than getting a fresh one
/// — only `agent_id` is fresh per spawn. Returns an `AgentInput` with
/// `carryover: None` (fresh first run) and `parent_handle: Some(..)`
/// populated to route child → parent signals back to the parent.
pub fn build_child_input(
    parent_workflow_id: &str,
    parent_agent_id: AgentId,
    parent_graph_id: GraphId,
    child_agent_id: AgentId,
    child_agent_name: String,
    child_mandate: Mandate,
    inherited_cfg: AgentConfig,
) -> AgentInput {
    // `parent_agent_id` is on the signature for symmetry + future use (e.g.
    // a `parent_handle.agent_id` field for routing), but today's
    // `ParentRef` shape only carries the workflow id. Acknowledge the
    // binding so clippy's unused-variable lint doesn't fire and a future
    // field addition doesn't need a new positional argument.
    let _ = parent_agent_id;
    AgentInput {
        cfg: inherited_cfg,
        fs_handle: FsHandle::for_agent(parent_graph_id, child_agent_id),
        parent_handle: Some(ParentRef {
            workflow_id: parent_workflow_id.to_string(),
            signal: ParentRef::DEFAULT_SIGNAL.to_string(),
        }),
        carryover: None,
        mandate: child_mandate,
        graph_id: parent_graph_id,
        agent_id: child_agent_id,
        agent_name: child_agent_name,
    }
}

/// Construct the [`AgentInput`] for a freshly-applied **root** agent (no
/// parent). Counterpart to [`build_child_input`]; the only difference is
/// `parent_handle: None`.
///
/// For roots there's nothing to inherit from, so the caller passes
/// `AgentConfig::default()` as `cfg`.
pub fn build_root_input(
    graph_id: GraphId,
    agent_id: AgentId,
    agent_name: String,
    mandate: Mandate,
    cfg: AgentConfig,
) -> AgentInput {
    AgentInput {
        cfg,
        fs_handle: FsHandle::for_agent(graph_id, agent_id),
        parent_handle: None,
        carryover: None,
        mandate,
        graph_id,
        agent_id,
        agent_name,
    }
}

/// The `Decision::SpawnChild` workflow arm body.
///
/// Sequence:
///
/// 1. Invoke `register_child_in_structural_db` activity — writes the
///    child's `agents` row + parent→child `edges` row, returns the
///    freshly-minted `AgentId`.
/// 2. Construct child workflow id (`graphs/<gid>/agents/<child_aid>`).
/// 3. Build the child's `AgentInput` via [`build_child_input`].
/// 4. `ctx.child_workflow(AgentWorkflow::run, ..)` with
///    `ParentClosePolicy::Abandon`. The `.await` here resolves once the
///    child workflow has *started*, not when it completes.
/// 5. Drop the started child handle without awaiting its result. The parent
///    does NOT block on the child; the child runs independently and reports
///    back via the `signal_external_workflow` path.
/// 6. Push the child's `AgentRef` onto `self.child_handles` for later
///    snapshot / reconcile / retire reads + carryover round-trip.
async fn spawn_child(
    ctx: &WorkflowContext<AgentWorkflow>,
    input: &AgentInput,
    child_agent_name: String,
    child_mandate: Mandate,
) -> WorkflowResult<Observation> {
    let reg = ctx
        .start_activity(
            AgentActivities::register_child_in_structural_db,
            RegisterChildInStructuralDbInput {
                parent_graph_id: input.graph_id,
                parent_agent_id: input.agent_id,
                child_agent_name: child_agent_name.clone(),
                child_tools: child_mandate.tools.clone(),
            },
            activity_opts(),
        )
        .await?;
    let child_agent_id = match reg {
        RegisterChildOutcome::Registered { child_agent_id } => child_agent_id,
        // A grant the graph doesn't define is a model error: return it as a
        // failure observation the model adapts to next step, leaving the
        // parent running rather than terminating it over a bad spawn.
        RegisterChildOutcome::RejectedUnknownTool { tool } => {
            return Ok(Observation::err(format!(
                "spawn rejected: tool {tool:?} is not defined in this graph; \
                 grant the child only tools this graph defines"
            )));
        }
    };

    let child_workflow_id =
        agent_workflow_id(&input.graph_id.to_string(), &child_agent_id.to_string());
    let parent_workflow_id = ctx.workflow_id().to_string();
    let observation = Observation::ok(format!("spawned child {child_agent_name}"));
    let child_input = build_child_input(
        &parent_workflow_id,
        input.agent_id,
        input.graph_id,
        child_agent_id,
        child_agent_name,
        child_mandate,
        input.cfg.clone(),
    );

    // Every child is spawned with `ParentClosePolicy::Abandon` so it
    // survives parent CAN, parent restart, and even parent retirement. The
    // only kill path is `Decision::RetireChild`.
    //
    // The SDK's `child_workflow(..)` returns a future that resolves once
    // the child has *started*; we await that (to surface a start failure as
    // a workflow error) and then drop the started handle without awaiting
    // its `.result()` — detached. Awaiting `started.result()` would block
    // the parent for the child's full lifetime, defeating the whole
    // `Abandon` design.
    let opts = ChildWorkflowOptions {
        workflow_id: child_workflow_id.clone(),
        parent_close_policy: ParentClosePolicy::Abandon,
        ..Default::default()
    };
    let started = ctx
        .child_workflow(AgentWorkflow::run, child_input, opts)
        .await
        .map_err(|e| anyhow::anyhow!("child_workflow start failed: {e:?}"))?;
    drop(started);

    ctx.state_mut(|s| {
        s.child_handles
            .push(AgentRef::new(child_workflow_id, child_agent_id));
    });
    Ok(observation)
}

/// The `Decision::RetireChild` workflow arm body (also reused by
/// `Decision::ReplaceChild`'s retire half).
///
/// Sequence:
///
/// 1. Fire `AgentWorkflow::retire` at the child's workflow via the SDK
///    two-step `external_workflow().signal()` chain.
/// 2. Log + continue on signal failure: if the child already exited (or its
///    workflow id is wrong) the signal fails and the parent proceeds. The
///    child's exit is durable on its own FS regardless of whether this
///    signal lands.
/// 3. Remove the child's `AgentRef` from `self.child_handles` so the
///    parent's snapshot / future reconcile / future retire paths see only
///    the live child set. Round-trips through CAN.
async fn retire_child(ctx: &WorkflowContext<AgentWorkflow>, child_ref: &AgentRef, reason: String) {
    let result = ctx
        .external_workflow(child_ref.workflow_id.clone(), None)
        .signal(AgentWorkflow::retire, reason)
        .await;
    if let Err(failure) = result {
        // A child that already exited (e.g. retired naturally on a previous
        // tick) is the common case here, not a hard error.
        tracing::warn!(
            child_workflow_id = %child_ref.workflow_id,
            child_agent_id = %child_ref.agent_id,
            error = ?failure,
            "signal_external_workflow(retire) to child failed; parent continuing best-effort"
        );
    }
    // Drop the child from the parent's live-handle set regardless of signal
    // outcome — the intent ("this child is gone from the parent's model")
    // is the load-bearing state mutation; the signal is best-effort delivery.
    let target_agent_id = child_ref.agent_id;
    ctx.state_mut(|s| {
        s.child_handles.retain(|h| h.agent_id != target_agent_id);
    });
}

/// The `Decision::ReconcileChildren` workflow arm body.
///
/// Calls the `reconcile_children` activity (which opens the parent's FS +
/// each child's FS read-only, writes one synthetic evidence record per
/// source into the parent's `evidence/`, and returns their paths). The
/// parent pulls the synthetic records on a later step via `List`/`Read` of
/// `evidence/` to cite them in a subsequent `WriteOutput` — no workflow-state
/// slot is needed.
///
/// Errors do NOT propagate via `?` — that would fail the whole workflow on
/// a single bad source. Instead the typed activity failure is returned as a
/// failure [`Observation`] the model adapts to next step, mirroring the
/// `Decision::CallTools` tool-failure flow.
async fn reconcile_children(
    ctx: &WorkflowContext<AgentWorkflow>,
    input: &AgentInput,
    sources: Vec<ReconcileSource>,
    conflict: Option<ConflictRecordIntent>,
) -> WorkflowResult<Observation> {
    let source_count = sources.len();
    let activity_input = ReconcileChildrenInput {
        parent_graph_id: input.graph_id,
        parent_agent_id: input.agent_id,
        sources,
        conflict,
    };
    match ctx
        .start_activity(
            AgentActivities::reconcile_children,
            activity_input,
            activity_opts(),
        )
        .await
    {
        Ok(out) => Ok(Observation::ok(format!(
            "reconciled {source_count} child source(s); cite this evidence: {}",
            out.synthetic_evidence.join(", ")
        ))),
        // The activity returned an `ApplicationFailure` carrying either a
        // typed `ReconciliationError` (non-retryable, structural) or a wrapped
        // transient error (retryable). Either way the failure becomes an
        // observation the model adapts to.
        Err(failure) => Ok(Observation::err(format!(
            "reconcile: activity failed: {failure:?}"
        ))),
    }
}

/// Render the staged correction text for a tool-batch failure.
fn format_correction(failures: &[ToolCallFailure]) -> String {
    if failures.len() == 1 {
        format!(
            "call_tool {:?} failed: {} (args={})",
            failures[0].tool, failures[0].error, failures[0].args
        )
    } else {
        let mut s = format!(
            "{} parallel call_tool(s) failed after exhausting retries:",
            failures.len()
        );
        for f in failures {
            s.push_str(&format!("\n- {:?}: {} (args={})", f.tool, f.error, f.args));
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_workflow_id_is_url_shaped() {
        assert_eq!(agent_workflow_id("g1", "a1"), "graphs/g1/agents/a1");
    }

    #[test]
    fn agent_workflow_id_passes_components_through_unchanged() {
        assert_eq!(
            agent_workflow_id("graph-with-dashes", "agent_with_underscores"),
            "graphs/graph-with-dashes/agents/agent_with_underscores"
        );
        assert_eq!(agent_workflow_id("", ""), "graphs//agents/");
    }

    #[test]
    fn agent_input_new_for_test_has_no_carryover_and_no_parent() {
        let input = AgentInput::new_for_test(
            GraphId::new(uuid::Uuid::nil()),
            AgentId::new(uuid::Uuid::nil()),
            "root",
        );
        assert!(input.carryover.is_none());
        assert!(input.parent_handle.is_none());
        assert_eq!(input.agent_name, "root");
    }

    #[test]
    fn agent_input_roundtrips_through_json() {
        let input = AgentInput {
            cfg: AgentConfig::default(),
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            parent_handle: None,
            carryover: Some(Carryover::default()),
            mandate: Mandate::new("hello", Duration::from_millis(123), Some(7)),
            graph_id: GraphId::new(uuid::Uuid::from_u128(0xAB)),
            agent_id: AgentId::new(uuid::Uuid::from_u128(0xCD)),
            agent_name: "root".into(),
        };
        let json = serde_json::to_string(&input).expect("serialize AgentInput");
        let back: AgentInput = serde_json::from_str(&json).expect("deserialize AgentInput");
        assert_eq!(input, back);
    }

    #[test]
    fn agent_result_default_is_retired_with_empty_reason() {
        let r = AgentResult::default();
        assert!(matches!(r, AgentResult::Retired { reason } if reason.is_empty()));
    }

    #[test]
    fn agent_result_roundtrips_through_json() {
        let r = AgentResult::Retired {
            reason: "op asked".into(),
        };
        let s = serde_json::to_string(&r).unwrap();
        assert!(s.contains("\"result\":\"retired\""), "wire shape: {s}");
        assert!(s.contains("\"reason\":\"op asked\""), "wire shape: {s}");
        let back: AgentResult = serde_json::from_str(&s).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn agent_result_default_roundtrips_through_json() {
        let r = AgentResult::default();
        let s = serde_json::to_string(&r).unwrap();
        let back: AgentResult = serde_json::from_str(&s).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn agent_snapshot_default_roundtrips_through_json() {
        let s = AgentSnapshot::default();
        let json = serde_json::to_string(&s).unwrap();
        let back: AgentSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
        assert_eq!(s.pending_triggers_count, 0);
        assert_eq!(s.pending_human_ops_count, 0);
        assert_eq!(s.pending_mandate_patches_count, 0);
        assert!(s.retirement_request.is_none());
        assert!(s.recent_output_ids.is_empty());
        assert_eq!(s.cumulative_triggers_observed, 0);
        assert_eq!(s.cumulative_human_ops_observed, 0);
        assert_eq!(s.cumulative_mandate_patches_observed, 0);
    }

    #[test]
    fn agent_snapshot_accepts_wire_shape_without_cumulative_counters() {
        // `cumulative_*_observed` fields are `#[serde(default)]` so a wire
        // form missing them still deserializes — the counters default to 0.
        let without_counters = r#"{
            "pending_triggers_count": 2,
            "pending_human_ops_count": 1,
            "pending_mandate_patches_count": 3,
            "retirement_request": "shutdown",
            "recent_output_ids": []
        }"#;
        let s: AgentSnapshot = serde_json::from_str(without_counters).unwrap();
        assert_eq!(s.pending_triggers_count, 2);
        assert_eq!(s.cumulative_triggers_observed, 0);
        assert_eq!(s.cumulative_human_ops_observed, 0);
        assert_eq!(s.cumulative_mandate_patches_observed, 0);
    }

    #[test]
    fn agent_snapshot_with_signals_roundtrips_through_json() {
        let s = AgentSnapshot {
            pending_triggers_count: 2,
            pending_human_ops_count: 1,
            pending_mandate_patches_count: 3,
            retirement_request: Some("shutdown for upgrade".into()),
            recent_output_ids: Vec::new(),
            cumulative_triggers_observed: 5,
            cumulative_human_ops_observed: 7,
            cumulative_mandate_patches_observed: 11,
        };
        let json = serde_json::to_string(&s).unwrap();
        let back: AgentSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn agent_snapshot_from_state_projects_bucket_lengths_and_retirement() {
        let mut wf = AgentWorkflow::default();
        wf.pending_triggers.push(Trigger::ScheduledWake);
        wf.pending_triggers.push(Trigger::External {
            kind: "k".into(),
            payload: serde_json::json!({}),
        });
        wf.pending_human_ops
            .push(HumanOp::new(serde_json::json!({"action": "pause"})));
        wf.pending_mandate_patches
            .push(MandatePatch::new(serde_json::json!({"model": "gpt-x"})));
        wf.pending_mandate_patches
            .push(MandatePatch::new(serde_json::json!({"budget_ms": 5000})));
        wf.retirement_request = Some("op asked".into());

        let snap = AgentSnapshot::from_state(&wf);
        assert_eq!(snap.pending_triggers_count, 2);
        assert_eq!(snap.pending_human_ops_count, 1);
        assert_eq!(snap.pending_mandate_patches_count, 2);
        assert_eq!(snap.retirement_request.as_deref(), Some("op asked"));
        assert!(snap.recent_output_ids.is_empty());
    }

    #[test]
    fn agent_workflow_default_has_empty_buckets_and_no_retirement() {
        let wf = AgentWorkflow::default();
        assert!(wf.pending_triggers.is_empty());
        assert!(wf.pending_human_ops.is_empty());
        assert!(wf.pending_mandate_patches.is_empty());
        assert!(wf.retirement_request.is_none());
        assert!(wf.next_wake.is_none());
        assert_eq!(wf.cumulative_triggers_observed, 0);
        assert_eq!(wf.cumulative_human_ops_observed, 0);
        assert_eq!(wf.cumulative_mandate_patches_observed, 0);
    }

    #[test]
    fn drain_buckets_takes_all_state_and_clears_workflow() {
        // Critical that all buckets get cleared so a redundant retire
        // signal arriving mid-tick doesn't trip the next iteration's
        // short-circuit.
        let mut wf = AgentWorkflow::default();
        wf.pending_triggers.push(Trigger::ScheduledWake);
        wf.pending_human_ops
            .push(HumanOp::new(serde_json::json!({"a": 1})));
        wf.pending_mandate_patches
            .push(MandatePatch::new(serde_json::json!({"m": 1})));
        wf.retirement_request = Some("done".into());

        let drained = drain_buckets(&mut wf);
        assert_eq!(drained.triggers.len(), 1);
        assert_eq!(drained.human_ops.len(), 1);
        assert_eq!(drained.mandate_patches.len(), 1);
        assert_eq!(drained.retirement.as_deref(), Some("done"));

        assert!(wf.pending_triggers.is_empty());
        assert!(wf.pending_human_ops.is_empty());
        assert!(wf.pending_mandate_patches.is_empty());
        assert!(wf.retirement_request.is_none());

        // drain_buckets itself does NOT bump cumulative counters; the
        // signal handlers do that at receipt time. The buckets were
        // populated directly here, bypassing the signal path, so counters
        // stay at 0.
        assert_eq!(wf.cumulative_triggers_observed, 0);
        assert_eq!(wf.cumulative_human_ops_observed, 0);
        assert_eq!(wf.cumulative_mandate_patches_observed, 0);
    }

    #[test]
    fn signal_handlers_bump_cumulative_counters_at_receipt() {
        // Mutating the bare fields directly because the SDK's
        // SyncWorkflowContext can't be constructed in a unit test — the
        // handler body invariant we care about is bucket push + counter
        // bump.
        let mut wf = AgentWorkflow::default();

        wf.pending_triggers.push(Trigger::ScheduledWake);
        wf.cumulative_triggers_observed = wf.cumulative_triggers_observed.saturating_add(1);
        wf.pending_human_ops
            .push(HumanOp::new(serde_json::json!({"a": 1})));
        wf.cumulative_human_ops_observed = wf.cumulative_human_ops_observed.saturating_add(1);
        wf.pending_mandate_patches
            .push(MandatePatch::new(serde_json::json!({"m": 1})));
        wf.cumulative_mandate_patches_observed =
            wf.cumulative_mandate_patches_observed.saturating_add(1);

        assert_eq!(wf.cumulative_triggers_observed, 1);
        assert_eq!(wf.cumulative_human_ops_observed, 1);
        assert_eq!(wf.cumulative_mandate_patches_observed, 1);

        let _ = drain_buckets(&mut wf);
        assert_eq!(wf.cumulative_triggers_observed, 1);
        assert_eq!(wf.cumulative_human_ops_observed, 1);
        assert_eq!(wf.cumulative_mandate_patches_observed, 1);
    }

    #[test]
    fn format_correction_single_failure_quotes_tool_and_includes_error_and_args() {
        let f = ToolCallFailure {
            tool: "search_web".into(),
            args: serde_json::json!({"q": "rust"}),
            error: "503".into(),
        };
        let s = format_correction(&[f]);
        assert!(s.contains("\"search_web\""), "got: {s}");
        assert!(s.contains("503"), "got: {s}");
        assert!(s.contains("\"q\""), "got: {s}");
    }

    /// Fully-populated [`Carryover`] fixture — the JSON round-trip and
    /// hydrate/encode tests below all build against this so a future field
    /// addition shows up as a test miss if not represented.
    fn fully_populated_carryover() -> Carryover {
        use uuid::Uuid;
        Carryover {
            pending_triggers: vec![
                Trigger::ScheduledWake,
                Trigger::External {
                    kind: "webhook".into(),
                    payload: serde_json::json!({"k": "v"}),
                },
            ],
            pending_human_ops: vec![HumanOp::new(serde_json::json!({"action": "pause"}))],
            pending_mandate_patches: vec![MandatePatch::new(serde_json::json!({"model": "gpt-x"}))],
            retirement_request: Some("op asked".into()),
            scheduler_cursor: SchedulerCursor {
                next_wake: Some(Duration::from_millis(250)),
            },
            child_handles: vec![AgentRef::new(
                "graphs/g1/agents/c1",
                AgentId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap()),
            )],
            last_output_id: Some(OutputId::from_hex("ab".repeat(32))),
            cumulative_triggers_observed: 5,
            cumulative_human_ops_observed: 7,
            cumulative_mandate_patches_observed: 11,
            tick: 13,
            in_flight: Some(sample_session()),
        }
    }

    /// Small in-flight [`Session`] fixture for the mid-cycle carryover tests:
    /// a seed plus one completed step, enough to exercise the
    /// in_flight-carry round trip without approaching the size guard.
    fn sample_session() -> Session {
        let seed = Seed::new(
            Mandate::new("resume me", Duration::from_millis(10), Some(5)),
            vec![Trigger::ScheduledWake],
            coral_node::decision::FsIndex::default(),
        );
        let mut session = Session::new(seed);
        session.push(
            Decision::Read {
                path: "notes/plan.md".into(),
            },
            Observation::ok("the plan body"),
        );
        session
    }

    #[test]
    fn default_carryover_has_no_in_flight_session() {
        assert!(Carryover::default().in_flight.is_none());
    }

    #[test]
    fn carryover_with_in_flight_session_roundtrips_through_json() {
        let c = Carryover {
            in_flight: Some(sample_session()),
            tick: 4,
            ..Carryover::default()
        };
        let json = serde_json::to_string(&c).expect("serialize in-flight Carryover");
        let back: Carryover = serde_json::from_str(&json).expect("deserialize in-flight Carryover");
        assert_eq!(c, back);
        assert_eq!(back.in_flight.as_ref().map(|s| s.len()), Some(1));
    }

    #[test]
    fn encode_carryover_never_emits_in_flight() {
        // The in-flight session lives in a run-loop local, never on the
        // workflow struct, so a boundary CAN (which goes through
        // `encode_carryover`) must always carry `in_flight: None`. Only the
        // mid-cycle site attaches a session.
        let mut wf = AgentWorkflow::default();
        wf.pending_triggers.push(Trigger::ScheduledWake);
        wf.tick = 9;
        assert!(wf.encode_carryover().in_flight.is_none());
    }

    #[test]
    fn mid_cycle_input_attaches_session_and_preserves_identity() {
        let input = AgentInput {
            cfg: AgentConfig::default(),
            fs_handle: FsHandle {
                prefix: "graphs/g/agents/a".into(),
            },
            parent_handle: None,
            carryover: None,
            mandate: Mandate::new("orig", Duration::from_millis(7), Some(3)),
            graph_id: GraphId::new(uuid::Uuid::from_u128(0xAB)),
            agent_id: AgentId::new(uuid::Uuid::from_u128(0xCD)),
            agent_name: "a".into(),
        };
        let base = Carryover {
            tick: 6,
            ..Carryover::default()
        };
        let session = sample_session();

        let candidate = mid_cycle_input(&input, base, session.clone());

        let carried = candidate.carryover.expect("carryover present");
        assert_eq!(carried.in_flight, Some(session));
        assert_eq!(carried.tick, 6);
        // Identity fields pass through untouched.
        assert_eq!(candidate.mandate, input.mandate);
        assert_eq!(candidate.graph_id, input.graph_id);
        assert_eq!(candidate.agent_id, input.agent_id);
        assert_eq!(candidate.fs_handle, input.fs_handle);
        assert_eq!(candidate.agent_name, input.agent_name);
    }

    #[test]
    fn carry_decision_classifies_at_thresholds() {
        assert_eq!(carry_decision(0), CarryDecision::Carry { warn: false });
        assert_eq!(
            carry_decision(CAN_PAYLOAD_WARN_BYTES),
            CarryDecision::Carry { warn: false },
            "exactly at the warn threshold still carries without warning (strict >)"
        );
        assert_eq!(
            carry_decision(CAN_PAYLOAD_WARN_BYTES + 1),
            CarryDecision::Carry { warn: true }
        );
        assert_eq!(
            carry_decision(CAN_PAYLOAD_HARD_BYTES),
            CarryDecision::Carry { warn: true },
            "exactly at the hard cap still carries (strict >)"
        );
        assert_eq!(
            carry_decision(CAN_PAYLOAD_HARD_BYTES + 1),
            CarryDecision::ForceIdle
        );
    }

    #[test]
    fn small_session_candidate_payload_carries_without_warning() {
        let input = AgentInput::new_for_test(
            GraphId::new(uuid::Uuid::nil()),
            AgentId::new(uuid::Uuid::nil()),
            "a",
        );
        let candidate = mid_cycle_input(&input, Carryover::default(), sample_session());
        let bytes = serde_json::to_vec(&candidate).unwrap().len();
        assert!(
            bytes < CAN_PAYLOAD_WARN_BYTES,
            "small session candidate ({bytes} bytes) should sit well under the warn threshold"
        );
        assert_eq!(carry_decision(bytes), CarryDecision::Carry { warn: false });
    }

    #[test]
    fn oversized_session_candidate_payload_force_idles() {
        // A session whose pushed observation alone exceeds the hard cap: the
        // measured candidate payload must trip the force-idle path so the
        // real CAN command can never exceed Temporal's blob-size limit.
        let seed = Seed::new(
            Mandate::new("big", Duration::from_millis(1), None),
            vec![Trigger::ScheduledWake],
            coral_node::decision::FsIndex::default(),
        );
        let mut session = Session::new(seed);
        session.push(
            Decision::Read {
                path: "notes/huge.md".into(),
            },
            Observation::ok("x".repeat(CAN_PAYLOAD_HARD_BYTES + 1)),
        );
        let input = AgentInput::new_for_test(
            GraphId::new(uuid::Uuid::nil()),
            AgentId::new(uuid::Uuid::nil()),
            "a",
        );
        let candidate = mid_cycle_input(&input, Carryover::default(), session);
        let bytes = serde_json::to_vec(&candidate).unwrap().len();
        assert!(bytes > CAN_PAYLOAD_HARD_BYTES, "candidate is {bytes} bytes");
        assert_eq!(carry_decision(bytes), CarryDecision::ForceIdle);
    }

    #[test]
    fn carryover_default_roundtrips_through_json() {
        let c = Carryover::default();
        let json = serde_json::to_string(&c).expect("serialize default Carryover");
        let back: Carryover = serde_json::from_str(&json).expect("deserialize default Carryover");
        assert_eq!(c, back);
    }

    #[test]
    fn carryover_fully_populated_roundtrips_through_json() {
        let c = fully_populated_carryover();
        let json = serde_json::to_string(&c).expect("serialize populated Carryover");
        let back: Carryover = serde_json::from_str(&json).expect("deserialize populated Carryover");
        assert_eq!(c, back);
    }

    #[test]
    fn agent_input_with_populated_carryover_roundtrips_through_json() {
        let input = AgentInput {
            cfg: AgentConfig::default(),
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            parent_handle: None,
            carryover: Some(fully_populated_carryover()),
            mandate: Mandate::new("populated-carryover", Duration::from_millis(50), None),
            graph_id: GraphId::new(uuid::Uuid::from_u128(0xAB)),
            agent_id: AgentId::new(uuid::Uuid::from_u128(0xCD)),
            agent_name: "root".into(),
        };
        let json = serde_json::to_string(&input).unwrap();
        let back: AgentInput = serde_json::from_str(&json).unwrap();
        assert_eq!(input, back);
    }

    #[test]
    fn encode_then_hydrate_is_identity_on_workflow_state() {
        let mut original = AgentWorkflow::default();
        original.pending_triggers.push(Trigger::ScheduledWake);
        original
            .pending_human_ops
            .push(HumanOp::new(serde_json::json!({"a": 1})));
        original
            .pending_mandate_patches
            .push(MandatePatch::new(serde_json::json!({"m": 1})));
        original.retirement_request = Some("op asked".into());
        original.next_wake = Some(Duration::from_millis(123));
        original.cumulative_triggers_observed = 9;
        original.cumulative_human_ops_observed = 13;
        original.cumulative_mandate_patches_observed = 17;
        original.last_output_id = Some(OutputId::from_hex("cd".repeat(32)));
        original.tick = 23;

        let c = original.encode_carryover();
        let mut hydrated = AgentWorkflow::default();
        hydrated.hydrate_from_carryover(c);

        assert_eq!(hydrated.pending_triggers, original.pending_triggers);
        assert_eq!(hydrated.pending_human_ops, original.pending_human_ops);
        assert_eq!(
            hydrated.pending_mandate_patches,
            original.pending_mandate_patches
        );
        assert_eq!(hydrated.retirement_request, original.retirement_request);
        assert_eq!(hydrated.next_wake, original.next_wake);
        assert_eq!(
            hydrated.cumulative_triggers_observed,
            original.cumulative_triggers_observed
        );
        assert_eq!(
            hydrated.cumulative_human_ops_observed,
            original.cumulative_human_ops_observed
        );
        assert_eq!(
            hydrated.cumulative_mandate_patches_observed,
            original.cumulative_mandate_patches_observed
        );
        assert_eq!(hydrated.last_output_id, original.last_output_id);
        assert_eq!(hydrated.tick, original.tick);
    }

    #[test]
    fn encode_then_serialize_then_deserialize_then_hydrate_round_trips_state() {
        // The full wire path that a real `continue_as_new` boundary
        // exercises: workflow state → encode_carryover → JSON (Temporal's
        // default payload codec) → JSON parse → hydrate_from_carryover →
        // workflow state on the new run.
        let mut pre_can = AgentWorkflow::default();
        pre_can.pending_triggers.push(Trigger::External {
            kind: "wire-roundtrip".into(),
            payload: serde_json::json!({"i": 42}),
        });
        pre_can
            .pending_human_ops
            .push(HumanOp::new(serde_json::json!({"a": "b"})));
        pre_can
            .pending_mandate_patches
            .push(MandatePatch::new(serde_json::json!({"m": "n"})));
        pre_can.retirement_request = Some("not yet".into());
        pre_can.next_wake = Some(Duration::from_millis(500));
        pre_can.cumulative_triggers_observed = 3;
        pre_can.cumulative_human_ops_observed = 5;
        pre_can.cumulative_mandate_patches_observed = 7;
        pre_can.last_output_id = Some(OutputId::from_hex("ef".repeat(32)));
        pre_can.tick = 19;

        let carryover_pre = pre_can.encode_carryover();
        let wire = serde_json::to_string(&carryover_pre).expect("wire-encode Carryover");
        let carryover_post: Carryover = serde_json::from_str(&wire).expect("wire-decode Carryover");
        let mut post_can = AgentWorkflow::default();
        post_can.hydrate_from_carryover(carryover_post);

        assert_eq!(post_can.pending_triggers, pre_can.pending_triggers);
        assert_eq!(post_can.pending_human_ops, pre_can.pending_human_ops);
        assert_eq!(
            post_can.pending_mandate_patches,
            pre_can.pending_mandate_patches
        );
        assert_eq!(post_can.retirement_request, pre_can.retirement_request);
        assert_eq!(post_can.next_wake, pre_can.next_wake);
        assert_eq!(
            post_can.cumulative_triggers_observed,
            pre_can.cumulative_triggers_observed
        );
        assert_eq!(
            post_can.cumulative_human_ops_observed,
            pre_can.cumulative_human_ops_observed
        );
        assert_eq!(
            post_can.cumulative_mandate_patches_observed,
            pre_can.cumulative_mandate_patches_observed
        );
        assert_eq!(post_can.last_output_id, pre_can.last_output_id);
        assert_eq!(post_can.tick, pre_can.tick);
    }

    #[test]
    fn hydrate_then_signal_handler_bumps_counter_past_carryover_value() {
        // The cumulative_*_observed counters must bridge a CAN boundary.
        // We can't construct a `SyncWorkflowContext` in a unit test (it's
        // SDK-private), so simulate the signal handler's effect by
        // replicating its `push + saturating_add` bookkeeping. The
        // load-bearing invariant is that the value the counter starts from
        // is the carryover's value, not zero.
        let pre_can = Carryover {
            cumulative_triggers_observed: 5,
            cumulative_human_ops_observed: 6,
            cumulative_mandate_patches_observed: 7,
            ..Carryover::default()
        };
        let mut wf = AgentWorkflow::default();
        wf.hydrate_from_carryover(pre_can);

        wf.pending_triggers.push(Trigger::ScheduledWake);
        wf.cumulative_triggers_observed = wf.cumulative_triggers_observed.saturating_add(1);

        // Cumulative view: 5 (pre-CAN) + 1 (post-CAN signal) = 6, NOT 1.
        assert_eq!(
            wf.cumulative_triggers_observed, 6,
            "post-CAN signal must increment past the carried value"
        );
        assert_eq!(wf.cumulative_human_ops_observed, 6);
        assert_eq!(wf.cumulative_mandate_patches_observed, 7);

        let snap = AgentSnapshot::from_state(&wf);
        assert_eq!(snap.cumulative_triggers_observed, 6);
        assert_eq!(snap.cumulative_human_ops_observed, 6);
        assert_eq!(snap.cumulative_mandate_patches_observed, 7);
    }

    #[test]
    fn carryover_from_default_workflow_is_default() {
        let wf = AgentWorkflow::default();
        let c = wf.encode_carryover();
        assert_eq!(c, Carryover::default());
    }

    #[test]
    fn scheduler_cursor_default_has_no_next_wake() {
        // The first-tick floor [`INITIAL_NEXT_WAKE`] is applied by the wake
        // gate when `next_wake.is_none()`, NOT by the SchedulerCursor
        // itself. Default cursor must surface a None.
        let c = SchedulerCursor::default();
        assert!(c.next_wake.is_none());
    }

    #[test]
    fn parent_ref_default_uses_external_signal_constant() {
        let p = ParentRef::default();
        assert!(p.workflow_id.is_empty());
        assert_eq!(p.signal, ParentRef::DEFAULT_SIGNAL);
        assert_eq!(p.signal, "external_signal");
    }

    #[test]
    fn parent_ref_round_trips_through_json() {
        let p = ParentRef {
            workflow_id: "graphs/g1/agents/parent".into(),
            signal: "custom_signal".into(),
        };
        let s = serde_json::to_string(&p).unwrap();
        let back: ParentRef = serde_json::from_str(&s).unwrap();
        assert_eq!(p, back);
        assert!(s.contains("\"workflow_id\":\"graphs/g1/agents/parent\""));
    }

    #[test]
    fn fs_handle_for_agent_uses_workflow_id_layout() {
        use uuid::Uuid;
        let g = GraphId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap());
        let a = AgentId::new(Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap());
        let h = FsHandle::for_agent(g, a);
        assert_eq!(
            h.prefix,
            "graphs/11111111-2222-3333-4444-555555555555/agents/66666666-7777-8888-9999-aaaaaaaaaaaa",
        );
    }

    #[test]
    fn build_child_input_populates_parent_handle_and_identity() {
        use uuid::Uuid;
        let parent_graph_id =
            GraphId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap());
        let parent_agent_id =
            AgentId::new(Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap());
        let child_agent_id =
            AgentId::new(Uuid::parse_str("bbbbbbbb-cccc-dddd-eeee-ffffffffffff").unwrap());
        let mandate = Mandate::new("child mandate", Duration::from_millis(500), Some(8));

        let input = build_child_input(
            "graphs/g1/agents/parent",
            parent_agent_id,
            parent_graph_id,
            child_agent_id,
            "fetcher".into(),
            mandate.clone(),
            AgentConfig::default(),
        );

        assert_eq!(input.graph_id, parent_graph_id);
        assert_eq!(input.agent_id, child_agent_id);
        assert_eq!(input.agent_name, "fetcher");
        assert_eq!(input.mandate, mandate);

        assert!(input.carryover.is_none());

        // FS prefix scopes under the parent's graph (NOT a fresh graph_id).
        assert_eq!(
            input.fs_handle.prefix,
            format!("graphs/{parent_graph_id}/agents/{child_agent_id}"),
        );

        let parent_handle = input
            .parent_handle
            .as_ref()
            .expect("build_child_input must populate parent_handle");
        assert_eq!(parent_handle.workflow_id, "graphs/g1/agents/parent");
        assert_eq!(parent_handle.signal, ParentRef::DEFAULT_SIGNAL);
    }

    #[test]
    fn child_handles_round_trip_via_carryover() {
        use uuid::Uuid;
        let h1 = AgentRef::new(
            "graphs/g1/agents/c1",
            AgentId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap()),
        );
        let h2 = AgentRef::new(
            "graphs/g1/agents/c2",
            AgentId::new(Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap()),
        );
        let mut wf = AgentWorkflow::default();
        wf.child_handles.push(h1.clone());
        wf.child_handles.push(h2.clone());

        let c = wf.encode_carryover();
        assert_eq!(c.child_handles, vec![h1.clone(), h2.clone()]);

        let json = serde_json::to_string(&c).expect("serialize carryover w/ child_handles");
        let c2: Carryover =
            serde_json::from_str(&json).expect("deserialize carryover w/ child_handles");
        assert_eq!(c2.child_handles, vec![h1.clone(), h2.clone()]);

        let mut wf2 = AgentWorkflow::default();
        wf2.hydrate_from_carryover(c2);
        assert_eq!(wf2.child_handles, vec![h1, h2]);
    }

    /// Simulate the workflow-state mutation `Decision::RetireChild`
    /// performs (drop the named child's [`AgentRef`] from `child_handles`)
    /// and assert the surviving set round-trips through [`Carryover`].
    ///
    /// We cannot construct a `WorkflowContext` in a unit test (it's SDK-
    /// private), so we exercise the load-bearing invariants at the
    /// workflow-state level.
    #[test]
    fn retire_child_removes_handle_and_survives_carryover() {
        use uuid::Uuid;
        let h1 = AgentRef::new(
            "graphs/g1/agents/c1",
            AgentId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap()),
        );
        let h2 = AgentRef::new(
            "graphs/g1/agents/c2",
            AgentId::new(Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap()),
        );
        let mut wf = AgentWorkflow::default();
        wf.child_handles.push(h1.clone());
        wf.child_handles.push(h2.clone());

        let target_agent_id = h1.agent_id;
        wf.child_handles.retain(|h| h.agent_id != target_agent_id);
        assert_eq!(wf.child_handles, vec![h2.clone()]);

        let c = wf.encode_carryover();
        let json = serde_json::to_string(&c).expect("serialize carryover");
        let c2: Carryover = serde_json::from_str(&json).expect("deserialize carryover");
        let mut wf2 = AgentWorkflow::default();
        wf2.hydrate_from_carryover(c2);
        assert_eq!(
            wf2.child_handles,
            vec![h2],
            "retire_child's removal must survive the CAN boundary",
        );
    }

    /// Simulate the workflow-state mutation `Decision::ReplaceChild`
    /// performs (drop the old child's [`AgentRef`], add the replacement's)
    /// and assert the swap round-trips through [`Carryover`].
    #[test]
    fn replace_child_swaps_handle_and_survives_carryover() {
        use uuid::Uuid;
        let old = AgentRef::new(
            "graphs/g1/agents/c1",
            AgentId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap()),
        );
        let other = AgentRef::new(
            "graphs/g1/agents/c2",
            AgentId::new(Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap()),
        );
        let replacement = AgentRef::new(
            "graphs/g1/agents/c3",
            AgentId::new(Uuid::parse_str("bbbbbbbb-cccc-dddd-eeee-ffffffffffff").unwrap()),
        );
        let mut wf = AgentWorkflow::default();
        wf.child_handles.push(old.clone());
        wf.child_handles.push(other.clone());

        // `retire_child` drops `old`, then `spawn_child`'s state_mut
        // pushes the replacement ref.
        let target_agent_id = old.agent_id;
        wf.child_handles.retain(|h| h.agent_id != target_agent_id);
        wf.child_handles.push(replacement.clone());
        assert_eq!(wf.child_handles, vec![other.clone(), replacement.clone()]);

        let c = wf.encode_carryover();
        let json = serde_json::to_string(&c).expect("serialize carryover");
        let c2: Carryover = serde_json::from_str(&json).expect("deserialize carryover");
        let mut wf2 = AgentWorkflow::default();
        wf2.hydrate_from_carryover(c2);
        assert_eq!(
            wf2.child_handles,
            vec![other, replacement],
            "replace_child's swap (old removed, replacement added) must survive the CAN boundary",
        );
    }

    /// `retire_child`'s retain step must not drop unrelated children when
    /// the target id doesn't match anything in `child_handles` (e.g. the
    /// LLM emitted a `RetireChild` for a child the parent never spawned, or
    /// the same `RetireChild` arm ran twice on a tick boundary). The set is
    /// unchanged in that case — the signal still goes out (best-effort),
    /// but the workflow-state mutation is a no-op.
    #[test]
    fn retire_child_with_unknown_id_leaves_handles_unchanged() {
        use uuid::Uuid;
        let h1 = AgentRef::new(
            "graphs/g1/agents/c1",
            AgentId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap()),
        );
        let h2 = AgentRef::new(
            "graphs/g1/agents/c2",
            AgentId::new(Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap()),
        );
        let mut wf = AgentWorkflow::default();
        wf.child_handles.push(h1.clone());
        wf.child_handles.push(h2.clone());

        let unknown_agent_id =
            AgentId::new(Uuid::parse_str("dddddddd-eeee-ffff-0000-111111111111").unwrap());
        wf.child_handles.retain(|h| h.agent_id != unknown_agent_id);
        assert_eq!(wf.child_handles, vec![h1, h2]);
    }

    /// The decision-log summary string is what the TUI reader displays per
    /// tick. Pin the shape of each `Decision` arm so a future refactor of
    /// the formatter can't silently drop one.
    #[test]
    fn decision_summary_covers_every_decision_arm() {
        use coral_node::decision::{ClaimSeed, FsOp};

        let s = decision_summary(&Decision::Idle {
            next_after: Duration::from_millis(250),
        });
        assert!(s.starts_with("Idle"), "got: {s}");
        assert!(s.contains("250"), "got: {s}");

        let s = decision_summary(&Decision::CallTools {
            calls: vec![
                coral_node::decision::ToolCall::new(
                    "echo",
                    serde_json::json!({}),
                    ClaimSeed::new("a"),
                ),
                coral_node::decision::ToolCall::new(
                    "echo",
                    serde_json::json!({}),
                    ClaimSeed::new("b"),
                ),
            ],
        });
        assert!(s.contains("CallTools"), "got: {s}");
        assert!(s.contains("count: 2"), "got: {s}");

        let s = decision_summary(&Decision::WriteOutput {
            body: "claim".into(),
            citations: vec!["evidence/claim-0123456789abcdef.json".to_string()],
        });
        assert!(s.contains("WriteOutput"), "got: {s}");
        assert!(s.contains("citations: 1"), "got: {s}");

        let s = decision_summary(&Decision::RewriteFs {
            ops: vec![FsOp::WriteFile {
                path: "notes/x.md".into(),
                content: "hi".into(),
            }],
        });
        assert!(s.contains("RewriteFs"), "got: {s}");
        assert!(s.contains("ops: 1"), "got: {s}");

        let s = decision_summary(&Decision::Read {
            path: "notes/x.md".into(),
        });
        assert!(s.contains("Read") && s.contains("notes/x.md"), "got: {s}");
        let s = decision_summary(&Decision::List {
            path: "notes/".into(),
        });
        assert!(s.contains("List"), "got: {s}");
        let s = decision_summary(&Decision::Search {
            query: "tsmc".into(),
            path: None,
        });
        assert!(s.contains("Search") && s.contains("tsmc"), "got: {s}");
    }

    #[test]
    fn format_correction_batch_failure_enumerates_each_call() {
        let s = format_correction(&[
            ToolCallFailure {
                tool: "a".into(),
                args: serde_json::json!({}),
                error: "x".into(),
            },
            ToolCallFailure {
                tool: "b".into(),
                args: serde_json::json!({}),
                error: "y".into(),
            },
        ]);
        assert!(s.contains("2 parallel"), "got: {s}");
        assert!(s.contains("\"a\""), "got: {s}");
        assert!(s.contains("\"b\""), "got: {s}");
    }

    #[test]
    fn step_cap_retire_reason_fires_at_or_past_the_cap() {
        // Boundary: tick == cap retires (the loop performs `cap` ticks then
        // stops on the would-be tick `cap`).
        assert_eq!(
            step_cap_retire_reason(3, Some(3)).as_deref(),
            Some("step_cap (3) reached"),
        );
        assert_eq!(
            step_cap_retire_reason(4, Some(3)).as_deref(),
            Some("step_cap (3) reached"),
        );
        // Under cap: keep running.
        assert_eq!(step_cap_retire_reason(2, Some(3)), None);
        assert_eq!(step_cap_retire_reason(0, Some(1)), None);
        // `None` falls back to the interim default rather than meaning "no
        // cap": below the default it keeps running, at it it retires.
        assert_eq!(step_cap_retire_reason(INTERIM_STEP_CAP - 1, None), None);
        assert_eq!(
            step_cap_retire_reason(INTERIM_STEP_CAP, None).as_deref(),
            Some(format!("step_cap ({INTERIM_STEP_CAP}) reached").as_str()),
        );
    }

    fn empty_drained() -> DrainedBuckets {
        DrainedBuckets {
            triggers: vec![],
            human_ops: vec![],
            mandate_patches: vec![],
            retirement: None,
        }
    }

    #[test]
    fn synthesize_scheduled_wake_injects_only_on_an_empty_idle_tick() {
        // Pure idle wake: nothing was queued → one synthesized ScheduledWake
        // lands in the trigger list that `assemble` forwards verbatim.
        let mut idle = empty_drained();
        synthesize_scheduled_wake(&mut idle);
        assert_eq!(idle.triggers, vec![Trigger::ScheduledWake]);
    }

    #[test]
    fn synthesize_scheduled_wake_is_a_noop_when_any_work_was_drained() {
        // A real trigger already present: don't add a spurious wake.
        let mut with_trigger = DrainedBuckets {
            triggers: vec![Trigger::External {
                kind: "webhook".into(),
                payload: serde_json::json!({}),
            }],
            ..empty_drained()
        };
        synthesize_scheduled_wake(&mut with_trigger);
        assert_eq!(with_trigger.triggers.len(), 1);
        assert!(!with_trigger
            .triggers
            .iter()
            .any(|t| matches!(t, Trigger::ScheduledWake)));

        // Human op pending → the tick has work; no wake.
        let mut with_human_op = DrainedBuckets {
            human_ops: vec![HumanOp::new(serde_json::json!({"action": "pause"}))],
            ..empty_drained()
        };
        synthesize_scheduled_wake(&mut with_human_op);
        assert!(with_human_op.triggers.is_empty());

        // Mandate patch pending → work; no wake.
        let mut with_patch = DrainedBuckets {
            mandate_patches: vec![MandatePatch::new(serde_json::json!({"model": "x"}))],
            ..empty_drained()
        };
        synthesize_scheduled_wake(&mut with_patch);
        assert!(with_patch.triggers.is_empty());
    }
}
