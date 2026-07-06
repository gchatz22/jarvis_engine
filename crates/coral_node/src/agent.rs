//! `Agent` — the run loop that wires FS, triggers, decide, tools, and health.
//!
//! Two nested loops. The **outer** loop is the wake boundary: each iteration
//! is one *cycle* (`tick` counts cycles). The **inner** loop is the unit of
//! work — a multi-step ReAct session the model drives until it chooses to
//! idle.
//!
//! One outer iteration:
//!
//! 1. **Wake.** Race the trigger queue against the scheduler's idle
//!    deadline. The race is `biased;` — when both arms are ready (the prior
//!    cycle took longer than `idle_period` and a real trigger is also
//!    buffered), the queue arm wins, so `ScheduledWake` is pushed only when
//!    the queue is genuinely empty. A `never`-cadence agent self-wakes only
//!    its first cycle, then blocks on triggers alone.
//! 2. **Begin the cycle.** Drain the queue, open a fresh per-cycle retry
//!    budget ([`HealthTracker::begin_tick`]), and build a thin orienting
//!    [`Session`] from a [`crate::decision::Seed`] — the mandate, the
//!    drained triggers, and a pointers-only FS index. File contents are
//!    pulled on demand inside the loop, not pushed here.
//! 3. **Inner loop.** Ask `Decide::decide(&session)` for the next step:
//!    * `Idle` is the sole terminal — set the next cadence, mark the cycle a
//!      success ([`HealthTracker::mark_tick_success`], which archives any
//!      prior Unhealthy incident on recovery), and end the cycle.
//!    * any repertoire step runs via [`agent_core::execute_step`]; its
//!      observation (success *or* failure) is appended to the session so the
//!      model adapts on the next step. A recoverable failure is also
//!      accounted against the per-cycle budget; on exhaustion the tracker
//!      transitions to `Unhealthy` and the cycle ends. There is no
//!      cross-cycle correction state — the failure lives in the session.
//!    * [`agent_core::CYCLE_RUNAWAY_FUSE`] force-idles a cycle whose model
//!      never converges (logged loudly; it never bites real work).
//!
//! Decide-side `Err` (e.g. inference parse retries exhausted in `LlmDecide`)
//! ends the cycle by transitioning to `Unhealthy` directly, without
//! consulting the budget — the `LlmDecide` impl already did its one allowed
//! retry internally, so spending another slot would double-count.
//!
//! **The agent never self-terminates.** No step ends the *outer* loop;
//! persistence is universal. `Unhealthy` transitions are not exit
//! conditions — the agent stays subscribed to its queue and a later
//! successful cycle recovers it to `Healthy`. The outer loop exits only on
//! the interim `step_cap` runaway backstop (counted in cycles, checked at
//! the top of each iteration) or a non-recoverable error.

use anyhow::Result;
use chrono::Utc;
use serde_json::json;
use tokio::time::sleep_until;
use tracing::{debug, info, info_span, warn, Instrument};

use crate::agent_core::{self, StepFailure};
use crate::decision::{Decide, Session};
use crate::fs::AgentFs;
use crate::health::{
    Attempt, FailingCall, FailureKind, HealthError, HealthIncident, HealthTracker,
};
use crate::mandate::Mandate;
use crate::scheduler::{arm_self_wake, Scheduler};
use crate::tools::ToolRegistry;
use crate::trigger::Trigger;
use crate::trigger_queue::{SignalSink, TriggerQueue};

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use std::sync::{Arc, Mutex};

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use crate::decide_llm::llm_decide::{LlmDecide, TickTotals};
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use crate::model_client::CallStats;

/// Reason an agent retired, surfaced from `Agent::run`. Newtype around the
/// raw string so callers can distinguish a clean retirement from any other
/// `String` they might be holding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireReason(pub String);

impl RetireReason {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The single-node agent. Owns the FS, the trigger queue, the model
/// adapter, the tool registry, the scheduler, and the health tracker.
pub struct Agent<D: Decide> {
    cfg: Mandate,
    fs: AgentFs,
    triggers: TriggerQueue,
    decide: D,
    tools: ToolRegistry,
    scheduler: Scheduler,
    sink: SignalSink,
    health: HealthTracker,
}

impl<D: Decide> Agent<D> {
    /// Wire an agent. The scheduler is seeded with `cfg.idle_period` so the
    /// first deadline arrives at the configured cadence. A fresh
    /// `TriggerQueue` is constructed; its `SignalSink` is retained so
    /// `signal()` can be called before `run()` consumes the agent.
    ///
    /// `health` is constructed by the caller (typically rooted at the same
    /// directory as `fs`) and passed in. The same construction-injection
    /// pattern as `decide`/`tools` keeps the wiring boundary clean and lets
    /// tests drive a tracker with deterministic timestamps and budgets.
    pub fn new(
        cfg: Mandate,
        fs: AgentFs,
        decide: D,
        tools: ToolRegistry,
        health: HealthTracker,
    ) -> Self {
        // A `never`-cadence mandate carries no recurring interval; the
        // scheduler is consulted only to bootstrap the first cycle, so seed
        // it with a fire-now deadline. After that first cycle the wake gate
        // stops arming the timer for `never` nodes.
        let scheduler = Scheduler::new(cfg.idle_period.unwrap_or(std::time::Duration::ZERO));
        let (triggers, sink) = TriggerQueue::new();
        Self {
            cfg,
            fs,
            triggers,
            decide,
            tools,
            scheduler,
            sink,
            health,
        }
    }

    /// Mint a clonable `SignalSink` an external producer can use to push
    /// triggers onto the queue. Safe to call before `run()` — the sink is
    /// stored on the agent at construction.
    pub fn signal(&self) -> SignalSink {
        self.sink.clone()
    }

    /// Run the loop until one of:
    /// - the interim `step_cap` runaway backstop is hit (writes
    ///   `retirement.json`, with a synthesized `step_cap (N) reached`
    ///   reason, and returns it);
    /// - a non-recoverable error bubbles via `?`.
    ///
    /// The agent never self-terminates: persistence is universal, so no
    /// `Decision` ends the loop. `Unhealthy` transitions and pending
    /// corrections are **not** exit conditions — see the module doc for why.
    pub async fn run(self) -> Result<RetireReason> {
        let Agent {
            cfg,
            fs,
            mut triggers,
            decide,
            tools,
            mut scheduler,
            sink: _sink,
            mut health,
        } = self;

        // `never` cadence (`idle_period == None`): self-wake only the first
        // cycle, then wait on triggers alone (the wake gate stops arming the
        // idle timer). `step_cap` is the interim runaway backstop, in cycles.
        let never = cfg.is_never();
        let step_cap = cfg.step_cap.unwrap_or(crate::mandate::INTERIM_STEP_CAP);

        let mut tick: u64 = 0;
        loop {
            // The interim `step_cap` runaway backstop, counted in cycles.
            // Checked before incrementing so the cap is the count of cycles
            // actually performed.
            if tick >= step_cap {
                let reason = format!("step_cap ({}) reached", step_cap);
                fs.persist_retirement(&reason, Utc::now()).await?;
                return Ok(RetireReason(reason));
            }
            tick += 1;
            let span = info_span!("agent.cycle", tick);
            async {
                // Wake boundary: race the trigger queue against the deadline.
                if arm_self_wake(never, tick == 1) {
                    // `biased;` makes tokio poll arms in declaration order
                    // rather than randomly. The queue arm is listed first so
                    // that when both are ready (the prior cycle took longer
                    // than `idle_period`, leaving an elapsed deadline
                    // alongside buffered triggers), the queue wins and we
                    // don't push a spurious `ScheduledWake` onto a queue that
                    // already has work. `ScheduledWake` should mean "the idle
                    // period elapsed without other work" — a stronger
                    // semantic than tokio's default tie-break gives us.
                    tokio::select! {
                        biased;
                        _ = triggers.wait_nonempty() => {}
                        _ = sleep_until(scheduler.next_deadline()) => {
                            triggers.push(Trigger::ScheduledWake);
                        }
                    }
                } else {
                    // `never` cadence past the first cycle: no self-wake
                    // timer, so block until a trigger arrives.
                    triggers.wait_nonempty().await;
                }

                let drained = triggers.drain_ordered();
                debug!(count = drained.len(), "drained triggers");

                // One cycle: open a fresh per-cycle retry budget, build the
                // thin orienting session, then run the inner ReAct loop over
                // the accumulating session until the model chooses `Idle`.
                health.begin_tick();
                let mut retry_trail: Vec<Attempt> = Vec::new();
                let seed = agent_core::build_seed(&fs, drained, &cfg).await?;
                let mut session = Session::new(seed);

                loop {
                    let action = match agent_core::decide(&session, &decide).await {
                        Ok(a) => a,
                        Err(e) => {
                            // Decide-side Err: LlmDecide already did its
                            // one-shot internal retry (or this is `MockDecide`
                            // returning a script error). Treat as direct
                            // inference-retry exhaustion — straight to
                            // Unhealthy without spending a budget slot. The
                            // cycle ends; the outer loop continues.
                            warn!(error = %e, "decide returned Err; transitioning to Unhealthy");
                            retry_trail.push(Attempt {
                                attempt: (retry_trail.len() as u32) + 1,
                                at: Utc::now(),
                                error: format!("{e:#}"),
                            });
                            let incident = HealthIncident {
                                failing: FailingCall {
                                    kind: FailureKind::Inference,
                                    details: json!({
                                        "stage": "decide",
                                        "error": format!("{e:#}"),
                                    }),
                                },
                                retry_trail: retry_trail.clone(),
                                last_error: format!("{e:#}"),
                                transitioned_at: Utc::now(),
                            };
                            health.transition_to_unhealthy(incident)?;
                            return Ok::<(), anyhow::Error>(());
                        }
                    };

                    if let Some(next_after) = action.idle_after() {
                        // `Idle` is the sole terminal step: set the next wake
                        // cadence, mark the cycle a success (archiving any
                        // prior Unhealthy incident on recovery), end the cycle.
                        debug!(
                            next_after_ms = next_after.as_millis() as u64,
                            "cycle: idle (terminal)"
                        );
                        info!(
                            steps = session.len(),
                            status_note_written = agent_core::status_note_written(&session),
                            "cycle complete: status-note telemetry"
                        );
                        scheduler.set_next_after(next_after);
                        health.mark_tick_success(Utc::now())?;
                        return Ok(());
                    }

                    // Repertoire step: run it, account any recoverable failure
                    // against the per-cycle budget, and append the observation
                    // (success *or* failure) so the model adapts next step.
                    let mut outcome = agent_core::execute_step(&fs, &tools, &action).await?;
                    let maybe_incident = match &outcome.failure {
                        None => None,
                        Some(failure) => {
                            record_cycle_failure(&mut health, &mut retry_trail, failure)?
                        }
                    };
                    if let Some(nudge) = agent_core::wander_nudge(&session, &action) {
                        outcome.observation.content.push_str(&nudge);
                    }
                    session.push(action, outcome.observation);
                    if let Some(incident) = maybe_incident {
                        warn!("per-cycle retry budget exhausted; transitioning to Unhealthy");
                        health.transition_to_unhealthy(incident)?;
                        return Ok(());
                    }

                    if session.len() >= agent_core::CYCLE_RUNAWAY_FUSE {
                        warn!(
                            steps = session.len(),
                            "cycle hit runaway fuse; forcing idle — this mandate never converges, decompose it"
                        );
                        return Ok(());
                    }
                }
            }
            .instrument(span)
            .await?;
        }
    }
}

/// Account one repertoire-step failure against the per-cycle retry budget,
/// extending `retry_trail`. Returns `Some(incident)` when the budget is now
/// exhausted — the caller transitions the tracker to Unhealthy and ends the
/// cycle — or `None` when there is still room to keep stepping.
///
/// A `ToolError` carries K per-call failures; each counts as one
/// `FailureKind::ToolCall` slot. Once the budget exhausts mid-batch the
/// remaining failures still join the retry trail (so the `HealthIncident`
/// archive captures the full batch) but stop spending slots.
fn record_cycle_failure(
    health: &mut HealthTracker,
    retry_trail: &mut Vec<Attempt>,
    failure: &StepFailure,
) -> Result<Option<HealthIncident>> {
    match failure {
        StepFailure::NeedsCorrection(desc) => {
            retry_trail.push(Attempt {
                attempt: (retry_trail.len() as u32) + 1,
                at: Utc::now(),
                error: desc.clone(),
            });
            match health.record_failure(FailureKind::Inference, desc) {
                Ok(()) => Ok(None),
                Err(HealthError::BudgetExhausted { kind }) => Ok(Some(HealthIncident {
                    failing: FailingCall {
                        kind,
                        details: json!({ "stage": "apply", "error": desc }),
                    },
                    retry_trail: retry_trail.clone(),
                    last_error: desc.clone(),
                    transitioned_at: Utc::now(),
                })),
                Err(other) => Err(other.into()),
            }
        }
        StepFailure::ToolError(failures) => {
            let mut budget_exhausted: Option<FailureKind> = None;
            let mut last_error = String::new();
            for f in failures {
                retry_trail.push(Attempt {
                    attempt: (retry_trail.len() as u32) + 1,
                    at: Utc::now(),
                    error: f.error.clone(),
                });
                last_error = f.error.clone();
                if budget_exhausted.is_some() {
                    continue;
                }
                match health.record_failure(FailureKind::ToolCall, &f.error) {
                    Ok(()) => {}
                    Err(HealthError::BudgetExhausted { kind }) => budget_exhausted = Some(kind),
                    Err(other) => return Err(other.into()),
                }
            }
            match budget_exhausted {
                None => Ok(None),
                Some(kind) => {
                    let failures_json: Vec<serde_json::Value> = failures
                        .iter()
                        .map(|f| json!({ "tool": f.tool, "error": f.error }))
                        .collect();
                    let first_tool = failures.first().map(|f| f.tool.clone()).unwrap_or_default();
                    Ok(Some(HealthIncident {
                        failing: FailingCall {
                            kind,
                            details: json!({
                                "stage": "apply",
                                "tool": first_tool,
                                "error": last_error.clone(),
                                "failures": failures_json,
                            }),
                        },
                        retry_trail: retry_trail.clone(),
                        last_error,
                        transitioned_at: Utc::now(),
                    }))
                }
            }
        }
    }
}

// ---------- Post-run / between-tick CallStats accessor ----------
//
// The accessor and its supporting `StatsHandle` are scoped to
// `Agent<LlmDecide>` — non-LLM `Decide` impls don't need a stats
// surface, and the `Decide` trait deliberately doesn't carry a stats
// method. Feature-gated on the `llm-*` features for the same reason
// `LlmDecide` itself is.

/// Cheap, clonable handle onto an `LlmDecide`'s per-tick `CallStats`
/// accumulator. Surfaces the same data as `LlmDecide::last_tick_calls` /
/// `last_tick_totals` but survives `Agent::run` consuming the `LlmDecide`,
/// so callers can read the most recent tick's stats after the run loop
/// retires.
///
/// **Update timing.** The underlying accumulator is reset at the start of
/// every `LlmDecide::decide` call and pushed once per upstream
/// `ModelClient::complete` call. A read in between two `decide`
/// invocations reflects the *previous* tick — there is no per-tick
/// history. Before the first `decide` runs the handle reports zero
/// calls and `TickTotals::default()`.
///
/// **Concurrency.** Internally an `Arc<Mutex<Vec<CallStats>>>`. The lock
/// is held only for the duration of the read; no `await` is performed
/// while holding it. Cloning the handle is `Arc::clone`-cheap.
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
#[derive(Clone)]
pub struct StatsHandle {
    inner: Arc<Mutex<Vec<CallStats>>>,
}

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
impl StatsHandle {
    /// Per-call stats for the most recent `decide()` invocation, in call
    /// order. Returns an owned `Vec` (clone of the inner storage) so the
    /// caller never has to hold the lock.
    pub fn last_tick_calls(&self) -> Vec<CallStats> {
        self.inner.lock().expect("stats mutex poisoned").clone()
    }

    /// Aggregate totals for the most recent `decide()` invocation.
    /// Returns `TickTotals::default()` before the first `decide` runs.
    pub fn last_tick_totals(&self) -> TickTotals {
        let stats = self.inner.lock().expect("stats mutex poisoned");
        TickTotals::from_calls(&stats)
    }
}

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
impl Agent<LlmDecide> {
    /// Capture a `StatsHandle` onto the inner `LlmDecide`'s per-tick
    /// `CallStats` accumulator. Cheap (one `Arc::clone`). The handle
    /// outlives the agent — `Agent::run` will consume `self` and drop the
    /// `LlmDecide`, but the `Arc<Mutex<...>>` storage remains reachable
    /// through any `StatsHandle` cloned out beforehand.
    ///
    /// Typical use in a test:
    /// ```ignore
    /// let agent = Agent::new(mandate, fs, decide, registry, health);
    /// let stats = agent.stats_handle();
    /// let RetireReason(_) = agent.run().await?;
    /// let calls = stats.last_tick_calls(); // last tick's CallStats
    /// ```
    pub fn stats_handle(&self) -> StatsHandle {
        StatsHandle {
            inner: self.decide.stats_handle(),
        }
    }

    /// Convenience: per-call stats for the most recent tick, via the
    /// inner `LlmDecide`. Useful between ticks from a borrow on the
    /// agent. Post-run callers must use `stats_handle()` to capture a
    /// handle before `.run()` consumes the agent.
    pub fn last_tick_calls(&self) -> Vec<CallStats> {
        self.decide.last_tick_calls()
    }

    /// Convenience: aggregate totals for the most recent tick. Same
    /// timing semantics as `StatsHandle::last_tick_totals`.
    pub fn last_tick_totals(&self) -> TickTotals {
        self.decide.last_tick_totals()
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for `Agent::run`-scoped surfaces. End-to-end coverage
    //! of the exhaustion → correction → recovery flow lives in
    //! `tests/loop_smoke.rs`.

    // ---------- Stats accessor unit tests ----------

    #[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
    mod stats_handle_tests {
        //! Unit tests for the `StatsHandle` / `Agent<LlmDecide>`
        //! accessors. End-to-end coverage through `Agent::run` lives in
        //! the LLM fixture suites
        //! (`tests/llm_fixture_anthropic.rs::unhealthy_then_recovery_cycle_via_agent_run`
        //! and the Cohere mirror).
        use crate::agent::*;
        use crate::fs::AgentFs;
        use crate::health::HealthTracker;
        use crate::mandate::Mandate;
        use crate::model_client::{
            CallStats, CompleteOptions, CompleteRequest, CompleteResponse, ContentBlock,
            ModelClient, ModelError, ToolCall, Usage, Vendor,
        };
        use crate::tools::ToolRegistry;
        use async_trait::async_trait;
        use chrono::Utc;
        use serde_json::json;
        use std::sync::Arc;
        use std::sync::Mutex as StdMutex;

        /// Minimal scripted `ModelClient`: pops the next `CompleteResponse`
        /// from a queue on each `complete` call. Mirrors the pattern in
        /// `decide_llm::llm_decide::tests::MockModelClient` but lives here
        /// so the agent-side accessor tests don't reach into another
        /// module's private test surface.
        struct ScriptedClient {
            script: StdMutex<Vec<CompleteResponse>>,
        }

        #[async_trait]
        impl ModelClient for ScriptedClient {
            async fn complete(
                &self,
                _req: CompleteRequest,
            ) -> Result<CompleteResponse, ModelError> {
                let next = self
                    .script
                    .lock()
                    .unwrap()
                    .drain(..1)
                    .next()
                    .expect("ScriptedClient: script exhausted");
                Ok(next)
            }
        }

        fn resp(stats: CallStats, tool_call: ToolCall) -> CompleteResponse {
            CompleteResponse {
                content: vec![ContentBlock::ToolUse {
                    id: tool_call.id.clone(),
                    name: tool_call.name.clone(),
                    input: tool_call.arguments.clone(),
                }],
                tool_calls: vec![tool_call],
                usage: stats.usage,
                stats,
            }
        }

        fn idle_call() -> ToolCall {
            ToolCall {
                id: "toolu_idle".into(),
                name: "idle".into(),
                arguments: json!({"next_after": 1000}),
            }
        }

        fn stats(input: u32, output: u32, latency_ms: u64) -> CallStats {
            CallStats {
                usage: Usage {
                    input_tokens: input,
                    output_tokens: output,
                },
                latency_ms,
                vendor: Vendor::Anthropic,
                model: "test-model".into(),
            }
        }

        fn empty_session() -> crate::decision::Session {
            crate::decision::Session::new(crate::decision::Seed::new(
                Mandate::new("stats-test", std::time::Duration::from_secs(1), Some(1)),
                vec![],
                crate::decision::FsIndex::default(),
            ))
        }

        #[tokio::test]
        async fn stats_handle_zero_before_first_decide() {
            // A fresh `Agent<LlmDecide>` has run no ticks. The handle must
            // report no calls and a zero `TickTotals`.
            let client: Arc<dyn ModelClient> = Arc::new(ScriptedClient {
                script: StdMutex::new(vec![]),
            });
            let decide = LlmDecide::new(client, CompleteOptions::default());
            let tmp = tempfile::TempDir::new().unwrap();
            let mandate = Mandate::new("stats-test", std::time::Duration::from_millis(10), Some(1));
            let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
                .await
                .unwrap();
            let registry = ToolRegistry::new();
            let health = HealthTracker::open(
                tmp.path(),
                crate::health::RetryBudget::default(),
                Utc::now(),
            )
            .unwrap();
            let agent = Agent::new(mandate, fs, decide, registry, health);

            let handle = agent.stats_handle();
            assert!(handle.last_tick_calls().is_empty());
            assert_eq!(handle.last_tick_totals(), TickTotals::default());
            // Pre-run convenience accessor should agree.
            assert!(agent.last_tick_calls().is_empty());
            assert_eq!(agent.last_tick_totals(), TickTotals::default());
        }

        #[tokio::test]
        async fn stats_handle_survives_after_run_consumes_agent() {
            // Core promise: capture the handle pre-run, run the agent to
            // retirement (consuming `self`), then read the most recent
            // tick's stats off the handle.
            let s = stats(11, 7, 42);
            let client: Arc<dyn ModelClient> = Arc::new(ScriptedClient {
                script: StdMutex::new(vec![
                    // Tick 1: idle → loop continues.
                    resp(s.clone(), idle_call()),
                    // Tick 2: idle again; `step_cap` stops the loop after it.
                    resp(stats(3, 2, 5), idle_call()),
                ]),
            });
            let decide = LlmDecide::new(client, CompleteOptions::default());
            let tmp = tempfile::TempDir::new().unwrap();
            // Small idle_period + a 2-tick cap so the loop runs both scripted
            // ticks then retires on the `step_cap` backstop (the agent no
            // longer self-terminates).
            let mandate = Mandate::new("stats-test", std::time::Duration::from_millis(1), Some(2));
            let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
                .await
                .unwrap();
            let registry = ToolRegistry::new();
            let health = HealthTracker::open(
                tmp.path(),
                crate::health::RetryBudget::default(),
                Utc::now(),
            )
            .unwrap();
            let agent = Agent::new(mandate, fs, decide, registry, health);

            // Capture *before* run consumes the agent — this is the API
            // contract test (c) depends on.
            let stats_handle = agent.stats_handle();

            let RetireReason(reason) =
                tokio::time::timeout(std::time::Duration::from_secs(5), agent.run())
                    .await
                    .expect("agent retired")
                    .expect("run ok");
            assert_eq!(reason, "step_cap (2) reached");

            // Stats handle must reflect the *last* tick (the second idle),
            // not the first tick. LlmDecide resets its accumulator at the
            // start of every `decide`.
            let calls = stats_handle.last_tick_calls();
            assert_eq!(calls.len(), 1, "last tick was a single-call idle decision");
            assert_eq!(calls[0].usage.input_tokens, 3);
            assert_eq!(calls[0].usage.output_tokens, 2);
            assert_eq!(calls[0].latency_ms, 5);

            let totals = stats_handle.last_tick_totals();
            assert_eq!(totals.calls, 1);
            assert_eq!(totals.input_tokens, 3);
            assert_eq!(totals.output_tokens, 2);
            assert_eq!(totals.latency_ms, 5);
        }

        #[tokio::test]
        async fn stats_handle_is_cheap_to_clone() {
            // The handle must be cheap to clone and clones must share
            // storage — otherwise callers can't, say, hand a clone to a
            // tracing layer and read another clone post-run.
            let client: Arc<dyn ModelClient> = Arc::new(ScriptedClient {
                script: StdMutex::new(vec![resp(stats(50, 5, 99), idle_call())]),
            });
            let decide = LlmDecide::new(client, CompleteOptions::default());
            let tmp = tempfile::TempDir::new().unwrap();
            let mandate = Mandate::new("stats-test", std::time::Duration::from_secs(1), Some(1));
            let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
                .await
                .unwrap();
            let registry = ToolRegistry::new();
            let health = HealthTracker::open(
                tmp.path(),
                crate::health::RetryBudget::default(),
                Utc::now(),
            )
            .unwrap();
            let agent = Agent::new(mandate, fs, decide, registry, health);

            let h1 = agent.stats_handle();
            let h2 = h1.clone();
            // Drive one tick directly via the inner `decide` (no run
            // loop) so the test exercises the `decide` reset semantics
            // without depending on agent scheduling.
            agent.decide.decide(&empty_session()).await.unwrap();
            // Both clones see the same accumulator state.
            assert_eq!(h1.last_tick_calls(), h2.last_tick_calls());
            assert_eq!(h1.last_tick_totals(), h2.last_tick_totals());
            assert_eq!(h1.last_tick_totals().calls, 1);
            assert_eq!(h1.last_tick_totals().input_tokens, 50);
        }
    }
}
