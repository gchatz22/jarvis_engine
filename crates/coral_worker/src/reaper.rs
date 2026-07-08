//! Quiescence-GC reaper: a background sweep that retires provably-dead graphs.
//!
//! An agent that finishes its cycle parks awaiting a signal and never
//! self-completes. That is correct for continuous monitors, but a `never`
//! cadence, single-invocation graph whose triggers are all consumed reaches a
//! fixpoint from which no further signal can ever originate — yet its workflows
//! stay alive forever. This reaper detects that fixpoint and retires the graph.
//!
//! It runs inside the worker (not as a Temporal workflow), so it may use the
//! wall clock freely and — being a live worker itself — satisfies the
//! "a live worker must be polling the graph" guard for every graph on the queue
//! this worker serves. The decision is delegated to the pure
//! [`coral_temporal::quiescence::GraphQuiescence`] debounce; this module is the
//! Temporal/DB shell around it. See `scratch/graph_quiescence_gc.md`.
//!
//! Scope of this first cut: only `never` graphs are ever retired (the predicate
//! requires it). A cadence or re-propagation graph is polled but never GC'd.
//! A graph with some agents already gone (partial termination) is skipped until
//! every agent is queryable again.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use coral_graph::GraphStore;
use coral_temporal::quiescence::{AgentObservation, GraphQuiescence, Verdict};
use coral_temporal::workflow::{agent_workflow_id, AgentWorkflow};
use temporalio_client::{
    Client, WorkflowDescribeOptions, WorkflowQueryOptions, WorkflowSignalOptions,
};
use temporalio_common::protos::temporal::api::enums::v1::WorkflowExecutionStatus;
use tracing::{debug, info, warn};

/// The reason recorded on every agent the reaper retires. Public so a test can
/// assert a workflow stopped *because of the reaper* and not some other path.
pub const RETIRE_REASON: &str =
    "quiescence GC: graph reached a fixpoint with no possible future signal";

/// Reaper tuning. Defaults are production-safe; both are overridable via env so
/// a live test can converge fast.
#[derive(Clone, Copy, Debug)]
pub struct ReaperConfig {
    /// How often to sweep every graph.
    pub interval: Duration,
    /// How long a graph must hold quiescent before it is retired. Must exceed a
    /// full root-ward propagation wave so an in-flight signal has time to bump a
    /// counter and reset the clock.
    pub wave_margin: Duration,
}

impl ReaperConfig {
    pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(300);
    pub const DEFAULT_WAVE_MARGIN: Duration = Duration::from_secs(1800);

    /// Read `CORAL_GC_INTERVAL_SECS` / `CORAL_GC_WAVE_MARGIN_SECS`, falling back
    /// to the production defaults.
    pub fn from_env() -> Self {
        Self {
            interval: env_secs("CORAL_GC_INTERVAL_SECS").unwrap_or(Self::DEFAULT_INTERVAL),
            wave_margin: env_secs("CORAL_GC_WAVE_MARGIN_SECS").unwrap_or(Self::DEFAULT_WAVE_MARGIN),
        }
    }
}

fn env_secs(key: &str) -> Option<Duration> {
    std::env::var(key)
        .ok()?
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

/// Run the reaper loop until the process exits. Spawned as a background task by
/// the worker daemon.
pub async fn run_reaper(client: Client, store: Arc<GraphStore>, config: ReaperConfig) {
    info!(
        interval_secs = config.interval.as_secs(),
        wave_margin_secs = config.wave_margin.as_secs(),
        "quiescence-GC reaper started"
    );
    let started = Instant::now();
    let mut trackers: HashMap<String, GraphQuiescence> = HashMap::new();
    let mut retired: HashSet<String> = HashSet::new();
    loop {
        tokio::time::sleep(config.interval).await;
        let now = started.elapsed();
        if let Err(e) = sweep(&client, &store, &config, now, &mut trackers, &mut retired).await {
            warn!(error = %e, "reaper sweep errored; retrying next interval");
        }
    }
}

async fn sweep(
    client: &Client,
    store: &GraphStore,
    config: &ReaperConfig,
    now: Duration,
    trackers: &mut HashMap<String, GraphQuiescence>,
    retired: &mut HashSet<String>,
) -> anyhow::Result<()> {
    let graph_ids = store.list_graphs().await?;
    let live: HashSet<String> = graph_ids.iter().map(|g| g.to_string()).collect();
    trackers.retain(|k, _| live.contains(k));

    for graph_id in graph_ids {
        let gid = graph_id.to_string();
        if retired.contains(&gid) {
            continue;
        }
        let agents = match store.list_agents_in_graph(graph_id).await {
            Ok(a) => a,
            Err(e) => {
                warn!(graph_id = %gid, error = %e, "reaper: list agents failed");
                trackers.remove(&gid);
                continue;
            }
        };
        match observe_graph(client, &gid, &agents).await {
            Some(obs) => {
                let tracker = trackers
                    .entry(gid.clone())
                    .or_insert_with(|| GraphQuiescence::new(config.wave_margin));
                if tracker.observe(now, &obs) == Verdict::Retire {
                    trackers.remove(&gid);
                    // Only tombstone the graph if every retire signal landed; a
                    // transient failure would otherwise mark a partially-retired
                    // graph "done". Left un-tombstoned, a later sweep re-assesses
                    // the survivors (closed agents drop out of `observe_graph`)
                    // and re-signals them.
                    if retire_graph(client, &gid, &obs).await {
                        retired.insert(gid);
                    }
                }
            }
            None => {
                // No live agent, or a live agent could not be read (mid-cycle,
                // query-rejected, timed out): can't assess — reset and retry.
                trackers.remove(&gid);
            }
        }
    }
    Ok(())
}

/// A single agent's `snapshot` query must not stall the whole sweep. A query to
/// a workflow whose task queue has no live worker (a graph from another
/// deployment, or a dead queue) can block until the server-side query timeout;
/// bound it so one unresponsive workflow only costs this much per sweep.
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Observe the graph's still-live agents. A `describe` (cheap) classifies each:
///
/// - **Running** → `query` its `snapshot` and include it.
/// - **Closed** (already retired, terminated, …) → treat as *absent*, not as a
///   skip signal. A closed workflow can neither send nor receive a signal, so
///   the live agents' quiescence is unaffected by it. This is what lets a graph
///   whose retirement partially failed self-heal: the next sweep re-assesses the
///   surviving parked agents and retires them. It also avoids issuing a `query`
///   to a closed workflow, which can block on a task queue with no live worker.
/// - **`describe`/`query` errored or timed out** → we cannot assess this agent,
///   so bail the whole graph (`None`) and retry next sweep (false-negative bias).
///
/// Returns `None` if no agent is live (nothing to retire) or any live agent
/// could not be read.
async fn observe_graph(
    client: &Client,
    gid: &str,
    agents: &[coral_graph::AgentRecord],
) -> Option<Vec<AgentObservation>> {
    let mut out = Vec::with_capacity(agents.len());
    for a in agents {
        let agent_id = a.id.to_string();
        let workflow_id = agent_workflow_id(gid, &agent_id);
        let handle = client.get_workflow_handle::<AgentWorkflow>(&workflow_id);

        let describe = handle.describe(WorkflowDescribeOptions::default());
        match tokio::time::timeout(QUERY_TIMEOUT, describe).await {
            Ok(Ok(desc)) if desc.status() == WorkflowExecutionStatus::Running => {}
            Ok(Ok(_)) => continue,
            Ok(Err(e)) => {
                debug!(workflow_id = %workflow_id, error = %e, "reaper: describe failed");
                return None;
            }
            Err(_) => {
                debug!(workflow_id = %workflow_id, "reaper: describe timed out");
                return None;
            }
        }

        let query = handle.query(AgentWorkflow::snapshot, (), WorkflowQueryOptions::default());
        match tokio::time::timeout(QUERY_TIMEOUT, query).await {
            Ok(Ok(snapshot)) => out.push(AgentObservation { agent_id, snapshot }),
            Ok(Err(e)) => {
                debug!(workflow_id = %workflow_id, error = %e, "reaper: snapshot query failed");
                return None;
            }
            Err(_) => {
                debug!(workflow_id = %workflow_id, "reaper: snapshot query timed out");
                return None;
            }
        }
    }
    if out.is_empty() {
        return None;
    }
    Some(out)
}

/// Signal `retire` to every (still-live) agent. Returns `true` only if all
/// signals landed; on any failure the caller leaves the graph un-tombstoned so a
/// later sweep re-assesses the survivors and re-signals them.
async fn retire_graph(client: &Client, gid: &str, agents: &[AgentObservation]) -> bool {
    info!(
        graph_id = %gid,
        agents = agents.len(),
        "quiescence GC: graph reached a fixpoint; retiring all agents"
    );
    let mut all_ok = true;
    for a in agents {
        let workflow_id = agent_workflow_id(gid, &a.agent_id);
        let handle = client.get_workflow_handle::<AgentWorkflow>(&workflow_id);
        if let Err(e) = handle
            .signal(
                AgentWorkflow::retire,
                RETIRE_REASON.to_string(),
                WorkflowSignalOptions::default(),
            )
            .await
        {
            warn!(workflow_id = %workflow_id, error = %e, "reaper: retire signal failed");
            all_ok = false;
        }
    }
    all_ok
}
