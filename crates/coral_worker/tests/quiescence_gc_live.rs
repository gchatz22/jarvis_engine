//! End-to-end quiescence-GC test on the workflow path.
//!
//! Proves the reaper's runtime contract against a real Temporal Server +
//! Postgres: a `never`-cadence star graph converges to a fixpoint (every agent
//! parked, no signal source), and the reaper retires all three — after the
//! parent has produced its synthesis, not before, and without touching the
//! durable record.
//!
//! A deterministic [`QuiescenceDecide`] drives the loop and the children cite
//! planted evidence, so this needs **no model key and no Node** — only the
//! `TEMPORAL_LIVE_TEST=1` + `DATABASE_URL` gates.
//!
//! The assertions attack the three things unit tests cannot reach, because they
//! are properties of the real workflow, not of the tracker's synthetic input:
//!   1. **Linchpin** — a `snapshot` query actually observes `parked=true`
//!      (with `is_never` + `tick>=1`) while the workflow is blocked at the wake
//!      gate. If this were false the predicate would silently never fire.
//!   2. **Negative** — the reaper does NOT retire before its window elapses
//!      (also guards against a too-small `wave_margin`).
//!   3. **Retire-after-output, durable-survives** — the parent's cited
//!      synthesis exists before retirement and still resolves after it.
//!
//! Run it:
//! ```bash
//! TEMPORAL_LIVE_TEST=1 \
//!   DATABASE_URL=postgres://coral:coral@localhost:5432/coral_structural \
//!   cargo test -p coral_worker --test quiescence_gc_live -- --nocapture
//! ```

use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowQueryOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use coral_graph::yaml::{build_workflow_starts, parse_and_validate, yaml_seed_triggers};
use coral_graph::{GraphStore, MIGRATOR};
use coral_node::agent_ref::GraphId;
use coral_node::decision::{Decide, Decision, ReconcileSource, Session};
use coral_node::evidence::EvidenceRecord;
use coral_node::fs::AgentFs;
use coral_node::mandate::Mandate;
use coral_node::storage::{AgentStorage, MemoryStorage};
use coral_node::tools::ToolRegistry;
use coral_node::trigger::Trigger;
use coral_temporal::worker::{
    build_worker, install_agent_storage, install_decide, install_structural_db_store,
    install_tool_registry, StructuralDbStore,
};
use coral_temporal::workflow::{AgentResult, AgentSnapshot, AgentWorkflow};
use coral_worker::reaper::{run_reaper, ReaperConfig, RETIRE_REASON};
use sqlx::postgres::PgPoolOptions;

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Substring identifying the parent's mandate.
const PARENT_MARKER: &str = "coordinate two researchers";

/// Fast sweep + short window so the test converges in seconds. The window is
/// still several sweeps wide so the negative assertion has room.
const GC_INTERVAL: Duration = Duration::from_secs(2);
const GC_WAVE_MARGIN: Duration = Duration::from_secs(8);

/// Evidence id the children cite, planted identically on each child's FS.
static CHILD_EVIDENCE: OnceLock<String> = OnceLock::new();

/// Serializes the single live test against the process-wide installs.
static LIVE_GUARD: Mutex<()> = Mutex::new(());

fn run_suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_else(|_| "no-suffix".into())
}

fn example_graph_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root above crates/coral_worker")
        .join("examples")
        .join("quiescence_gc")
        .join("graph.yaml")
}

/// Deterministic driver: children emit one finding citing the planted evidence
/// then idle; the parent folds each `ChildOutput`, writes a consolidated report
/// citing the reconcile evidence, then idles. Every agent parks after its cycle
/// — none self-terminates — so only the reaper can end the graph.
struct QuiescenceDecide;

fn first_evidence_id(reconcile_observation: &str) -> Option<String> {
    reconcile_observation
        .split_whitespace()
        .map(|t| t.trim_end_matches([',', '.']))
        .find(|t| t.starts_with("evidence/") && t.ends_with(".json"))
        .map(|t| t.to_string())
}

#[async_trait]
impl Decide for QuiescenceDecide {
    async fn decide(&self, session: &Session) -> Result<Decision> {
        let seed = &session.seed;
        let idle = Decision::Idle {
            next_after: seed.mandate.idle_period.unwrap_or_default(),
        };
        if seed.mandate.text.contains(PARENT_MARKER) {
            let last = session.steps.last();
            match last.map(|s| &s.action) {
                Some(Decision::ReconcileChildren { .. }) => {
                    let observation = &last.expect("last is Some in this arm").observation;
                    if let Some(cite) = first_evidence_id(&observation.content) {
                        return Ok(Decision::WriteOutput {
                            body: format!("consolidated report {}", seed.index.outputs.len() + 1),
                            citations: vec![cite],
                        });
                    }
                    return Ok(idle);
                }
                Some(Decision::WriteOutput { .. }) => return Ok(idle),
                _ => {}
            }
            let sources: Vec<ReconcileSource> = seed
                .triggers
                .iter()
                .filter_map(|t| match t {
                    Trigger::ChildOutput {
                        child_ref,
                        output_id,
                        ..
                    } => Some(ReconcileSource {
                        child_ref: child_ref.clone(),
                        output_id: output_id.clone(),
                    }),
                    _ => None,
                })
                .collect();
            if !sources.is_empty() {
                return Ok(Decision::ReconcileChildren {
                    sources,
                    conflict: None,
                });
            }
            Ok(idle)
        } else if session.steps.is_empty() {
            let n = seed.index.outputs.len() + 1;
            let ev = CHILD_EVIDENCE
                .get()
                .expect("CHILD_EVIDENCE planted before worker start")
                .clone();
            Ok(Decision::WriteOutput {
                body: format!("finding {n}"),
                citations: vec![ev],
            })
        } else {
            Ok(idle)
        }
    }
}

async fn build_client() -> Result<Client> {
    let address = env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.into());
    let namespace = env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| DEFAULT_NAMESPACE.into());
    let url = Url::parse(&address).context("parsing TEMPORAL_ADDRESS")?;
    let connection = Connection::connect(ConnectionOptions::new(url).build())
        .await
        .context("connecting to Temporal Server")?;
    Client::new(connection, ClientOptions::new(namespace).build())
        .context("building Temporal client")
}

async fn snapshot(client: &Client, workflow_id: &str) -> Result<AgentSnapshot> {
    client
        .get_workflow_handle::<AgentWorkflow>(workflow_id)
        .query(AgentWorkflow::snapshot, (), WorkflowQueryOptions::default())
        .await
        .with_context(|| format!("snapshot query {workflow_id}"))
}

/// Hermetic: the fixture parses and validates, and is `never` on every node —
/// the precondition the whole feature turns on.
#[test]
fn example_graph_is_never_and_validates() {
    let yaml = std::fs::read_to_string(example_graph_path())
        .expect("read examples/quiescence_gc/graph.yaml");
    let graph = parse_and_validate(&yaml).expect("fixture validates");
    assert_eq!(graph.agents.len(), 1, "single root");
    assert_eq!(graph.agents[0].children.len(), 2, "two researchers");
    // Only the two children are seeded; the parent wakes on ChildOutput.
    assert_eq!(graph.seed.triggers.len(), 2, "two seed kickoffs");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::await_holding_lock)]
async fn quiescent_never_graph_is_reaped_after_producing_output() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping quiescent_never_graph_is_reaped_after_producing_output; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    let Some(database_url) = env::var("DATABASE_URL").ok().filter(|s| !s.is_empty()) else {
        eprintln!(
            "skipping quiescent_never_graph_is_reaped_after_producing_output; \
             set DATABASE_URL to a docker-compose Postgres to run"
        );
        return;
    };
    let _guard = LIVE_GUARD.lock().unwrap_or_else(|p| p.into_inner());
    run_test(&database_url).await.expect("quiescence GC smoke");
}

async fn run_test(database_url: &str) -> Result<()> {
    let suffix = run_suffix();

    let yaml_text = std::fs::read_to_string(example_graph_path()).context("read fixture")?;
    let mut graph_yaml = parse_and_validate(&yaml_text).context("validate fixture")?;
    graph_yaml.metadata.name = format!("quiescence-gc-{suffix}");

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(database_url)
        .await
        .context("connecting to structural DB (DATABASE_URL)")?;
    MIGRATOR.run(&pool).await.context("migrate structural DB")?;
    let store = Arc::new(GraphStore::new(pool));
    let applied = store
        .create_from_yaml(&graph_yaml)
        .await
        .context("create_from_yaml(quiescence gc)")?;
    let graph_id = applied.graph_id;

    let storage = Arc::new(MemoryStorage::new());
    install_agent_storage(storage.clone() as Arc<dyn AgentStorage>);
    install_decide(Arc::new(QuiescenceDecide) as Arc<dyn Decide>);
    install_structural_db_store(store.clone() as Arc<dyn StructuralDbStore>);
    install_tool_registry(Arc::new(ToolRegistry::new()));

    // Plant identical evidence on each child so the same content-addressed id
    // resolves under either prefix.
    let plant_mandate = Mandate::new("plant", Duration::from_millis(0), None);
    let mut planted: Option<String> = None;
    for operator_id in ["researcher-alpha", "researcher-beta"] {
        let agent = applied
            .agents
            .iter()
            .find(|a| a.operator_id == operator_id)
            .ok_or_else(|| anyhow!("missing {operator_id} in applied graph"))?;
        let prefix = format!("graphs/{graph_id}/agents/{}/", agent.db_agent_id);
        let fs = AgentFs::new_with_storage(
            storage.clone() as Arc<dyn AgentStorage>,
            &prefix,
            &plant_mandate,
        )
        .await
        .with_context(|| format!("open child FS for {operator_id}"))?;
        let id = fs
            .record_evidence(
                EvidenceRecord::new(
                    "echo",
                    serde_json::json!({"seed": "quiescence-gc"}),
                    serde_json::json!({"ok": true}),
                    chrono::Utc::now(),
                ),
                "echo",
            )
            .await
            .context("plant child evidence")?;
        match &planted {
            Some(prev) => assert_eq!(prev, &id, "identical evidence ⇒ identical id"),
            None => planted = Some(id),
        }
    }
    CHILD_EVIDENCE
        .set(planted.expect("planted at least one"))
        .map_err(|_| anyhow!("CHILD_EVIDENCE set twice"))?;

    let task_queue = format!("coral-quiescence-gc-{suffix}");
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(TelemetryOptions::builder().build())
            .build()
            .map_err(|e| anyhow!("RuntimeOptions build failed: {e}"))?,
    )?;
    let client = build_client().await?;
    let mut worker = build_worker(&runtime, client.clone(), &task_queue)?;
    let shutdown = worker.shutdown_handle();

    // The reaper under test, sharing the worker's client + store.
    tokio::spawn(run_reaper(
        client.clone(),
        store.clone(),
        ReaperConfig {
            interval: GC_INTERVAL,
            wave_margin: GC_WAVE_MARGIN,
        },
    ));

    let starts = build_workflow_starts(&graph_yaml, &applied);
    let seeds = yaml_seed_triggers(&graph_yaml, &applied).context("resolve seed triggers")?;

    let driver_storage = storage.clone();
    let driver_tq = task_queue.clone();
    let driver = tokio::spawn(async move {
        struct ShutdownGuard<F: Fn()>(F);
        impl<F: Fn()> Drop for ShutdownGuard<F> {
            fn drop(&mut self) {
                (self.0)();
            }
        }
        let _guard = ShutdownGuard(shutdown);
        drive(client, &driver_tq, graph_id, starts, seeds, driver_storage).await
    });

    let worker_result = tokio::time::timeout(Duration::from_secs(180), worker.run())
        .await
        .map_err(|_| anyhow!("worker.run() timed out (180s)"))?
        .map_err(|e| anyhow!("worker.run() exited with error: {e}"));
    let driver_result = driver.await.context("driver task panicked")?;
    worker_result?;
    driver_result
}

async fn drive(
    client: Client,
    task_queue: &str,
    graph_id: GraphId,
    starts: Vec<coral_graph::yaml::WorkflowStart>,
    seeds: Vec<coral_graph::yaml::ResolvedSeedTrigger>,
    storage: Arc<MemoryStorage>,
) -> Result<()> {
    // No `step_cap`: agents must park indefinitely (never self-terminate) so the
    // reaper is the only thing that can stop them.
    for start in &starts {
        client
            .start_workflow(
                AgentWorkflow::run,
                start.input.clone(),
                WorkflowStartOptions::new(task_queue, &start.workflow_id).build(),
            )
            .await
            .with_context(|| format!("start_workflow {}", start.workflow_id))?;
    }
    for seed in &seeds {
        client
            .get_workflow_handle::<AgentWorkflow>(&seed.workflow_id)
            .signal(
                AgentWorkflow::external_signal,
                seed.trigger.clone(),
                WorkflowSignalOptions::default(),
            )
            .await
            .with_context(|| format!("signal seed {}", seed.workflow_id))?;
    }

    let parent = starts
        .iter()
        .find(|s| s.input.agent_name == "analyst")
        .ok_or_else(|| anyhow!("no analyst start"))?;
    let parent_fs = AgentFs::open_for_agent(
        storage.clone() as Arc<dyn AgentStorage>,
        graph_id,
        parent.input.agent_id,
    );

    // ---- Wait for convergence: the parent parked AND its synthesis written.
    //      The read is what proves the parked state we observe is *post-fold*,
    //      not the bare first-tick park.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if Instant::now() > deadline {
            return Err(anyhow!(
                "parent never converged (parked + output) within 60s"
            ));
        }
        let snap = snapshot(&client, &parent.workflow_id).await?;
        let has_output = parent_fs.read_output().await.is_ok();
        if snap.parked && has_output {
            // ---- Assertion 1 (linchpin): a query observes the parked state,
            //      and the agent is `never` past its first cycle. If `parked`
            //      never surfaced true here the predicate could never fire.
            assert!(
                snap.is_never,
                "fixture agent must be never-cadence: {snap:?}"
            );
            assert!(snap.tick >= 1, "parent past its first wake: {snap:?}");
            assert!(
                snap.retirement_request.is_none(),
                "parent must not be retired at first parked observation: {snap:?}"
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // ---- Assertion 2 (retire-after-output): the cited synthesis exists.
    let body_before = parent_fs
        .read_output()
        .await
        .context("read parent output")?;
    assert!(
        body_before.starts_with("consolidated report "),
        "parent output drifted from the scripted WriteOutput; got {body_before:?}"
    );

    // ---- Assertion 3 (negative): the reaper does NOT retire before its window
    //      elapses. Measured from convergence above; the earliest possible retire
    //      (first_sweep_seeing_parked + margin >= converged - interval + margin)
    //      has not arrived at converged + margin/2, so the parent is still
    //      un-retired. This also fails loudly if `wave_margin` were set too small.
    tokio::time::sleep(GC_WAVE_MARGIN / 2).await;
    let mid = snapshot(&client, &parent.workflow_id).await?;
    assert!(
        mid.retirement_request.is_none(),
        "reaper retired before its window elapsed (margin too small?): {mid:?}"
    );

    // ---- Assertion 4 (positive): every agent is retired BY THE REAPER. The
    //      reason string proves it (not step_cap, not self-termination — agents
    //      never self-terminate).
    for start in &starts {
        let name = start.input.agent_name.clone();
        let result: AgentResult = tokio::time::timeout(
            Duration::from_secs(90),
            client
                .get_workflow_handle::<AgentWorkflow>(&start.workflow_id)
                .get_result(WorkflowGetResultOptions::default()),
        )
        .await
        .map_err(|_| anyhow!("{name} not retired within 90s"))?
        .with_context(|| format!("get_result for {name}"))?;
        let AgentResult::Retired { reason } = result;
        assert_eq!(
            reason, RETIRE_REASON,
            "{name} must be retired by the quiescence-GC reaper"
        );
    }

    // ---- Assertion 5 (durable-survives): the parent's output still resolves
    //      after retirement — the workflow died, the record did not.
    let body_after = parent_fs
        .read_output()
        .await
        .context("read parent output after retirement")?;
    assert_eq!(
        body_before, body_after,
        "durable output must survive workflow retirement unchanged"
    );

    eprintln!("quiescence_gc: converged, produced output, then reaper retired all 3 agents");
    Ok(())
}
