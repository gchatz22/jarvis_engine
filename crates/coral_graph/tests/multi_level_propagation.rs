//! Multi-level propagation live test: a linear `root <- mid <- leaf`
//! graph on `never` cadence, in two phases.
//!
//! **Phase 1 (cold assembly):** each node's first-cycle wake plus the
//! `ChildOutput` signals assemble a leaf output up to the root, hop-by-hop
//! — nothing polls.
//!
//! **Phase 2 (re-propagation):** after the graph has settled and every
//! parent has emitted, the leaf emits a *changed* output. This re-wakes the
//! already-emitted, blocked-idle `mid` (via its `ChildOutput` signal), which
//! re-reconciles and re-emits, re-waking `root` — so the change ripples to
//! the root of a *settled* all-`never` graph. This is the continuous-monitor
//! property; phase 1 is the degenerate cold-start case.
//!
//! The genuinely load-bearing node is MID: it both RECEIVES a `ChildOutput`
//! (from leaf) and, after reconciling + emitting, FIRES its own (to root) —
//! and in phase 2 it does so from a settled/idle state.
//!
//! `MockDecide`-scripted (via `RoutingDecide` keyed on mandate text); runs
//! against a real Temporal Server, gated on `TEMPORAL_LIVE_TEST=1`. The
//! scripted decisions prove the wake -> reconcile -> re-signal chain plumbs
//! across levels and across a re-wake; whether a *real model* chooses to
//! reconcile at each hop is a separate manual real-LLM run.
//!
//! Asserted end state (after phase 2):
//! 1. Each level's canonical `outputs/output.md` holds its *v2* body — the
//!    root body flipping `root_v1 -> root_v2` is itself the re-propagation
//!    proof (on `never`, root re-emits only if the change reached it).
//! 2. `mid` has a reconcile evidence record pinning leaf's v1 *and* one
//!    pinning leaf's v2; `root` likewise for mid's two versions.
//! 3. Cross-FS trail: mid's v2 reconcile evidence -> leaf's current output
//!    (`leaf_v2`); root's v2 reconcile evidence -> mid's current output
//!    (`mid_v2`). The chain resolves across three agent FS roots.

use std::collections::VecDeque;
use std::env;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use uuid::Uuid;

use coral_graph::yaml::parse_and_validate;
use coral_node::agent_ref::{AgentId, GraphId};
use coral_node::decision::{Decide, Decision, ReconcileSource, Session};
use coral_node::evidence::EvidenceRecord;
use coral_node::fs::AgentFs;
use coral_node::mandate::{Mandate, OutputId};
use coral_node::storage::{AgentStorage, BlobSha, MemoryStorage};
use coral_node::tools::ToolRegistry;
use coral_node::trigger::Trigger;
use coral_temporal::activities::set_decision_script;
use coral_temporal::worker::{
    build_worker, install_agent_storage, install_decide, install_structural_db_store,
    install_tool_registry, StructuralDbStore,
};
use coral_temporal::workflow::{AgentInput, AgentResult, AgentWorkflow, FsHandle, ParentRef};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Mandate-text discriminators the `RoutingDecide` matches on. Mirror the
/// strings in `examples/smoke_multi_level/graph.yaml`; the test's
/// `parse_and_validate` startup check fires if either side drifts.
const ROOT_MANDATE_TEXT: &str = "multi-level-root";
const MID_MANDATE_TEXT: &str = "multi-level-mid";
const LEAF_MANDATE_TEXT: &str = "multi-level-leaf";

/// Operator-authored agent ids (from the fixture), threaded through
/// `AgentInput.agent_name` and carried on each `ChildOutput` trigger. The
/// relay synthesizer matches on these — agent_id UUIDs are minted per run.
const ROOT_NAME: &str = "root";
const MID_NAME: &str = "mid";
const LEAF_NAME: &str = "leaf";

const GRAPH_YAML_REL: &str = "../../examples/smoke_multi_level/graph.yaml";

/// Scripted per-level, per-round output bodies. The `_v1` set assembles in
/// phase 1; the `_v2` set re-propagates in phase 2. Distinct content per
/// round is what makes re-propagation *observable* (the root body flips).
const LEAF_V1: &str = "leaf level v1";
const LEAF_V2: &str = "leaf level v2";
const MID_V1: &str = "mid folded leaf v1";
const MID_V2: &str = "mid folded leaf v2";
const ROOT_V1: &str = "root folded mid v1";
const ROOT_V2: &str = "root folded mid v2";

static SHARED_STORAGE: OnceLock<Arc<MemoryStorage>> = OnceLock::new();

/// Triggers each relay role's `Decide` has observed, accumulated across the
/// role's cycles (never cleared mid-run) so a `ChildOutput` seen on the wake
/// cycle is still visible on the reconcile cycle. The synthesizer picks the
/// *most recent* matching one, so round 2 reconciles the child's v2.
static MID_OBSERVED: OnceLock<Arc<Mutex<Vec<Trigger>>>> = OnceLock::new();
static ROOT_OBSERVED: OnceLock<Arc<Mutex<Vec<Trigger>>>> = OnceLock::new();

/// Per-role decision queues. Relay roles (mid, root) run two rounds of the
/// reconcile / emit sentinel pair; the leaf is a straight FIFO of two writes.
static LEAF_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();
static MID_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();
static ROOT_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();

/// Per-role emit-content queues: each relay role pops its next body when it
/// synthesizes a `WriteOutput`, so round 1 emits `_v1` and round 2 `_v2`.
static MID_EMIT: OnceLock<Mutex<VecDeque<String>>> = OnceLock::new();
static ROOT_EMIT: OnceLock<Mutex<VecDeque<String>>> = OnceLock::new();

static INIT: std::sync::Once = std::sync::Once::new();

/// No-op `StructuralDbStore`. This test asserts the FS-side provenance
/// trail (reconcile evidence + cross-agent reads), not the DB reference
/// graph, and seeds no agents via `coral apply`, so every port method is a
/// value stub or unreachable.
struct NoopStructuralDb;

#[async_trait]
impl StructuralDbStore for NoopStructuralDb {
    async fn add_agent(&self, _graph_id: GraphId, _name: &str) -> anyhow::Result<AgentId> {
        Ok(AgentId::new(Uuid::new_v4()))
    }
    async fn add_edge(
        &self,
        _parent_agent_id: AgentId,
        _child_agent_id: AgentId,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    async fn list_tool_def_ids_for_graph(&self, _graph_id: GraphId) -> anyhow::Result<Vec<String>> {
        Ok(Vec::new())
    }
    async fn set_file_version(
        &self,
        _agent_id: AgentId,
        _filepath: &str,
        _blob_sha: &BlobSha,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    async fn add_citation(
        &self,
        _citing_agent_id: AgentId,
        _citing_filepath: &str,
        _citing_blob_sha: &BlobSha,
        _cited_agent_id: AgentId,
        _cited_filepath: &str,
        _cited_blob_sha: &BlobSha,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

/// One-shot install of shared storage + empty tool registry +
/// `RoutingDecide` + no-op DB store.
fn ensure_installed() -> Arc<MemoryStorage> {
    INIT.call_once(|| {
        let storage: Arc<MemoryStorage> = Arc::new(MemoryStorage::new());
        SHARED_STORAGE
            .set(Arc::clone(&storage))
            .expect("SHARED_STORAGE set exactly once");
        let dyn_storage: Arc<dyn AgentStorage> = storage;
        install_agent_storage(dyn_storage);
        install_tool_registry(Arc::new(ToolRegistry::new()));
        MID_OBSERVED
            .set(Arc::new(Mutex::new(Vec::new())))
            .expect("MID_OBSERVED set exactly once");
        ROOT_OBSERVED
            .set(Arc::new(Mutex::new(Vec::new())))
            .expect("ROOT_OBSERVED set exactly once");
        LEAF_SCRIPT
            .set(Mutex::new(VecDeque::new()))
            .expect("LEAF_SCRIPT set exactly once");
        MID_SCRIPT
            .set(Mutex::new(VecDeque::new()))
            .expect("MID_SCRIPT set exactly once");
        ROOT_SCRIPT
            .set(Mutex::new(VecDeque::new()))
            .expect("ROOT_SCRIPT set exactly once");
        MID_EMIT
            .set(Mutex::new(VecDeque::new()))
            .expect("MID_EMIT set exactly once");
        ROOT_EMIT
            .set(Mutex::new(VecDeque::new()))
            .expect("ROOT_EMIT set exactly once");
        install_decide(Arc::new(RoutingDecide));
        install_structural_db_store(Arc::new(NoopStructuralDb));
    });
    SHARED_STORAGE.get().cloned().expect("storage installed")
}

/// Routes by `session.seed.mandate.text`: the leaf pops its FIFO; mid and
/// root run the shared relay body (wait for their one child's `ChildOutput`,
/// reconcile the most recent, emit the next queued body).
struct RoutingDecide;

#[async_trait]
impl Decide for RoutingDecide {
    async fn decide(&self, session: &Session) -> anyhow::Result<Decision> {
        match session.seed.mandate.text.as_str() {
            LEAF_MANDATE_TEXT => decide_leaf(LEAF_SCRIPT.get().expect("LEAF_SCRIPT")),
            MID_MANDATE_TEXT => decide_relay(
                session,
                MID_SCRIPT.get().expect("MID_SCRIPT"),
                MID_OBSERVED.get().expect("MID_OBSERVED"),
                MID_EMIT.get().expect("MID_EMIT"),
                LEAF_NAME,
            ),
            ROOT_MANDATE_TEXT => decide_relay(
                session,
                ROOT_SCRIPT.get().expect("ROOT_SCRIPT"),
                ROOT_OBSERVED.get().expect("ROOT_OBSERVED"),
                ROOT_EMIT.get().expect("ROOT_EMIT"),
                MID_NAME,
            ),
            other => panic!(
                "RoutingDecide saw unexpected mandate text: {other:?} \
                 (test scripts only root / mid / leaf mandate text)"
            ),
        }
    }
}

/// Leaf: pop the per-role script; default to a short idle when empty.
fn decide_leaf(script: &Mutex<VecDeque<Decision>>) -> anyhow::Result<Decision> {
    let popped = script
        .lock()
        .expect("leaf script mutex poisoned")
        .pop_front();
    Ok(popped.unwrap_or(Decision::Idle {
        next_after: Duration::from_millis(50),
    }))
}

/// Relay role (mid, root): record observed triggers once per cycle, then
/// return the next scripted decision or synthesize from a sentinel.
fn decide_relay(
    session: &Session,
    script: &Mutex<VecDeque<Decision>>,
    observed: &Arc<Mutex<Vec<Trigger>>>,
    emit_queue: &Mutex<VecDeque<String>>,
    expected_child: &str,
) -> anyhow::Result<Decision> {
    if session.is_empty() && !session.seed.triggers.is_empty() {
        let mut guard = observed.lock().expect("observed triggers mutex poisoned");
        for t in &session.seed.triggers {
            guard.push(t.clone());
        }
    }
    let popped = script
        .lock()
        .expect("relay script mutex poisoned")
        .pop_front();
    match popped {
        Some(d) if is_reconcile_placeholder(&d) => {
            synthesize_reconcile_or_wait(script, observed, expected_child)
        }
        Some(d) if is_emit_placeholder(&d) => synthesize_emit_or_wait(session, script, emit_queue),
        Some(d) => Ok(d),
        None => Ok(Decision::Idle {
            next_after: Duration::from_millis(50),
        }),
    }
}

// Sentinels: `Decision` is the contract enum, so "synthesize at decide
// time" is encoded by overloading `Idle { next_after }` with `Duration`
// values no production script would emit.

fn reconcile_placeholder() -> Decision {
    Decision::Idle {
        next_after: Duration::from_secs(u64::MAX),
    }
}

fn is_reconcile_placeholder(d: &Decision) -> bool {
    matches!(d, Decision::Idle { next_after } if *next_after == Duration::from_secs(u64::MAX))
}

fn emit_placeholder() -> Decision {
    Decision::Idle {
        next_after: Duration::from_secs(u64::MAX - 1),
    }
}

fn is_emit_placeholder(d: &Decision) -> bool {
    matches!(d, Decision::Idle { next_after } if *next_after == Duration::from_secs(u64::MAX - 1))
}

/// Synthesize a single-source `ReconcileChildren` for the *most recent*
/// observed `ChildOutput` from the expected child. Until one is observed,
/// push the placeholder back and idle so the wake gate blocks on the pending
/// signal (on `never`, the child's signal is the only thing that re-wakes
/// this node). Taking the most-recent trigger is what makes round 2
/// reconcile the child's v2 rather than the still-present v1.
fn synthesize_reconcile_or_wait(
    script: &Mutex<VecDeque<Decision>>,
    observed: &Arc<Mutex<Vec<Trigger>>>,
    expected_child: &str,
) -> anyhow::Result<Decision> {
    let found = observed
        .lock()
        .expect("observed triggers mutex poisoned")
        .iter()
        .rev()
        .find_map(|t| match t {
            Trigger::ChildOutput {
                child_ref,
                agent_name,
                output_id,
            } if agent_name == expected_child => Some((child_ref.clone(), output_id.clone())),
            _ => None,
        });
    let Some((child_ref, output_id)) = found else {
        script
            .lock()
            .expect("relay script mutex poisoned")
            .push_front(reconcile_placeholder());
        return Ok(Decision::Idle {
            next_after: Duration::from_millis(50),
        });
    };
    Ok(Decision::ReconcileChildren {
        sources: vec![ReconcileSource {
            child_ref,
            output_id,
        }],
        conflict: None,
    })
}

/// Synthesize `WriteOutput` citing the one synthetic evidence path the
/// reconcile step named in its observation earlier this cycle, with the next
/// queued body. If the reconcile step hasn't run yet (no path visible), push
/// the placeholder back and idle.
fn synthesize_emit_or_wait(
    session: &Session,
    script: &Mutex<VecDeque<Decision>>,
    emit_queue: &Mutex<VecDeque<String>>,
) -> anyhow::Result<Decision> {
    let observation = session
        .steps
        .iter()
        .rev()
        .find_map(|s| match &s.action {
            Decision::ReconcileChildren { .. } => Some(s.observation.content.clone()),
            _ => None,
        })
        .unwrap_or_default();
    let synthetic: Vec<String> = observation
        .split_whitespace()
        .map(|t| t.trim_end_matches([',', '.']))
        .filter(|t| t.starts_with("evidence/") && t.ends_with(".json"))
        .map(|t| t.to_string())
        .collect();
    match synthetic.len() {
        0 => {
            script
                .lock()
                .expect("relay script mutex poisoned")
                .push_front(emit_placeholder());
            Ok(Decision::Idle {
                next_after: Duration::from_millis(50),
            })
        }
        1 => {
            let body = emit_queue
                .lock()
                .expect("emit queue mutex poisoned")
                .pop_front()
                .expect("emit queue exhausted — more reconcile cycles ran than scripted bodies");
            Ok(Decision::WriteOutput {
                body,
                citations: synthetic,
            })
        }
        n => panic!(
            "synthesize_emit_or_wait: expected exactly 1 synthetic evidence path \
             (single reconcile source), got {n}"
        ),
    }
}

fn install_role_scripts(
    root: Vec<Decision>,
    mid: Vec<Decision>,
    leaf: Vec<Decision>,
    mid_emit: Vec<String>,
    root_emit: Vec<String>,
) {
    *ROOT_SCRIPT.get().expect("ROOT_SCRIPT").lock().unwrap() = root.into();
    *MID_SCRIPT.get().expect("MID_SCRIPT").lock().unwrap() = mid.into();
    *LEAF_SCRIPT.get().expect("LEAF_SCRIPT").lock().unwrap() = leaf.into();
    *MID_EMIT.get().expect("MID_EMIT").lock().unwrap() = mid_emit.into();
    *ROOT_EMIT.get().expect("ROOT_EMIT").lock().unwrap() = root_emit.into();
    // Clear the activity's script-first guardrail static so a stale decision
    // from a previous binary run can't leak in.
    set_decision_script(Vec::new());
}

fn reset_observed() {
    MID_OBSERVED
        .get()
        .expect("MID_OBSERVED")
        .lock()
        .unwrap()
        .clear();
    ROOT_OBSERVED
        .get()
        .expect("ROOT_OBSERVED")
        .lock()
        .unwrap()
        .clear();
}

fn run_suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_else(|_| "no-suffix".into())
}

async fn build_client() -> Result<Client> {
    let address = env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.into());
    let namespace = env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| DEFAULT_NAMESPACE.into());
    let url = Url::parse(&address).context("parsing TEMPORAL_ADDRESS")?;
    let connection_options = ConnectionOptions::new(url).build();
    let connection = Connection::connect(connection_options)
        .await
        .context("connecting to Temporal Server (is the dev stack running?)")?;
    let client_options = ClientOptions::new(namespace).build();
    let client = Client::new(connection, client_options).context("building Temporal client")?;
    Ok(client)
}

fn build_runtime() -> Result<CoreRuntime> {
    let telemetry_options = TelemetryOptions::builder().build();
    let rt = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(telemetry_options)
            .build()
            .map_err(|e| anyhow::anyhow!("RuntimeOptions build failed: {e}"))?,
    )?;
    Ok(rt)
}

fn load_graph_yaml() -> Result<String> {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path = manifest_dir.join(GRAPH_YAML_REL);
    std::fs::read_to_string(&path)
        .with_context(|| format!("reading multi-level fixture from {}", path.display()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::await_holding_lock)]
async fn three_level_all_never_graph_assembles_and_repropagates() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping three_level_all_never_graph_assembles_and_repropagates; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    run_end_to_end()
        .await
        .expect("multi-level propagation end-to-end");
}

async fn run_end_to_end() -> Result<()> {
    // ---- 1. Fixture schema/topology smoke-check -----------------------
    let yaml_text = load_graph_yaml()?;
    let graph = parse_and_validate(&yaml_text).context("parse_and_validate multi-level fixture")?;
    assert_eq!(graph.metadata.name, "smoke-multi-level");
    assert_eq!(graph.agents.len(), 1, "single root forest");
    let root = &graph.agents[0];
    assert_eq!(root.id, ROOT_NAME);
    assert_eq!(root.mandate.text, ROOT_MANDATE_TEXT);
    assert_eq!(root.children.len(), 1, "root has exactly one child (mid)");
    let mid = &root.children[0];
    assert_eq!(mid.id, MID_NAME);
    assert_eq!(mid.mandate.text, MID_MANDATE_TEXT);
    assert_eq!(mid.children.len(), 1, "mid has exactly one child (leaf)");
    let leaf = &mid.children[0];
    assert_eq!(leaf.id, LEAF_NAME);
    assert_eq!(leaf.mandate.text, LEAF_MANDATE_TEXT);

    // ---- 2. Per-run setup ---------------------------------------------
    let suffix = run_suffix();
    let task_queue = format!("coral-multi-level-{suffix}");
    let graph_id = GraphId::new(Uuid::new_v4());
    let root_agent_id = AgentId::new(Uuid::new_v4());
    let mid_agent_id = AgentId::new(Uuid::new_v4());
    let leaf_agent_id = AgentId::new(Uuid::new_v4());
    let root_prefix = format!("graphs/{graph_id}/agents/{root_agent_id}");
    let mid_prefix = format!("graphs/{graph_id}/agents/{mid_agent_id}");
    let leaf_prefix = format!("graphs/{graph_id}/agents/{leaf_agent_id}");
    let root_workflow_id = root_prefix.clone();
    let mid_workflow_id = mid_prefix.clone();
    let leaf_workflow_id = leaf_prefix.clone();

    let storage = ensure_installed();
    reset_observed();

    // Plant one evidence record under the leaf so both scripted writes
    // resolve provenance (`persist_output` rejects empty citations). mid and
    // root cite the synthetic reconcile evidence they mint each round.
    let plant_mandate = Mandate::new("plant", Duration::from_millis(0), None);
    let plant_storage: Arc<dyn AgentStorage> = storage.clone();
    let plant_fs = AgentFs::new_with_storage(plant_storage, &leaf_prefix, &plant_mandate)
        .await
        .context("open planting AgentFs for leaf")?;
    let planted_leaf_id = plant_fs
        .record_evidence(
            EvidenceRecord::new(
                "echo",
                serde_json::json!({"level": "leaf"}),
                serde_json::json!({"hit": true}),
                chrono::Utc::now(),
            ),
            "echo leaf",
        )
        .await
        .context("plant evidence for leaf WriteOutput")?;

    // ---- 3. Scripts ---------------------------------------------------
    //
    // leaf: two write cycles. Cycle 0 (first wake) writes v1; the driver
    // later sends an External trigger to wake it for the v2 write. `never` +
    // no step_cap: it idles between writes and is retired by the driver.
    // mid / root: two rounds of reconcile-then-emit, each round's body from
    // the emit queue. On `never`, each round's wake is the child's signal.
    let leaf_script = vec![
        Decision::WriteOutput {
            body: LEAF_V1.into(),
            citations: vec![planted_leaf_id.clone()],
        },
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
        Decision::WriteOutput {
            body: LEAF_V2.into(),
            citations: vec![planted_leaf_id.clone()],
        },
    ];
    // The terminal `Idle` between the two rounds is load-bearing: the inner
    // ReAct loop runs until an `Idle`, so without it both rounds would fire in
    // a single wake (reconcile+emit+reconcile+emit), decoupling the second
    // emit from any real re-fold. The `Idle` ends round 1; round 2 then only
    // runs on the *next* wake — leaf's v2 `ChildOutput` — from a settled
    // state, which is the re-propagation we're verifying.
    let mid_script = vec![
        reconcile_placeholder(),
        emit_placeholder(),
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
        reconcile_placeholder(),
        emit_placeholder(),
    ];
    let root_script = mid_script.clone();
    install_role_scripts(
        root_script,
        mid_script,
        leaf_script,
        vec![MID_V1.into(), MID_V2.into()],
        vec![ROOT_V1.into(), ROOT_V2.into()],
    );

    // ---- 4. Worker + driver -------------------------------------------
    let runtime = build_runtime()?;
    let client = build_client().await?;
    let mut worker = build_worker(&runtime, client.clone(), &task_queue)?;
    let shutdown = worker.shutdown_handle();

    let driver = tokio::spawn({
        let task_queue = task_queue.clone();
        let root_prefix = root_prefix.clone();
        let mid_prefix = mid_prefix.clone();
        let leaf_prefix = leaf_prefix.clone();
        let root_workflow_id = root_workflow_id.clone();
        let mid_workflow_id = mid_workflow_id.clone();
        let leaf_workflow_id = leaf_workflow_id.clone();
        let storage_arc: Arc<MemoryStorage> = SHARED_STORAGE
            .get()
            .expect("SHARED_STORAGE installed")
            .clone();
        async move {
            struct ShutdownGuard<F: Fn()>(F);
            impl<F: Fn()> Drop for ShutdownGuard<F> {
                fn drop(&mut self) {
                    (self.0)();
                }
            }
            let _g = ShutdownGuard(shutdown);
            drive(
                client,
                &task_queue,
                graph_id,
                root_agent_id,
                mid_agent_id,
                leaf_agent_id,
                &root_workflow_id,
                &mid_workflow_id,
                &leaf_workflow_id,
                &root_prefix,
                &mid_prefix,
                &leaf_prefix,
                storage_arc,
            )
            .await
        }
    });

    let worker_result = tokio::time::timeout(Duration::from_secs(240), worker.run())
        .await
        .map_err(|_| anyhow::anyhow!("worker.run() timed out (240s)"))?
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"));
    let driver_result = driver.await.context("driver task panicked")?;
    worker_result?;
    driver_result?;
    Ok(())
}

/// Poll a freshly-read canonical output until it equals `want`, or fail after
/// 120s. `read_output` is a live storage read each call, so reusing the view
/// across iterations sees new writes.
async fn poll_output_eq(view: &AgentFs, want: &str, phase: &str) -> Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        if let Ok(body) = view.read_output().await {
            if body == want {
                return Ok(());
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "{phase}: root output did not reach the expected body within 120s — \
                 the leaf->mid->root relay stalled (a woken node did not reconcile + re-signal)"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive(
    client: Client,
    task_queue: &str,
    graph_id: GraphId,
    root_agent_id: AgentId,
    mid_agent_id: AgentId,
    leaf_agent_id: AgentId,
    root_workflow_id: &str,
    mid_workflow_id: &str,
    leaf_workflow_id: &str,
    root_prefix: &str,
    mid_prefix: &str,
    leaf_prefix: &str,
    storage: Arc<MemoryStorage>,
) -> Result<()> {
    // Start parents before children so each is addressable when its child
    // fires `ChildOutput`. All three are `never` with no step_cap — they idle
    // after their work and are retired by this driver once phase 2 lands.
    let root_input = AgentInput {
        cfg: Default::default(),
        fs_handle: FsHandle {
            prefix: root_prefix.into(),
        },
        parent_handle: None,
        carryover: None,
        mandate: Mandate::new_never(ROOT_MANDATE_TEXT, None),
        graph_id,
        agent_id: root_agent_id,
        agent_name: ROOT_NAME.into(),
    };
    let root_handle = client
        .start_workflow(
            AgentWorkflow::run,
            root_input,
            WorkflowStartOptions::new(task_queue, root_workflow_id).build(),
        )
        .await
        .context("start_workflow(root)")?;

    let mid_input = AgentInput {
        cfg: Default::default(),
        fs_handle: FsHandle {
            prefix: mid_prefix.into(),
        },
        parent_handle: Some(ParentRef {
            workflow_id: root_workflow_id.to_string(),
            ..ParentRef::default()
        }),
        carryover: None,
        mandate: Mandate::new_never(MID_MANDATE_TEXT, None),
        graph_id,
        agent_id: mid_agent_id,
        agent_name: MID_NAME.into(),
    };
    let mid_handle = client
        .start_workflow(
            AgentWorkflow::run,
            mid_input,
            WorkflowStartOptions::new(task_queue, mid_workflow_id).build(),
        )
        .await
        .context("start_workflow(mid)")?;

    let leaf_input = AgentInput {
        cfg: Default::default(),
        fs_handle: FsHandle {
            prefix: leaf_prefix.into(),
        },
        parent_handle: Some(ParentRef {
            workflow_id: mid_workflow_id.to_string(),
            ..ParentRef::default()
        }),
        carryover: None,
        mandate: Mandate::new_never(LEAF_MANDATE_TEXT, None),
        graph_id,
        agent_id: leaf_agent_id,
        agent_name: LEAF_NAME.into(),
    };
    let leaf_handle = client
        .start_workflow(
            AgentWorkflow::run,
            leaf_input,
            WorkflowStartOptions::new(task_queue, leaf_workflow_id).build(),
        )
        .await
        .context("start_workflow(leaf)")?;

    let inspect_mandate = Mandate::new("inspect", Duration::from_millis(0), None);
    let root_view = AgentFs::new_with_storage(
        storage.clone() as Arc<dyn AgentStorage>,
        root_prefix,
        &inspect_mandate,
    )
    .await
    .context("open root AgentFs for polling")?;

    // Phase 1: cold assembly. leaf's first-cycle write ripples to the root.
    poll_output_eq(&root_view, ROOT_V1, "phase 1 (cold assembly)").await?;

    // Phase 2: re-propagation. The graph is now settled (root emitted v1, mid
    // and root idle on `never`). Wake the leaf with an external event so it
    // emits v2; that ChildOutput must re-wake the settled mid, which
    // re-reconciles + re-emits, re-waking the settled root.
    leaf_handle
        .signal(
            AgentWorkflow::external_signal,
            Trigger::External {
                kind: "update".into(),
                payload: serde_json::json!({"round": 2}),
            },
            WorkflowSignalOptions::default(),
        )
        .await
        .context("signal leaf External (trigger v2 write)")?;
    poll_output_eq(&root_view, ROOT_V2, "phase 2 (re-propagation)").await?;

    // Both phases landed. Retire all three `never` nodes so their workflows
    // close and `get_result` returns.
    for (handle, label) in [
        (&leaf_handle, "leaf"),
        (&mid_handle, "mid"),
        (&root_handle, "root"),
    ] {
        handle
            .signal(
                AgentWorkflow::retire,
                "test complete".to_string(),
                WorkflowSignalOptions::default(),
            )
            .await
            .with_context(|| format!("signal {label} retire"))?;
        let _r: AgentResult = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .with_context(|| format!("{label} get_result after retire"))?;
    }

    assert_end_state(
        storage,
        graph_id,
        root_agent_id,
        mid_agent_id,
        leaf_agent_id,
        root_prefix,
        mid_prefix,
        leaf_prefix,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn assert_end_state(
    storage: Arc<MemoryStorage>,
    graph_id: GraphId,
    root_agent_id: AgentId,
    mid_agent_id: AgentId,
    leaf_agent_id: AgentId,
    root_prefix: &str,
    mid_prefix: &str,
    leaf_prefix: &str,
) -> Result<()> {
    let inspect_mandate = Mandate::new("inspect", Duration::from_millis(0), None);
    let inspect: Arc<dyn AgentStorage> = storage.clone();
    let _ = root_agent_id;

    // --- Current (v2) output bodies ------------------------------------
    let leaf_view = AgentFs::new_with_storage(inspect.clone(), leaf_prefix, &inspect_mandate)
        .await
        .context("open leaf AgentFs")?;
    assert_eq!(
        leaf_view.read_output().await.context("read_output(leaf)")?,
        LEAF_V2,
        "leaf current output should be v2"
    );

    let mid_view = AgentFs::new_with_storage(inspect.clone(), mid_prefix, &inspect_mandate)
        .await
        .context("open mid AgentFs")?;
    assert_eq!(
        mid_view.read_output().await.context("read_output(mid)")?,
        MID_V2,
        "mid should have re-emitted v2 after re-reconciling leaf's v2"
    );

    let root_view = AgentFs::new_with_storage(inspect.clone(), root_prefix, &inspect_mandate)
        .await
        .context("open root AgentFs")?;
    assert_eq!(
        root_view.read_output().await.context("read_output(root)")?,
        ROOT_V2,
        "root current output should be v2 — the change did not re-propagate to the root"
    );

    // --- Reconcile-evidence chain, both rounds -------------------------
    //
    // mid reconciled leaf twice (v1, then v2); root reconciled mid twice.
    // Assert each round's synthetic record exists and pins the right child
    // output id — the v2 records prove the re-propagation reconcile happened,
    // the v1 records prove the initial assembly did.
    let mid_recs = reconcile_records(&mid_view).await?;
    assert_pins(&mid_recs, "mid", leaf_agent_id, &OutputId::new(LEAF_V1))?;
    let mid_v2_rec = assert_pins(&mid_recs, "mid", leaf_agent_id, &OutputId::new(LEAF_V2))?;

    let root_recs = reconcile_records(&root_view).await?;
    assert_pins(&root_recs, "root", mid_agent_id, &OutputId::new(MID_V1))?;
    let root_v2_rec = assert_pins(&root_recs, "root", mid_agent_id, &OutputId::new(MID_V2))?;

    // --- Cross-FS trail on the v2 records (load-bearing) ---------------
    //
    // root's v2 reconcile evidence -> open mid FS -> current output == mid_v2;
    // mid's v2 reconcile evidence -> open leaf FS -> current output == leaf_v2.
    for (rec, expected_child_body) in [(&root_v2_rec, MID_V2), (&mid_v2_rec, LEAF_V2)] {
        let child_aid: AgentId = rec
            .args
            .as_object()
            .and_then(|o| o.get("child_agent_id"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("reconcile evidence missing child_agent_id"))?
            .parse()
            .context("child_agent_id parses as AgentId")?;
        let child_fs = AgentFs::open_for_agent(storage.clone(), graph_id, child_aid);
        let resolved = child_fs
            .read_output()
            .await
            .with_context(|| format!("cross-agent read_output on child {child_aid}"))?;
        assert_eq!(resolved, expected_child_body);
        assert_eq!(OutputId::new(&resolved), OutputId::new(expected_child_body));
    }

    Ok(())
}

/// All `tool == "reconcile"` evidence records under an agent's FS.
async fn reconcile_records(view: &AgentFs) -> Result<Vec<EvidenceRecord>> {
    Ok(view
        .list_recent_evidence(16)
        .await
        .context("list_recent_evidence")?
        .into_iter()
        .filter(|e| e.tool == "reconcile")
        .collect())
}

/// Assert exactly one of `records` pins `expected_output_id` for the expected
/// child; return it.
fn assert_pins(
    records: &[EvidenceRecord],
    label: &str,
    expected_child_agent_id: AgentId,
    expected_output_id: &OutputId,
) -> Result<EvidenceRecord> {
    let matches: Vec<&EvidenceRecord> = records
        .iter()
        .filter(|r| {
            r.args
                .as_object()
                .and_then(|o| o.get("source_output_id"))
                .and_then(|v| serde_json::from_value::<OutputId>(v.clone()).ok())
                .map(|o| &o == expected_output_id)
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "{label}: expected exactly one reconcile record pinning {expected_output_id:?}, \
         got {}",
        matches.len()
    );
    let rec = matches[0];
    let child_aid: AgentId = rec
        .args
        .as_object()
        .and_then(|o| o.get("child_agent_id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("{label}: reconcile evidence missing child_agent_id"))?
        .parse()
        .with_context(|| format!("{label}: child_agent_id parses"))?;
    assert_eq!(
        child_aid, expected_child_agent_id,
        "{label}: reconcile evidence points at the wrong child"
    );
    Ok(rec.clone())
}
