//! Long-lived Coral worker daemon. Connects to a Temporal Server,
//! installs the process-wide backends every activity body reaches for
//! (agent storage, the `Decide` impl, the per-graph tool-registry provider,
//! and the structural-DB store), registers [`coral_temporal::workflow::AgentWorkflow`]
//! and [`coral_temporal::activities::AgentActivities`] against the
//! canonical task queue, and runs until SIGINT.
//!
//! Required env: `DATABASE_URL` — Postgres URL for the structural DB. The
//! daemon installs a `GraphStore`-backed [`StructuralDbStore`] so
//! `Decision::SpawnChild`'s `register_child_in_structural_db` activity can
//! write child `agents` + `edges` rows. The worker does **not** run
//! migrations; apply the schema via `coral apply` first.
//!
//! Optional env: `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`,
//! `TEMPORAL_TASK_QUEUE`, `AGENT_FS_ROOT`, `ANTHROPIC_API_KEY` /
//! `ANTHROPIC_MODEL`, `COHERE_API_KEY` / `COHERE_MODEL`. The model registry
//! is booted from every provider compiled in whose key is set, so agents
//! select a provider per their `provider/model` config. Provider `Decide`
//! construction is `#[cfg]`-gated behind this crate's `llm-anthropic` /
//! `llm-cohere` features; a build with no provider compiles but errors at
//! boot when no `Decide` impl can be installed.

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use coral_graph::GraphStore;
use coral_node::storage::{PerAgentGitStorage, VersionedStorage};
use coral_worker::reaper::{run_reaper, ReaperConfig};
use coral_worker::tool_provider::DbToolRegistryProvider;
use sqlx::postgres::PgPoolOptions;
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use tracing::info;

use coral_temporal::worker::{
    build_decide_from_env, build_worker, install_agent_storage, install_decide,
    install_structural_db_store, install_tool_registry_provider, install_versioned_storage,
    StructuralDbStore, DEFAULT_TASK_QUEUE,
};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_FS_ROOT: &str = "./agent-fs";
const DATABASE_URL_ENV: &str = "DATABASE_URL";
const DB_POOL_MAX_CONNECTIONS: u32 = 8;

/// Resolve the structural-DB connection string from the raw env value.
/// Required: without it the daemon has no `StructuralDbStore` and the first
/// `Decision::SpawnChild` fails mid-workflow. Failing here turns that
/// deferred panic into a clean boot-time error.
fn require_database_url(value: Option<String>) -> Result<String> {
    match value {
        Some(url) if !url.is_empty() => Ok(url),
        _ => Err(anyhow!(
            "{DATABASE_URL_ENV} must be set; the worker installs the structural-DB store at boot \
             so Decision::SpawnChild can register child agents (see crates/coral_worker/README.md)"
        )),
    }
}

async fn build_client() -> Result<Client> {
    let address = env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.into());
    let namespace = env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| DEFAULT_NAMESPACE.into());

    let url = Url::parse(&address).context("parsing TEMPORAL_ADDRESS")?;
    let connection_options = ConnectionOptions::new(url).build();
    let connection = Connection::connect(connection_options)
        .await
        .context("connecting to Temporal Server")?;
    let client_options = ClientOptions::new(namespace).build();
    let client = Client::new(connection, client_options).context("building Temporal client")?;
    Ok(client)
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,temporalio=warn")),
        )
        .try_init();

    let task_queue = env::var("TEMPORAL_TASK_QUEUE").unwrap_or_else(|_| DEFAULT_TASK_QUEUE.into());

    let fs_root = env::var("AGENT_FS_ROOT").unwrap_or_else(|_| DEFAULT_FS_ROOT.into());
    let fs_root_path = PathBuf::from(&fs_root);
    let storage = Arc::new(
        PerAgentGitStorage::new(fs_root_path.clone())
            .with_context(|| format!("opening PerAgentGitStorage at {fs_root}"))?,
    );
    // One backend object, installed under both handles: the data plane
    // (put/get/list) and the versioning plane (the cycle-boundary `commit_tick`
    // activity commits through `agent_versioned_storage_opt()`).
    install_versioned_storage(Arc::clone(&storage) as Arc<dyn VersionedStorage>);
    install_agent_storage(storage);
    info!(
        fs_root = fs_root.as_str(),
        "installed AgentStorage backend (per-agent git-versioned)"
    );

    let (providers, decide) = build_decide_from_env()?;
    install_decide(decide);
    info!(providers = providers.as_str(), "installed Decide backend");

    // Structural-DB store: installed before the worker serves any task so
    // `register_child_in_structural_db` always finds a real `GraphStore`.
    // The same `GraphStore` backs the per-graph tool-registry provider. The
    // URL is kept out of the logs because it carries the DB password.
    let database_url = require_database_url(env::var(DATABASE_URL_ENV).ok())?;
    let pool = PgPoolOptions::new()
        .max_connections(DB_POOL_MAX_CONNECTIONS)
        .connect(&database_url)
        .await
        .with_context(|| format!("connecting to structural DB at {DATABASE_URL_ENV}"))?;
    let graph_store = Arc::new(GraphStore::new(pool));
    let structural_store: Arc<dyn StructuralDbStore> = graph_store.clone();
    install_structural_db_store(structural_store);
    info!("installed StructuralDbStore backend (GraphStore over DATABASE_URL)");

    // Per-graph tool registries are built lazily from each graph's structural
    // rows (builtin echo plus that graph's MCP servers), so `graph.yaml` is
    // the runtime source of truth for tools. Clone the store first so the
    // quiescence-GC reaper keeps a handle after the provider takes ownership.
    let reaper_store = graph_store.clone();
    install_tool_registry_provider(Arc::new(DbToolRegistryProvider::new(graph_store)));
    info!(
        "installed ToolRegistryProvider (per-graph registries from structural DB; builtin: echo)"
    );

    let telemetry_options = TelemetryOptions::builder().build();
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(telemetry_options)
            .build()
            .map_err(|e| anyhow::anyhow!("RuntimeOptions build failed: {e}"))?,
    )?;

    let client = build_client().await?;
    let mut worker = build_worker(&runtime, client.clone(), &task_queue)?;
    let shutdown = worker.shutdown_handle();

    // Quiescence-GC reaper: a background sweep that retires provably-dead graphs
    // (all agents `never`-cadence, parked, and stable-idle across a full
    // propagation wave). Being a live worker itself, it satisfies the
    // "a live worker must be observing the graph" guard.
    let reaper_config = ReaperConfig::from_env();
    tokio::spawn(run_reaper(client, reaper_store, reaper_config));
    info!(
        interval_secs = reaper_config.interval.as_secs(),
        wave_margin_secs = reaper_config.wave_margin.as_secs(),
        "spawned quiescence-GC reaper"
    );

    // SIGINT handler runs on a spawned task because `Worker` is not `Send`
    // and must stay pinned to the main task.
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("received Ctrl-C; initiating worker shutdown");
        shutdown();
    });

    info!(
        task_queue = task_queue.as_str(),
        providers = providers.as_str(),
        "coral worker starting; registered: AgentWorkflow + AgentActivities"
    );
    worker
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"))?;
    info!("coral worker exited cleanly");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn require_database_url_returns_value_when_set() {
        let got = require_database_url(Some("postgres://coral@localhost/db".to_string()))
            .expect("set url accepted");
        assert_eq!(got, "postgres://coral@localhost/db");
    }

    #[test]
    fn require_database_url_errors_when_unset() {
        let err = require_database_url(None).expect_err("missing url rejected");
        assert!(
            format!("{err}").contains(DATABASE_URL_ENV),
            "error should name the env var; got: {err}"
        );
    }

    #[test]
    fn require_database_url_errors_when_empty() {
        let err = require_database_url(Some(String::new())).expect_err("empty url rejected");
        assert!(
            format!("{err}").contains(DATABASE_URL_ENV),
            "error should name the env var; got: {err}"
        );
    }
}
