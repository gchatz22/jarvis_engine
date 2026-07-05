//! Recorded-fixture integration tests for the Cohere adapter.
//!
//! Mirror of `llm_fixture_anthropic.rs`. Drives `LlmDecide` and
//! `Agent::run` against the real `CohereClient` HTTP path, pointed at
//! the mock server in `tests/llm_fixture/mod.rs`. Fixtures live under
//! `tests/fixtures/llm/cohere/`. Plus one `CORAL_LIVE_LLM=1`-gated
//! smoke against the real Cohere API. Feature-gated:
//! `--features llm-cohere`.

#![cfg(feature = "llm-cohere")]

mod llm_fixture;

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::time::timeout;

use coral_node::agent::{Agent, RetireReason};
use coral_node::decision::{Decide, Decision, FsIndex, Seed, Session};
use coral_node::fs::AgentFs;
use coral_node::health::{HealthTracker, RetryBudget};
use coral_node::mandate::Mandate;
use coral_node::model_client::cohere::CohereClient;
use coral_node::model_client::{
    CompleteOptions, CompleteRequest, ContentBlock, Message, ModelClient, Role, ToolCall, Vendor,
};
use coral_node::tools::{EchoTool, ToolRegistry};

use coral_node::decide_llm::llm_decide::LlmDecide;

use llm_fixture::{live_llm_enabled, load_fixture, MockServer};

const FIXTURE_API_KEY: &str = "fixture-key-not-real";

fn ensure_dummy_api_key() {
    if std::env::var("COHERE_API_KEY")
        .map(|v| v.is_empty())
        .unwrap_or(true)
    {
        // SAFETY: hermetic tests don't read the env concurrently.
        unsafe {
            std::env::set_var("COHERE_API_KEY", FIXTURE_API_KEY);
        }
    }
}

fn empty_session() -> Session {
    Session::new(Seed::new(
        Mandate::new("cohere fixture", Duration::from_secs(60), Some(8)),
        vec![],
        FsIndex::default(),
    ))
}

fn build_client(base_url: String) -> CohereClient {
    CohereClient::new().with_base_url(base_url)
}

fn expected_model() -> String {
    CohereClient::new().model().to_string()
}

// ---------- (a) happy-path tick ----------

#[tokio::test]
async fn happy_path_tick_drives_call_tool_then_write_output() {
    ensure_dummy_api_key();
    let fixtures = load_fixture("cohere", "happy_path_tick");
    let mock = MockServer::spawn(fixtures).await;

    let client = Arc::new(build_client(mock.base_url()));
    let decide = LlmDecide::new(client.clone(), CompleteOptions::default());

    // Tick 1: model emits CallTools(vec![echo, ...]).
    let dec_1 = decide
        .decide(&empty_session())
        .await
        .expect("tick 1 decide");
    match &dec_1 {
        Decision::CallTools { calls } => {
            assert_eq!(calls.len(), 1, "single-call happy path");
            let c = &calls[0];
            assert_eq!(c.name, "echo");
            assert_eq!(c.args, json!({"msg": "hello coral"}));
            assert_eq!(c.claim_seed.as_str(), "fixture-seed-1");
            // Vendor `tool_use.id` from the fixture must propagate.
            assert_eq!(c.tool_use_id.as_deref(), Some("tc_call_tool_1"));
        }
        other => panic!("expected CallTools, got {other:?}"),
    }
    let calls_1 = decide.last_tick_calls();
    assert_eq!(calls_1.len(), 1);
    assert_eq!(calls_1[0].vendor, Vendor::Cohere);
    assert_eq!(calls_1[0].model, expected_model());
    assert!(
        calls_1[0].latency_ms > 0,
        "tick 1 stats.latency_ms must be > 0"
    );

    // Tick 2: model emits WriteOutput.
    let dec_2 = decide
        .decide(&empty_session())
        .await
        .expect("tick 2 decide");
    match &dec_2 {
        Decision::WriteOutput { body, citations } => {
            assert_eq!(body, "the echo tool returned hello coral");
            assert_eq!(citations.len(), 1);
            assert_eq!(
                citations[0].as_str(),
                "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
            );
        }
        other => panic!("expected WriteOutput, got {other:?}"),
    }
    let calls_2 = decide.last_tick_calls();
    assert_eq!(calls_2.len(), 1);
    assert_eq!(calls_2[0].vendor, Vendor::Cohere);
    assert_eq!(calls_2[0].model, expected_model());
    assert!(
        calls_2[0].latency_ms > 0,
        "tick 2 stats.latency_ms must be > 0"
    );

    assert_eq!(mock.remaining(), 0);
    let captured = mock.captured();
    assert_eq!(captured.len(), 2);
    for req in &captured {
        assert_eq!(req.method, "POST");
        let body: Value = req.json();
        assert_eq!(body["model"], json!(expected_model()));
        // Cohere wire format keeps the system turn inside `messages[]`,
        // so the array is non-empty even on an otherwise-empty bundle.
        let msgs = body["messages"].as_array().expect("messages array");
        assert!(!msgs.is_empty(), "messages must be non-empty");
        assert_eq!(msgs[0]["role"], json!("system"));
        // Tool list wraps each entry in `{type: "function", function: {...}}`.
        let tools = body["tools"].as_array().expect("tools array");
        assert!(
            tools
                .iter()
                .any(|t| t["function"]["name"] == json!("write_output")),
            "decision-tool list must include write_output"
        );
    }
}

// ---------- (b) parse-retry correction (LlmDecide-internal) ----------

#[tokio::test]
async fn parse_retry_recovers_after_malformed_tool_use() {
    ensure_dummy_api_key();
    let fixtures = load_fixture("cohere", "corrective_tick");
    let mock = MockServer::spawn(fixtures).await;

    let client = Arc::new(build_client(mock.base_url()));
    let decide = LlmDecide::new(client.clone(), CompleteOptions::default());

    let dec = decide
        .decide(&empty_session())
        .await
        .expect("decide should recover");
    match &dec {
        Decision::Idle { next_after } => {
            assert_eq!(*next_after, Duration::from_millis(1500));
        }
        other => panic!("expected Idle after corrective retry, got {other:?}"),
    }

    let calls = decide.last_tick_calls();
    assert_eq!(
        calls.len(),
        2,
        "corrective tick should record both upstream calls"
    );
    for s in &calls {
        assert_eq!(s.vendor, Vendor::Cohere);
        assert_eq!(s.model, expected_model());
        assert!(s.latency_ms > 0, "every call's latency_ms must be > 0");
    }
    let totals = decide.last_tick_totals();
    assert_eq!(totals.calls, 2);
    assert_eq!(totals.input_tokens, 130 + 188);
    assert_eq!(totals.output_tokens, 36 + 25);
    assert!(totals.latency_ms > 0);

    // Cohere keeps `system` turns inside `messages[]` (no top-level
    // `system` field), so the corrective system message we appended on
    // retry shows up as a `{role: "system", content: "..."}` entry near
    // the end of the second request's messages array.
    let captured = mock.captured();
    assert_eq!(captured.len(), 2);
    let retry_body: Value = captured[1].json();
    let msgs = retry_body["messages"]
        .as_array()
        .expect("retry messages array");
    let has_corrective_system = msgs.iter().any(|m| {
        m["role"] == json!("system")
            && m["content"]
                .as_str()
                .map(|s| s.contains("could not be parsed"))
                .unwrap_or(false)
    });
    assert!(
        has_corrective_system,
        "retry must include a system message describing the parse failure, got: {msgs:?}"
    );
}

// ---------- (c) unhealthy → recovery via Agent::run ----------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn unhealthy_then_recovery_cycle_via_agent_run() {
    ensure_dummy_api_key();
    let fixtures = load_fixture("cohere", "unhealthy_recovery");
    let mock = MockServer::spawn(fixtures).await;

    let tmp = TempDir::new().expect("tempdir");
    // The fixture's 4 responses span 3 ticks (one tick parse-fails then
    // recovers on its internal retry); cap at 3 so the loop consumes them
    // all then stops on the safety cap (agents never self-terminate).
    let mandate = Mandate::new(
        "unhealthy recovery cycle",
        Duration::from_millis(50),
        Some(3),
    );
    let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
        .await
        .expect("open fs");

    let client = Arc::new(build_client(mock.base_url()));
    let decide = LlmDecide::new(client.clone(), CompleteOptions::default());

    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(EchoTool))
        .expect("register echo");

    let health = HealthTracker::open(tmp.path(), RetryBudget::default(), chrono::Utc::now())
        .expect("open health");

    let agent = Agent::new(mandate, fs, decide, registry, health);

    // Capture the stats handle before `Agent::run` consumes the agent.
    let stats = agent.stats_handle();

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(10), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "step_cap (3) reached");

    // Wire-level count assertion — kept because the new stats accessor
    // only sees the most recent tick, not the whole run.
    assert_eq!(mock.remaining(), 0);
    assert_eq!(mock.captured().len(), 4);

    // Post-run stats inspection. The recovery tick issues one successful
    // upstream call; its CallStats must carry Cohere vendor + configured
    // model. Latency is not asserted here — `start_paused` freezes
    // tokio's clock and the millisecond-resolution adapter measurement
    // can round to 0.
    let calls = stats.last_tick_calls();
    assert_eq!(
        calls.len(),
        1,
        "last tick was the recovery idle — one upstream call"
    );
    assert_eq!(calls[0].vendor, Vendor::Cohere);
    assert_eq!(calls[0].model, expected_model());
    let totals = stats.last_tick_totals();
    assert_eq!(totals.calls, 1);
    assert!(totals.input_tokens > 0);
    assert!(totals.output_tokens > 0);

    let live: Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(
        live.get("state").and_then(|x| x.as_str()),
        Some("Healthy"),
        "agent must recover to Healthy after the next successful tick"
    );

    let archive_dir = tmp.path().join("health");
    assert!(archive_dir.is_dir());
    let archived: Vec<_> = std::fs::read_dir(&archive_dir)
        .expect("read archive")
        .map(|e| e.expect("dirent").path())
        .collect();
    assert_eq!(archived.len(), 1, "exactly one archived incident expected");
    let inc: Value = serde_json::from_slice(&std::fs::read(&archived[0]).expect("read archive"))
        .expect("parse archive");
    assert_eq!(
        inc.get("incident")
            .and_then(|i| i.get("failing"))
            .and_then(|f| f.get("type"))
            .and_then(|x| x.as_str()),
        Some("Inference"),
    );
}

// ---------- (d) Bug A regression: tool-call -> tool-result roundtrip ----------

/// Cohere rejects HTTP requests where an assistant turn serializes as
/// `{"role":"assistant","content":"","tool_calls":[...]}` with HTTP 400
/// `must have non-empty content or tool calls`. The serializer must
/// therefore omit `content` entirely on tool-call-only assistant turns.
///
/// The mock doesn't replicate Cohere's validation, so the test pins the
/// captured request body shape: `content` must be absent on the
/// tool-call-only assistant message.
#[tokio::test]
async fn tool_call_roundtrip_assistant_turn_omits_empty_content() {
    ensure_dummy_api_key();
    let fixtures = load_fixture("cohere", "tool_call_roundtrip");
    let mock = MockServer::spawn(fixtures).await;
    let client = build_client(mock.base_url());

    // Mirror the wire shape `LlmDecide`'s parse-retry path produces: the
    // model's prior assistant turn was tool-call-only (no text), and the
    // runtime is now appending the corresponding tool result and asking
    // the model what to do next. This is the exact shape that fires the
    // live HTTP 400 against Cohere's chat endpoint.
    let req = CompleteRequest {
        messages: vec![
            Message::system("be terse"),
            Message::user("Echo the message \"tool-call roundtrip\" via the echo tool."),
            Message {
                role: Role::Assistant,
                content: vec![ContentBlock::ToolUse {
                    id: "tc_call_tool_42".into(),
                    name: "call_tool".into(),
                    input: json!({
                        "name": "echo",
                        "args": {"msg": "tool-call roundtrip"},
                        "claim_seed": "roundtrip-seed",
                    }),
                }],
            },
            Message {
                role: Role::Tool,
                content: vec![ContentBlock::ToolResult {
                    tool_use_id: "tc_call_tool_42".into(),
                    content: "echoed: tool-call roundtrip".into(),
                }],
            },
        ],
        tools: vec![],
        runtime_tools: Vec::new(),
        model: None,
        options: CompleteOptions::default(),
    };

    // Call the second fixture (the first is unused; queue it so the
    // mock's FIFO matches the same shape the LlmDecide path uses).
    // We actually only issue one HTTP call here, so drop the first
    // queued response by sending one request that consumes it.
    let _warmup = client
        .complete(CompleteRequest {
            messages: vec![Message::user("warmup")],
            tools: vec![],
            runtime_tools: Vec::new(),
            model: None,
            options: CompleteOptions::default(),
        })
        .await
        .expect("warmup ok (consumes fixture 1)");

    let resp = client
        .complete(req)
        .await
        .expect("Cohere accepts tool-call -> tool-result roundtrip after the Bug-A fix");

    // The model's reply on tick 2 is `write_output` (per the fixture).
    assert_eq!(resp.tool_calls.len(), 1);
    let tc: &ToolCall = &resp.tool_calls[0];
    assert_eq!(tc.name, "write_output");

    // The captured request body for the *second* call is what we care
    // about. The pre-fix code path would have emitted
    // `messages[2] = {"role": "assistant", "content": "", "tool_calls": [...]}`;
    // the post-fix code path omits `content` entirely.
    let captured = mock.captured();
    assert_eq!(captured.len(), 2, "warmup + roundtrip = 2 requests");
    let second: Value = captured[1].json();
    let msgs = second["messages"].as_array().expect("messages array");
    // system + user + assistant + tool = 4
    assert_eq!(msgs.len(), 4, "messages: {msgs:?}");
    assert_eq!(msgs[2]["role"], json!("assistant"));
    assert!(
        msgs[2].get("content").is_none(),
        "assistant turn with only tool_calls must OMIT `content` (Cohere rejects empty-string content + tool_calls). got: {}",
        msgs[2]
    );
    assert!(
        msgs[2].get("tool_calls").is_some(),
        "assistant turn must keep tool_calls"
    );
    assert_eq!(msgs[3]["role"], json!("tool"));
    assert_eq!(msgs[3]["tool_call_id"], json!("tc_call_tool_42"));

    assert_eq!(resp.stats.vendor, Vendor::Cohere);
    assert_eq!(resp.stats.model, expected_model());
}

// ---------- (e) parallel tool calls ----------

/// A Cohere response carrying K=3 entries in `message.tool_calls` parses
/// into one `Decision::CallTools` with three entries. The adapter
/// synthesizes per-block `ContentBlock::ToolUse` from the wire shape;
/// the parser keeps the per-call `tool_use_id` traceable through the
/// propagation pipeline.
#[tokio::test]
async fn parallel_tool_calls_k3_folds_into_single_call_tools_decision() {
    ensure_dummy_api_key();
    let fixtures = load_fixture("cohere", "parallel_tool_calls");
    let mock = MockServer::spawn(fixtures).await;

    let client = Arc::new(build_client(mock.base_url()));
    let decide = LlmDecide::new(client.clone(), CompleteOptions::default());

    let dec = decide
        .decide(&empty_session())
        .await
        .expect("parallel decide");
    match dec {
        Decision::CallTools { calls } => {
            assert_eq!(calls.len(), 3, "parser must fold K=3 tool_calls entries");
            assert_eq!(calls[0].name, "echo");
            assert_eq!(calls[0].args, json!({"path": "a.md"}));
            assert_eq!(calls[0].claim_seed.as_str(), "seed-a");
            assert_eq!(calls[0].tool_use_id.as_deref(), Some("tc_read_a"));
            assert_eq!(calls[1].tool_use_id.as_deref(), Some("tc_read_b"));
            assert_eq!(calls[2].tool_use_id.as_deref(), Some("tc_read_c"));
        }
        other => panic!("expected CallTools, got {other:?}"),
    }
    let calls = decide.last_tick_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].vendor, Vendor::Cohere);
}

// ---------- live-gated smoke ----------

/// Hits the real Cohere API when `CORAL_LIVE_LLM=1` is set.
///   `CORAL_LIVE_LLM=1 cargo test --features llm-cohere \
///       happy_path_tick_live_smoke -- --ignored --nocapture`
#[tokio::test]
#[ignore]
async fn happy_path_tick_live_smoke() {
    if !live_llm_enabled() {
        eprintln!("CORAL_LIVE_LLM!=1; skipping live smoke");
        return;
    }
    if std::env::var("COHERE_API_KEY")
        .map(|v| v.is_empty())
        .unwrap_or(true)
    {
        panic!("COHERE_API_KEY must be set in the environment for the live smoke");
    }

    let client: Arc<dyn ModelClient> = Arc::new(CohereClient::new());
    let decide = LlmDecide::new(client, CompleteOptions::default());
    // Cohere rejects requests whose `messages[]` carries only a system
    // turn ("invalid request: message must be at least 1 token long");
    // seed one trigger so the prompt renderer emits a user message too.
    let session = Session::new(Seed::new(
        Mandate::new(
            "Reply with idle for at least 1000ms.",
            Duration::from_secs(60),
            Some(1),
        ),
        vec![coral_node::trigger::Trigger::ScheduledWake],
        FsIndex::default(),
    ));
    let dec = decide
        .decide(&session)
        .await
        .expect("live cohere decide should succeed");
    let calls = decide.last_tick_calls();
    assert!(!calls.is_empty());
    assert_eq!(calls[0].vendor, Vendor::Cohere);
    assert!(calls[0].latency_ms > 0);
    eprintln!("live cohere decision: {dec:?}; stats: {:?}", calls[0]);
}
