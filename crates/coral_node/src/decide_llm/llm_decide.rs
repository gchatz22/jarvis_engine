//! `LlmDecide` — `Decide` impl backed by a `ModelClient`.
//!
//! Per `decide` call: render the session to messages, call
//! `ModelClient::complete` with the decision-tool list, parse the response.
//! On parse failure: append the bad turn plus a corrective `system` message
//! and retry up to [`MAX_DECISION_RETRIES`] times; if every attempt fails,
//! return `Err` (the run loop escalates to `Unhealthy`). Vendor errors
//! bubble immediately without retry.
//!
//! The corrective message uses the `system` role because vendor adapters
//! concatenate all `system` turns into the top-level `system` field, so the
//! correction lands as standing instructions rather than as in-conversation
//! context the model might summarize.

use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tracing::debug;

use crate::decide_llm::prompt;
use crate::decide_llm::schema::{decision_tools, parse_decision, DecisionParseError};
use crate::decision::{Decide, Decision, Session};
use crate::model_client::{
    CallStats, CompleteOptions, CompleteRequest, ContentBlock, Message, ModelClient, ModelError,
    ModelRegistry, Role,
};

/// Number of corrective re-asks performed after the first attempt fails to
/// parse. Total upstream calls per `decide` is therefore at most
/// `1 + MAX_DECISION_RETRIES`.
pub const MAX_DECISION_RETRIES: usize = 1;

/// `Decide` impl that asks a model what to do next.
///
/// `registry` resolves each agent's `Mandate.model` (a `provider/model`
/// name) to the [`ModelClient`] that serves it, so one `LlmDecide` shared
/// across a graph can drive an Anthropic parent over Cohere children.
/// `tick_stats` is the per-tick cost/latency accumulator: one `decide()`
/// call may issue multiple `ModelClient::complete` calls (parse-retry /
/// corrective re-ask), and the accumulator captures every call within that
/// tick and resets at the start of the next `decide`. `Decide::decide` takes
/// `&self`, so storage uses interior mutability via `Mutex` — the lock is
/// held only for `push`/`clear`, never across an `await`. The accumulator is
/// wrapped in an `Arc<Mutex<...>>` so callers can capture a cheap read-only
/// handle (via [`LlmDecide::stats_handle`]) before construction and survive
/// `Agent::run` consuming the `LlmDecide`.
pub struct LlmDecide {
    registry: ModelRegistry,
    options: CompleteOptions,
    tick_stats: Arc<Mutex<Vec<CallStats>>>,
}

impl LlmDecide {
    /// Wire a single-vendor `LlmDecide` against one client and sampling
    /// options. Every agent's model resolves to `client`, ignoring any
    /// provider prefix — the in-process CLI and tests use this. The
    /// multi-provider worker path uses [`LlmDecide::with_registry`]. The
    /// options are reused verbatim for both the initial attempt and the
    /// corrective retry.
    pub fn new(client: Arc<dyn ModelClient>, options: CompleteOptions) -> Self {
        Self::with_registry(ModelRegistry::single(client), options)
    }

    /// Wire an `LlmDecide` against a populated [`ModelRegistry`] so each
    /// agent's `provider/model` resolves to its own client.
    pub fn with_registry(registry: ModelRegistry, options: CompleteOptions) -> Self {
        Self {
            registry,
            options,
            tick_stats: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Aggregate stats across every `complete` call issued during the most
    /// recent `decide()` invocation. Reset at the start of every `decide`,
    /// so reading this between ticks returns the previous tick's totals.
    /// Returns `TickTotals::default()` (all zeros, no calls) before the
    /// first `decide()` runs.
    pub fn last_tick_totals(&self) -> TickTotals {
        let stats = self.tick_stats.lock().expect("tick_stats mutex poisoned");
        TickTotals::from_calls(&stats)
    }

    /// Per-call stats for the most recent `decide()` invocation, in call
    /// order. Useful for assertions that need to inspect individual calls
    /// (e.g. that vendor/model fields are populated correctly).
    pub fn last_tick_calls(&self) -> Vec<CallStats> {
        self.tick_stats
            .lock()
            .expect("tick_stats mutex poisoned")
            .clone()
    }

    /// Clone of the per-tick stats accumulator handle. Callers that need
    /// to read stats after `Agent::run` has consumed the `LlmDecide`
    /// should capture this before construction and read it post-run. The
    /// returned `Arc` shares storage with this `LlmDecide`'s internal
    /// accumulator; the inner vec is updated at the end of every upstream
    /// `complete()` call and cleared at the start of every `decide()`.
    /// Lock the inner mutex only briefly — never across `await`
    /// boundaries — and clone the contents out rather than holding the
    /// guard.
    pub fn stats_handle(&self) -> Arc<Mutex<Vec<CallStats>>> {
        self.tick_stats.clone()
    }
}

/// Sum of one tick's worth of `CallStats`. Tokens accumulate, latency
/// sums (wall-clock budget, not max or mean), and `calls` is the count of
/// upstream `complete` invocations the tick issued. Vendor/model are
/// deliberately not aggregated here — one tick resolves to a single client,
/// so they are constant across the tick and live on the per-call `CallStats`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TickTotals {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub latency_ms: u64,
    pub calls: u32,
}

impl TickTotals {
    pub(crate) fn from_calls(calls: &[CallStats]) -> Self {
        let mut totals = TickTotals::default();
        for c in calls {
            totals.input_tokens = totals.input_tokens.saturating_add(c.usage.input_tokens);
            totals.output_tokens = totals.output_tokens.saturating_add(c.usage.output_tokens);
            totals.latency_ms = totals.latency_ms.saturating_add(c.latency_ms);
            totals.calls = totals.calls.saturating_add(1);
        }
        totals
    }
}

#[async_trait]
impl Decide for LlmDecide {
    async fn decide(&self, session: &Session) -> Result<Decision> {
        let tools = decision_tools();
        // Resolve the agent's `provider/model` to its client and a bare
        // model id. The pair rides every attempt this step, including the
        // corrective retry. An unregistered provider is an operator
        // misconfig surfacing here; the run loop escalates it to Unhealthy.
        let model_name = session.seed.mandate.model.as_deref();
        let (client, model) = self
            .registry
            .resolve(model_name)
            .map_err(|e| anyhow!("resolving model {model_name:?}: {e}"))?;
        // Conversation grows across attempts: original prompt, then for
        // each parse failure an assistant-echo of the bad turn followed by
        // a system-role corrective. The model sees its full failure
        // history, not just the most recent miss.
        let mut messages = prompt::render(session);
        let mut errors: Vec<AttemptFailure> = Vec::new();
        let total_attempts = MAX_DECISION_RETRIES + 1;

        self.tick_stats
            .lock()
            .expect("tick_stats mutex poisoned")
            .clear();

        for attempt in 0..total_attempts {
            let is_last = attempt + 1 == total_attempts;
            let resp = match client
                .complete(CompleteRequest {
                    messages: messages.clone(),
                    tools: tools.clone(),
                    options: self.options.clone(),
                    runtime_tools: Vec::new(),
                    model: model.clone(),
                })
                .await
            {
                Ok(resp) => resp,
                // A rejected tool call is the server-side twin of a parse
                // failure: the provider refused the model's generation, so
                // nothing came back to echo. Fold it into the same bounded
                // retry — a resample under a corrective nudge usually clears
                // it — rather than failing the whole tick on one bad sample.
                Err(ModelError::InvalidToolGeneration(msg)) => {
                    if !is_last {
                        messages.push(Message {
                            role: Role::System,
                            content: vec![ContentBlock::Text {
                                text: rejected_tool_call_corrective_text(),
                            }],
                        });
                    }
                    errors.push(AttemptFailure::ProviderRejectedToolCall(msg));
                    continue;
                }
                Err(e) => return Err(model_err_to_anyhow(e)),
            };

            debug!(
                vendor = resp.stats.vendor.as_str(),
                model = %resp.stats.model,
                input_tokens = resp.stats.usage.input_tokens,
                output_tokens = resp.stats.usage.output_tokens,
                latency_ms = resp.stats.latency_ms,
                attempt,
                "llm_decide: call stats",
            );
            self.tick_stats
                .lock()
                .expect("tick_stats mutex poisoned")
                .push(resp.stats.clone());

            match parse_decision(&resp.tool_calls) {
                Ok(d) => {
                    self.emit_tick_summary();
                    return Ok(d);
                }
                Err(e) => {
                    if !is_last {
                        // When the bad assistant turn contains K `tool_use`
                        // blocks (parallel-tool path), both vendor APIs
                        // require K matching `tool_result` blocks in the
                        // immediately following user turn before they
                        // accept another assistant response. Synthesize
                        // placeholder `tool_result`s here so the retry
                        // request stays schema-valid; the semantic signal
                        // lives in the corrective system message that
                        // follows.
                        messages.push(assistant_echo(&resp.content));
                        let tool_use_ids = tool_use_ids(&resp.content);
                        if !tool_use_ids.is_empty() {
                            messages.push(synthesized_tool_results(&tool_use_ids));
                        }
                        messages.push(Message {
                            role: Role::System,
                            content: vec![ContentBlock::Text {
                                text: corrective_system_text(&e),
                            }],
                        });
                    }
                    errors.push(AttemptFailure::Parse(e));
                }
            }
        }

        // Emit a tick summary even on the failure path so operators see
        // the cost the tick burned before going `Unhealthy`.
        self.emit_tick_summary();
        Err(anyhow!(
            "LlmDecide: no valid decision on all {} attempt(s). {}",
            total_attempts,
            format_attempt_errors(&errors)
        ))
    }
}

impl LlmDecide {
    /// Emit a single tracing event with the aggregate totals for the tick
    /// that just finished. Matches the `debug!` level used elsewhere for
    /// per-tick events.
    fn emit_tick_summary(&self) {
        let totals = self.last_tick_totals();
        debug!(
            calls = totals.calls,
            input_tokens = totals.input_tokens,
            output_tokens = totals.output_tokens,
            latency_ms = totals.latency_ms,
            "llm_decide: tick totals",
        );
    }
}

/// Why one decide attempt failed to yield a usable `Decision`. Either the
/// provider returned tool calls that didn't parse, or it rejected the
/// generation outright (Cohere's 422 `INVALID_TOOL_GENERATION`) — the
/// server-side twin of a parse failure. Both drive the same bounded
/// retry-with-correction; they differ only in the corrective text and in
/// whether there is an assistant turn to echo.
enum AttemptFailure {
    Parse(DecisionParseError),
    ProviderRejectedToolCall(String),
}

impl std::fmt::Display for AttemptFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AttemptFailure::Parse(e) => write!(f, "{e}"),
            AttemptFailure::ProviderRejectedToolCall(msg) => {
                write!(f, "provider rejected tool call: {msg}")
            }
        }
    }
}

/// Render a list of per-attempt failures into a single string for the final
/// `anyhow!` payload. Each entry is prefixed `attempt N` so a reader can
/// correlate it with the upstream call count.
fn format_attempt_errors(errors: &[AttemptFailure]) -> String {
    errors
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let n = i + 1;
            if i == 0 {
                format!("attempt {n}: {e}")
            } else {
                format!("attempt {n} (after corrective system message): {e}")
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// Build the `assistant` turn we replay back to the model on retry, so the
/// corrective message has the model's own bad output to refer to. We echo
/// every content block verbatim — the parser already failed, so trimming
/// or pretty-printing is the wrong layer.
fn assistant_echo(content: &[ContentBlock]) -> Message {
    // A model can return nothing at all. Echoing an empty assistant turn
    // back on retry both drops the "you produced no decision" signal and,
    // on vendors that reject content-less assistant messages (Cohere 400s),
    // makes the recovery request itself unsendable. Stand in a placeholder
    // so the turn is non-empty and the corrective has something to attach to.
    let content = if content.is_empty() {
        vec![ContentBlock::Text {
            text: "(model returned no output)".to_string(),
        }]
    } else {
        content.to_vec()
    };
    Message {
        role: Role::Assistant,
        content,
    }
}

/// Collect the `tool_use.id` of every `ToolUse` block in `content`, in
/// document order. Drives `synthesized_tool_results` on the retry path
/// so each replayed `tool_use` block has a matching `tool_result`.
fn tool_use_ids(content: &[ContentBlock]) -> Vec<String> {
    content
        .iter()
        .filter_map(|b| match b {
            ContentBlock::ToolUse { id, .. } => Some(id.clone()),
            _ => None,
        })
        .collect()
}

/// Synthesize a single tool turn that carries one `tool_result` block
/// per supplied `tool_use.id`. The vendor adapters reshape this into the
/// correct wire form — Anthropic wraps the whole turn as a `user`
/// message with K `tool_result` blocks; Cohere emits K `tool` messages,
/// one per block, each carrying its own `tool_call_id`. The placeholder
/// text is the same across blocks: the corrective system message that
/// follows carries the real "what went wrong" signal, and the per-block
/// content only has to be non-empty to keep the request schema-valid.
fn synthesized_tool_results(ids: &[String]) -> Message {
    Message {
        role: Role::Tool,
        content: ids
            .iter()
            .map(|id| ContentBlock::ToolResult {
                tool_use_id: id.clone(),
                content: "(rejected by parser; see corrective system message)".into(),
            })
            .collect(),
    }
}

/// Phrasing of the corrective system message. Promoted to a function so
/// tests can reference the same source of truth as the renderer.
fn corrective_system_text(err: &DecisionParseError) -> String {
    format!(
        "Your previous tool-use response could not be parsed into a Decision: {err}. \
         Reply by calling exactly one decision tool \
         (`read`, `list`, `search`, `write_output`, `rewrite_fs`, `idle`) \
         OR one or more `call_tool` blocks dispatched together as a single \
         parallel batch, with schema-correct arguments. Do not mix `call_tool` \
         with another decision tool in the same response."
    )
}

/// Corrective for a provider-rejected tool call (e.g. a 422). No assistant
/// turn came back, so this is the only signal replayed before the resample:
/// there is nothing to echo, just the nudge to emit a valid tool call.
fn rejected_tool_call_corrective_text() -> String {
    "The provider rejected your previous response as an invalid tool call. \
     Reply by calling exactly one decision tool \
     (`read`, `list`, `search`, `write_output`, `rewrite_fs`, `idle`) \
     OR one or more `call_tool` blocks dispatched together as a single \
     parallel batch, with schema-correct arguments. Do not mix `call_tool` \
     with another decision tool in the same response."
        .to_string()
}

/// Wrap a typed `ModelError` into the `anyhow::Error` the `Decide` trait
/// returns. Preserves the `ModelError` source so callers that care about
/// the category can downcast.
fn model_err_to_anyhow(err: ModelError) -> anyhow::Error {
    anyhow::Error::new(err)
}

#[cfg(test)]
mod tests {
    //! A test-only `MockModelClient` returns scripted `CompleteResponse`s;
    //! no live HTTP traffic.

    use super::*;
    use crate::decision::{ClaimSeed, FsIndex, Seed, ToolCall as DecisionToolCall};
    use crate::mandate::Mandate;
    use crate::model_client::{CompleteResponse, ToolCall, Usage, Vendor};
    use serde_json::json;
    use std::sync::Mutex;
    use std::time::Duration;

    /// Test-only `ModelClient`. Each call to `complete` pops the next
    /// scripted outcome and returns it. Captures the requests it saw so
    /// tests can assert on the messages the adapter sent.
    struct MockModelClient {
        script: Mutex<Vec<MockOutcome>>,
        seen: Mutex<Vec<CompleteRequest>>,
    }

    /// One scripted outcome: either a successful response or a typed
    /// vendor error to surface verbatim.
    enum MockOutcome {
        Resp(CompleteResponse),
        Err(ModelError),
    }

    impl MockModelClient {
        fn new(script: Vec<MockOutcome>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script),
                seen: Mutex::new(Vec::new()),
            })
        }

        fn seen(&self) -> Vec<CompleteRequest> {
            self.seen.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ModelClient for MockModelClient {
        async fn complete(&self, req: CompleteRequest) -> Result<CompleteResponse, ModelError> {
            self.seen.lock().unwrap().push(req);
            let next = self
                .script
                .lock()
                .unwrap()
                .drain(..1)
                .next()
                .expect("MockModelClient: script exhausted");
            match next {
                MockOutcome::Resp(r) => Ok(r),
                MockOutcome::Err(e) => Err(e),
            }
        }
    }

    /// Zero-valued `CallStats` for mock responses that don't care about
    /// per-call accounting. Tests that care about vendor stamping use
    /// `resp_with_stats` with explicit per-vendor stats instead.
    fn stub_stats() -> CallStats {
        CallStats {
            usage: Usage::default(),
            latency_ms: 0,
            vendor: Vendor::Anthropic,
            model: String::new(),
        }
    }

    /// Build a `CompleteResponse` whose `tool_calls` is the supplied list,
    /// with empty content/usage and stub stats. The parser only inspects
    /// `tool_calls`; stats-aware tests use `resp_with_stats` instead.
    fn resp_with_tool_calls(calls: Vec<ToolCall>) -> CompleteResponse {
        CompleteResponse {
            content: calls
                .iter()
                .map(|c| ContentBlock::ToolUse {
                    id: c.id.clone(),
                    name: c.name.clone(),
                    input: c.arguments.clone(),
                })
                .collect(),
            tool_calls: calls,
            usage: Usage::default(),
            stats: stub_stats(),
        }
    }

    /// Like `resp_with_tool_calls`, but with a specific `CallStats` block.
    /// Lets accumulator tests assert on the totals math without depending
    /// on a real wall-clock measurement.
    fn resp_with_stats(calls: Vec<ToolCall>, stats: CallStats) -> CompleteResponse {
        let usage = stats.usage;
        CompleteResponse {
            content: calls
                .iter()
                .map(|c| ContentBlock::ToolUse {
                    id: c.id.clone(),
                    name: c.name.clone(),
                    input: c.arguments.clone(),
                })
                .collect(),
            tool_calls: calls,
            usage,
            stats,
        }
    }

    fn good_idle_call() -> ToolCall {
        ToolCall {
            id: "toolu_idle".into(),
            name: "idle".into(),
            arguments: json!({"next_after": 1000}),
        }
    }

    fn good_call_tool() -> ToolCall {
        ToolCall {
            id: "toolu_ct".into(),
            name: "call_tool".into(),
            arguments: json!({
                "name": "echo",
                "args": {"msg": "hi"},
                "claim_seed": "seed-1"
            }),
        }
    }

    fn malformed_unknown_tool() -> ToolCall {
        ToolCall {
            id: "toolu_bad".into(),
            name: "send_email".into(),
            arguments: json!({"to": "ops@example.com"}),
        }
    }

    fn empty_session() -> Session {
        Session::new(Seed::new(
            Mandate::new("test", Duration::from_secs(1), Some(1)),
            vec![],
            FsIndex::default(),
        ))
    }

    #[tokio::test]
    async fn first_attempt_success_returns_decision_without_retry() {
        let mock = MockModelClient::new(vec![MockOutcome::Resp(resp_with_tool_calls(vec![
            good_idle_call(),
        ]))]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let dec = decide.decide(&empty_session()).await.unwrap();
        assert_eq!(
            dec,
            Decision::Idle {
                next_after: Duration::from_millis(1000),
            }
        );
        // No retry → exactly one upstream call.
        assert_eq!(mock.seen().len(), 1);
    }

    #[tokio::test]
    async fn decide_threads_mandate_model_onto_every_request() {
        // First response fails to parse, forcing a corrective retry, so the
        // assertion covers BOTH the initial request and the retry — the
        // per-agent model must ride on every upstream call within a tick.
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(resp_with_tool_calls(vec![malformed_unknown_tool()])),
            MockOutcome::Resp(resp_with_tool_calls(vec![good_idle_call()])),
        ]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let mut session = empty_session();
        session.seed.mandate.model = Some("claude-opus-4-8".into());
        decide.decide(&session).await.unwrap();

        let seen = mock.seen();
        assert_eq!(seen.len(), 2, "expected initial + corrective request");
        for (i, req) in seen.iter().enumerate() {
            assert_eq!(
                req.model.as_deref(),
                Some("claude-opus-4-8"),
                "mandate.model must ride on request {i}, got {:?}",
                req.model
            );
        }
    }

    #[tokio::test]
    async fn decide_routes_qualified_model_to_its_provider_client() {
        let anthropic = MockModelClient::new(vec![MockOutcome::Resp(resp_with_tool_calls(vec![
            good_idle_call(),
        ]))]);
        let cohere = MockModelClient::new(vec![MockOutcome::Resp(resp_with_tool_calls(vec![
            good_idle_call(),
        ]))]);
        let registry = ModelRegistry::new(
            [
                (
                    "anthropic".to_string(),
                    anthropic.clone() as Arc<dyn ModelClient>,
                ),
                ("cohere".to_string(), cohere.clone() as Arc<dyn ModelClient>),
            ],
            "anthropic",
        )
        .unwrap();
        let decide = LlmDecide::with_registry(registry, CompleteOptions::default());

        let mut session = empty_session();
        session.seed.mandate.model = Some("cohere/command-a".into());
        decide.decide(&session).await.unwrap();

        // The cohere prefix routes to the cohere client, carrying the bare
        // model id; the anthropic client is never touched.
        assert_eq!(anthropic.seen().len(), 0, "anthropic must not be called");
        let seen = cohere.seen();
        assert_eq!(seen.len(), 1, "cohere client should serve the request");
        assert_eq!(seen[0].model.as_deref(), Some("command-a"));
    }

    #[tokio::test]
    async fn decide_errors_on_unknown_provider() {
        let anthropic = MockModelClient::new(vec![MockOutcome::Resp(resp_with_tool_calls(vec![
            good_idle_call(),
        ]))]);
        let registry = ModelRegistry::new(
            [(
                "anthropic".to_string(),
                anthropic.clone() as Arc<dyn ModelClient>,
            )],
            "anthropic",
        )
        .unwrap();
        let decide = LlmDecide::with_registry(registry, CompleteOptions::default());

        let mut session = empty_session();
        session.seed.mandate.model = Some("local/llama-3".into());
        let err = decide.decide(&session).await.unwrap_err();
        assert!(
            err.to_string().contains("unknown provider `local`"),
            "expected unknown-provider error, got: {err}"
        );
        assert_eq!(
            anthropic.seen().len(),
            0,
            "no client should be called when resolution fails"
        );
    }

    #[tokio::test]
    async fn decide_leaves_request_model_none_when_mandate_omits_it() {
        // `empty_bundle`'s mandate has `model: None` ⇒ the adapter falls back
        // to its configured default (no per-request override on the wire).
        let mock = MockModelClient::new(vec![MockOutcome::Resp(resp_with_tool_calls(vec![
            good_idle_call(),
        ]))]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());
        decide.decide(&empty_session()).await.unwrap();
        assert_eq!(mock.seen()[0].model, None);
    }

    #[tokio::test]
    async fn parse_failure_then_recovery_succeeds_with_corrective_system_message() {
        let mock = MockModelClient::new(vec![
            // 1st attempt: model picks an unknown tool name.
            MockOutcome::Resp(resp_with_tool_calls(vec![malformed_unknown_tool()])),
            // 2nd attempt (after corrective): valid call_tool decision.
            MockOutcome::Resp(resp_with_tool_calls(vec![good_call_tool()])),
        ]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let dec = decide.decide(&empty_session()).await.unwrap();
        assert_eq!(
            dec,
            Decision::CallTools {
                calls: vec![DecisionToolCall::with_tool_use_id(
                    "echo",
                    json!({"msg": "hi"}),
                    ClaimSeed::new("seed-1"),
                    "toolu_ct",
                )]
            }
        );

        let seen = mock.seen();
        assert_eq!(seen.len(), 2, "expected exactly two upstream calls");

        // The retry must replay the assistant's bad turn, follow it with
        // matching `tool_result` blocks, and close with the corrective
        // system message.
        let retry = &seen[1];
        let last = retry.messages.last().expect("retry has messages");
        assert_eq!(last.role, Role::System);
        let last_text = match last.content.as_slice() {
            [ContentBlock::Text { text }] => text.as_str(),
            _ => panic!("corrective should be a single text block"),
        };
        assert!(
            last_text.contains("could not be parsed"),
            "corrective text should describe the parse failure, got: {last_text}"
        );

        // Second-to-last message is the synthesized tool turn with one
        // `tool_result` block per `tool_use` in the bad assistant echo.
        let tool_turn = &retry.messages[retry.messages.len() - 2];
        assert_eq!(tool_turn.role, Role::Tool);
        let result_ids: Vec<&str> = tool_turn
            .content
            .iter()
            .filter_map(|b| match b {
                ContentBlock::ToolResult { tool_use_id, .. } => Some(tool_use_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(result_ids, vec!["toolu_bad"]);

        let echoed = &retry.messages[retry.messages.len() - 3];
        assert_eq!(echoed.role, Role::Assistant);
        assert!(matches!(
            echoed.content[0],
            ContentBlock::ToolUse { ref name, .. } if name == "send_email"
        ));
    }

    /// When the bad assistant turn contains K parallel `tool_use` blocks,
    /// the retry must synthesize K matching `tool_result` blocks so the
    /// next request stays schema-valid against both vendor APIs.
    #[tokio::test]
    async fn parse_retry_synthesizes_one_tool_result_per_parallel_tool_use_block() {
        // First attempt: a malformed parallel batch (mixed shape) the
        // parser rejects. Two `tool_use` blocks → two `tool_result`
        // blocks must appear in the retry.
        let bad = vec![
            ToolCall {
                id: "toolu_one".into(),
                name: "call_tool".into(),
                arguments: json!({
                    "name": "echo",
                    "args": {},
                    "claim_seed": "s1",
                }),
            },
            ToolCall {
                id: "toolu_two".into(),
                name: "retire".into(),
                arguments: json!({"reason": "stop"}),
            },
        ];
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(resp_with_tool_calls(bad)),
            MockOutcome::Resp(resp_with_tool_calls(vec![good_idle_call()])),
        ]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());
        decide.decide(&empty_session()).await.unwrap();

        let seen = mock.seen();
        assert_eq!(seen.len(), 2);
        let retry = &seen[1];
        // Find the Tool message in the retry payload.
        let tool_turn = retry
            .messages
            .iter()
            .find(|m| m.role == Role::Tool)
            .expect("retry must include a tool turn for the parallel tool_use ids");
        let result_ids: Vec<&str> = tool_turn
            .content
            .iter()
            .filter_map(|b| match b {
                ContentBlock::ToolResult { tool_use_id, .. } => Some(tool_use_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            result_ids,
            vec!["toolu_one", "toolu_two"],
            "every replayed `tool_use.id` needs a paired `tool_result`",
        );
    }

    #[tokio::test]
    async fn parse_failure_on_both_attempts_returns_err() {
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(resp_with_tool_calls(vec![malformed_unknown_tool()])),
            // 2nd attempt: still bad — different malformed payload.
            MockOutcome::Resp(resp_with_tool_calls(vec![ToolCall {
                id: "toolu_bad2".into(),
                name: "retire".into(),
                arguments: json!({}), // missing required `reason` field
            }])),
        ]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let err = decide.decide(&empty_session()).await.unwrap_err();
        let s = err.to_string();
        // The message must enumerate every attempt so an operator reading
        // a log can see how the model failed at each step. We pin the
        // exact attempt count so the loop refactor stays honest.
        let total_attempts = MAX_DECISION_RETRIES + 1;
        assert!(
            s.contains(&format!("all {total_attempts} attempt")),
            "error should report total attempt count, got: {s}"
        );
        for n in 1..=total_attempts {
            assert!(
                s.contains(&format!("attempt {n}")),
                "error should reference attempt {n}, got: {s}"
            );
        }
        assert_eq!(mock.seen().len(), total_attempts);
    }

    #[tokio::test]
    async fn parse_failure_when_model_returns_no_tool_calls_triggers_retry() {
        // Defensive: a model could reply text-only (no tool_calls). The
        // schema parser surfaces that as `NoCalls`, which is a parse-style
        // error and must enter the same retry path.
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(CompleteResponse {
                content: vec![ContentBlock::Text {
                    text: "I think we should call echo.".into(),
                }],
                tool_calls: vec![],
                usage: Usage::default(),
                stats: stub_stats(),
            }),
            MockOutcome::Resp(resp_with_tool_calls(vec![good_idle_call()])),
        ]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let dec = decide.decide(&empty_session()).await.unwrap();
        assert_eq!(
            dec,
            Decision::Idle {
                next_after: Duration::from_millis(1000),
            }
        );
        assert_eq!(mock.seen().len(), 2);
    }

    #[tokio::test]
    async fn empty_model_response_is_replayed_as_a_non_empty_assistant_turn() {
        // A model can return a wholly empty response — no text, no tool
        // calls. The parser surfaces `NoCalls`, and the retry echoes the
        // model's turn back. If that echo is empty, Cohere rejects the
        // recovery request itself (HTTP 400: assistant messages need
        // content or tool_calls), so the retry that was meant to recover
        // instead kills the workflow. The echo must stand in a placeholder.
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(CompleteResponse {
                content: vec![],
                tool_calls: vec![],
                usage: Usage::default(),
                stats: stub_stats(),
            }),
            MockOutcome::Resp(resp_with_tool_calls(vec![good_idle_call()])),
        ]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let dec = decide.decide(&empty_session()).await.unwrap();
        assert_eq!(
            dec,
            Decision::Idle {
                next_after: Duration::from_millis(1000),
            }
        );

        let seen = mock.seen();
        assert_eq!(seen.len(), 2);
        let echoed = seen[1]
            .messages
            .iter()
            .find(|m| m.role == Role::Assistant)
            .expect("retry must replay the model's (empty) assistant turn");
        let text: String = echoed
            .content
            .iter()
            .filter_map(|b| match b {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert!(
            !text.is_empty(),
            "replayed assistant turn must carry non-empty content, else the vendor rejects the recovery request"
        );
    }

    #[tokio::test]
    async fn invalid_tool_generation_folds_into_bounded_retry_with_correction() {
        // The provider rejects the model's tool call (Cohere 422). Nothing
        // comes back to echo, so the loop replays only a corrective system
        // message and resamples — a rejected call is the server-side twin of
        // a parse failure, not a fatal error that should kill the tick.
        let mock = MockModelClient::new(vec![
            MockOutcome::Err(ModelError::InvalidToolGeneration(
                "HTTP 422: INVALID_TOOL_GENERATION".into(),
            )),
            MockOutcome::Resp(resp_with_tool_calls(vec![good_idle_call()])),
        ]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let dec = decide.decide(&empty_session()).await.unwrap();
        assert_eq!(
            dec,
            Decision::Idle {
                next_after: Duration::from_millis(1000),
            }
        );

        let seen = mock.seen();
        assert_eq!(
            seen.len(),
            2,
            "the rejected call must be retried, not fatal"
        );
        let retry = &seen[1];
        let has_corrective = retry.messages.iter().any(|m| {
            m.role == Role::System
                && m.content
                    .iter()
                    .any(|b| matches!(b, ContentBlock::Text { text } if text.contains("rejected")))
        });
        assert!(
            has_corrective,
            "retry must carry the rejected-tool-call corrective"
        );
        // The fold appends exactly the corrective and echoes no assistant
        // turn (there was none to echo).
        assert_eq!(
            retry.messages.len(),
            seen[0].messages.len() + 1,
            "fold appends only the corrective system message, not an echo"
        );
    }

    #[tokio::test]
    async fn invalid_tool_generation_on_every_attempt_returns_bounded_err() {
        // Even if every sample is rejected, the loop gives up after its
        // bounded attempts rather than hanging or wedging under retry — the
        // child fails this tick and lives to wake again.
        let total_attempts = MAX_DECISION_RETRIES + 1;
        let script: Vec<MockOutcome> = (0..total_attempts)
            .map(|_| MockOutcome::Err(ModelError::InvalidToolGeneration("HTTP 422".into())))
            .collect();
        let mock = MockModelClient::new(script);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let err = decide.decide(&empty_session()).await.unwrap_err();
        let s = err.to_string();
        assert!(
            s.contains(&format!("all {total_attempts} attempt")),
            "got: {s}"
        );
        assert!(s.contains("provider rejected tool call"), "got: {s}");
        assert_eq!(
            mock.seen().len(),
            total_attempts,
            "attempts must be bounded, not unbounded"
        );
    }

    #[tokio::test]
    async fn vendor_transport_error_bubbles_immediately_without_retry() {
        let mock = MockModelClient::new(vec![MockOutcome::Err(ModelError::Transport(
            "DNS failure".into(),
        ))]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let err = decide.decide(&empty_session()).await.unwrap_err();
        // Source chain must preserve the `ModelError` for callers that
        // want to discriminate by category.
        let model_err = err
            .downcast_ref::<ModelError>()
            .expect("ModelError preserved in source chain");
        assert!(matches!(model_err, ModelError::Transport(_)));
        // No retry attempted on vendor error.
        assert_eq!(mock.seen().len(), 1);
    }

    #[tokio::test]
    async fn vendor_rate_limit_on_retry_bubbles() {
        // 1st: parse fails. 2nd: rate-limited. We must surface the
        // rate-limit error rather than swallowing it.
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(resp_with_tool_calls(vec![malformed_unknown_tool()])),
            MockOutcome::Err(ModelError::RateLimit("slow down".into())),
        ]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let err = decide.decide(&empty_session()).await.unwrap_err();
        let model_err = err
            .downcast_ref::<ModelError>()
            .expect("ModelError preserved");
        assert!(matches!(model_err, ModelError::RateLimit(_)));
        assert_eq!(mock.seen().len(), 2);
    }

    #[tokio::test]
    async fn first_attempt_request_carries_decision_tools() {
        let mock = MockModelClient::new(vec![MockOutcome::Resp(resp_with_tool_calls(vec![
            good_idle_call(),
        ]))]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());
        let _ = decide.decide(&empty_session()).await.unwrap();

        let seen = mock.seen();
        let req = &seen[0];
        let names: Vec<&str> = req.tools.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"call_tool"));
        assert!(names.contains(&"write_output"));
        assert!(names.contains(&"rewrite_fs"));
        assert!(names.contains(&"idle"));
        // Persistence is universal: the model is never offered a
        // self-terminate tool.
        assert!(!names.contains(&"retire"));
    }

    #[tokio::test]
    async fn first_attempt_request_messages_match_prompt_render() {
        let session = empty_session();
        let expected_messages = prompt::render(&session);

        let mock = MockModelClient::new(vec![MockOutcome::Resp(resp_with_tool_calls(vec![
            good_idle_call(),
        ]))]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());
        let _ = decide.decide(&session).await.unwrap();

        let seen = mock.seen();
        assert_eq!(seen[0].messages, expected_messages);
    }

    #[tokio::test]
    async fn retry_does_not_blow_away_write_output_evidence_correlation() {
        // Sanity: when the corrective fixes the issue and the second
        // attempt is `write_output`, the parsed `Decision` carries the
        // citation path verbatim. This pins one of the more error-prone
        // variants through the retry path.
        let cite = "evidence/the-answer-abcdef00.json";
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(resp_with_tool_calls(vec![malformed_unknown_tool()])),
            MockOutcome::Resp(resp_with_tool_calls(vec![ToolCall {
                id: "toolu_emit".into(),
                name: "write_output".into(),
                arguments: json!({
                    "body": "the answer",
                    "citations": [cite],
                }),
            }])),
        ]);
        let decide = LlmDecide::new(mock, CompleteOptions::default());

        let dec = decide.decide(&empty_session()).await.unwrap();
        match dec {
            Decision::WriteOutput { body, citations } => {
                assert_eq!(body, "the answer");
                assert_eq!(citations, vec![cite.to_string()]);
            }
            other => panic!("expected WriteOutput, got {other:?}"),
        }
    }

    // ---------- cost + latency accounting ------------------------------

    fn anthropic_stats(input: u32, output: u32, latency_ms: u64) -> CallStats {
        CallStats {
            usage: Usage {
                input_tokens: input,
                output_tokens: output,
            },
            latency_ms,
            vendor: Vendor::Anthropic,
            model: "claude-haiku-4-5".into(),
        }
    }

    fn cohere_stats(input: u32, output: u32, latency_ms: u64) -> CallStats {
        CallStats {
            usage: Usage {
                input_tokens: input,
                output_tokens: output,
            },
            latency_ms,
            vendor: Vendor::Cohere,
            model: "command-a-03-2025".into(),
        }
    }

    #[tokio::test]
    async fn tick_totals_default_zero_before_first_decide() {
        let mock = MockModelClient::new(vec![]);
        let decide = LlmDecide::new(mock, CompleteOptions::default());
        assert_eq!(decide.last_tick_totals(), TickTotals::default());
        assert!(decide.last_tick_calls().is_empty());
    }

    #[tokio::test]
    async fn tick_totals_single_call_equal_call_stats() {
        let stats = anthropic_stats(11, 7, 42);
        let mock = MockModelClient::new(vec![MockOutcome::Resp(resp_with_stats(
            vec![good_idle_call()],
            stats.clone(),
        ))]);
        let decide = LlmDecide::new(mock, CompleteOptions::default());
        decide.decide(&empty_session()).await.unwrap();

        let totals = decide.last_tick_totals();
        assert_eq!(totals.calls, 1);
        assert_eq!(totals.input_tokens, stats.usage.input_tokens);
        assert_eq!(totals.output_tokens, stats.usage.output_tokens);
        assert_eq!(totals.latency_ms, stats.latency_ms);

        let calls = decide.last_tick_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0], stats);
    }

    #[tokio::test]
    async fn tick_totals_multi_call_sum_correctly() {
        // Two upstream calls in one tick — first is a parse failure that
        // forces a retry, second succeeds. Totals must cover both.
        let s1 = anthropic_stats(100, 25, 350);
        let s2 = anthropic_stats(180, 12, 410);
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(resp_with_stats(vec![malformed_unknown_tool()], s1.clone())),
            MockOutcome::Resp(resp_with_stats(vec![good_idle_call()], s2.clone())),
        ]);
        let decide = LlmDecide::new(mock, CompleteOptions::default());
        decide.decide(&empty_session()).await.unwrap();

        let totals = decide.last_tick_totals();
        assert_eq!(totals.calls, 2);
        assert_eq!(totals.input_tokens, 280);
        assert_eq!(totals.output_tokens, 37);
        assert_eq!(totals.latency_ms, 760);

        let calls = decide.last_tick_calls();
        assert_eq!(calls, vec![s1, s2]);
    }

    #[tokio::test]
    async fn tick_totals_reset_between_ticks() {
        // First decide: one call with stats s1. Second decide on the same
        // LlmDecide: one call with stats s2. After the second tick, totals
        // must reflect *only* s2 — the first tick's totals are gone.
        let s1 = anthropic_stats(50, 5, 100);
        let s2 = anthropic_stats(33, 9, 250);
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(resp_with_stats(vec![good_idle_call()], s1.clone())),
            MockOutcome::Resp(resp_with_stats(vec![good_idle_call()], s2.clone())),
        ]);
        let decide = LlmDecide::new(mock, CompleteOptions::default());

        decide.decide(&empty_session()).await.unwrap();
        let after_first = decide.last_tick_totals();
        assert_eq!(after_first.calls, 1);
        assert_eq!(after_first.input_tokens, s1.usage.input_tokens);

        decide.decide(&empty_session()).await.unwrap();
        let after_second = decide.last_tick_totals();
        assert_eq!(after_second.calls, 1, "totals reset between ticks");
        assert_eq!(after_second.input_tokens, s2.usage.input_tokens);
        assert_eq!(after_second.output_tokens, s2.usage.output_tokens);
        assert_eq!(after_second.latency_ms, s2.latency_ms);
    }

    #[tokio::test]
    async fn tick_totals_capture_stats_even_when_all_attempts_fail_to_parse() {
        // When parsing fails on every attempt the call still happened on
        // the wire; the operator log must see the cost even though decide
        // returns Err. Both attempts contribute to the totals.
        let s1 = anthropic_stats(20, 3, 80);
        let s2 = anthropic_stats(40, 4, 90);
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(resp_with_stats(vec![malformed_unknown_tool()], s1.clone())),
            MockOutcome::Resp(resp_with_stats(
                vec![ToolCall {
                    id: "toolu_bad2".into(),
                    name: "retire".into(),
                    arguments: json!({}),
                }],
                s2.clone(),
            )),
        ]);
        let decide = LlmDecide::new(mock, CompleteOptions::default());
        let err = decide.decide(&empty_session()).await.unwrap_err();
        assert!(err.to_string().contains("no valid decision"));

        let totals = decide.last_tick_totals();
        assert_eq!(totals.calls, 2);
        assert_eq!(totals.input_tokens, 60);
        assert_eq!(totals.output_tokens, 7);
        assert_eq!(totals.latency_ms, 170);
    }

    #[tokio::test]
    async fn tick_totals_carry_vendor_and_model_for_both_vendors() {
        // The accumulator preserves per-call vendor and model fields so
        // cost can be attributed by provider. Cover both sides of the
        // `Vendor` enum.
        for stats in [anthropic_stats(7, 2, 50), cohere_stats(12, 4, 75)] {
            let mock = MockModelClient::new(vec![MockOutcome::Resp(resp_with_stats(
                vec![good_idle_call()],
                stats.clone(),
            ))]);
            let decide = LlmDecide::new(mock, CompleteOptions::default());
            decide.decide(&empty_session()).await.unwrap();
            let calls = decide.last_tick_calls();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].vendor, stats.vendor);
            assert_eq!(calls[0].model, stats.model);
            assert_eq!(calls[0].usage, stats.usage);
            assert_eq!(calls[0].latency_ms, stats.latency_ms);
        }
    }

    #[cfg(feature = "llm-anthropic")]
    #[test]
    fn anthropic_parse_response_carries_tokens_in_parsed_complete() {
        // The pure `parse_response` returns a vendor-private
        // `ParsedComplete { content, tool_calls, usage }` — no `stats`,
        // because latency/vendor/model are not on the wire and live on
        // `complete`. This pins the contract the vendor adapter depends
        // on.
        let raw = br#"{
            "content": [{"type": "text", "text": "hi"}],
            "usage": {"input_tokens": 21, "output_tokens": 8}
        }"#;
        let r = crate::model_client::anthropic::parse_response(raw).unwrap();
        assert_eq!(r.usage.input_tokens, 21);
        assert_eq!(r.usage.output_tokens, 8);
        assert_eq!(r.content, vec![ContentBlock::Text { text: "hi".into() }]);
        assert!(r.tool_calls.is_empty());
    }

    #[cfg(feature = "llm-cohere")]
    #[test]
    fn cohere_parse_response_carries_tokens_in_parsed_complete() {
        let raw = br#"{
            "message": {"role": "assistant", "content": [{"type": "text", "text": "hi"}]},
            "usage": {"tokens": {"input_tokens": 33, "output_tokens": 14}}
        }"#;
        let r = crate::model_client::cohere::parse_response(raw).unwrap();
        assert_eq!(r.usage.input_tokens, 33);
        assert_eq!(r.usage.output_tokens, 14);
        assert_eq!(r.content, vec![ContentBlock::Text { text: "hi".into() }]);
        assert!(r.tool_calls.is_empty());
    }
}
