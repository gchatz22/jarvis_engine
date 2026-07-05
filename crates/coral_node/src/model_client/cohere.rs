//! Cohere V2 Chat API implementation of `ModelClient`.
//!
//! Endpoint: `POST https://api.cohere.com/v2/chat`. Auth via the
//! `Authorization: Bearer <token>` header pulled from `COHERE_API_KEY`.
//!
//! # Wire-format quirks vs. the Anthropic adapter
//!
//! 1. Tool definitions are wrapped: `{type: "function", function: {name,
//!    description, parameters}}`. Note `parameters`, not `input_schema`.
//! 2. Cohere's response keeps `tool_use` out of `content` entirely. Tool
//!    calls only appear at `message.tool_calls[]`. We synthesize
//!    `ContentBlock::ToolUse` blocks from those so the trait invariant
//!    (calls visible in both `content` and `tool_calls`) holds.
//! 3. `tool_calls[].function.arguments` is a JSON-encoded **string** on the
//!    wire. We `serde_json::from_str` it; a malformed string is
//!    `ModelError::Parse`.
//! 4. Usage lives at `usage.tokens.input_tokens` / `usage.tokens.output_tokens`
//!    (nested under `tokens`, not flat).
//! 5. `Role::Tool` maps to a real `tool` message with a top-level
//!    `tool_call_id` field — Cohere has a native `tool` role. One Cohere
//!    message is emitted per `ToolResult` block to preserve correlation.
//! 6. `stream: false` is sent explicitly per the docs.

use std::env;
use std::time::Instant;

use serde_json::{json, Value};

use super::{
    effective_model, CallStats, CompleteRequest, CompleteResponse, ContentBlock, ModelClient,
    ModelError, Role, ToolCall, Usage, Vendor,
};

/// Default model identifier. Used when neither `MODEL_ENV` nor
/// `CohereClient::with_model` overrides it.
///
/// `command-a-03-2025` is Cohere's current cost-optimized flagship per the
/// V2 chat API reference docs (verified May 2026).
pub const DEFAULT_MODEL: &str = "command-a-03-2025";

/// `COHERE_MODEL` env var name. Read once at `new()` time; a non-empty
/// value wins over `DEFAULT_MODEL`, an explicit `with_model` call still
/// wins over the env. Lets ops swap models via `.envrc` without recompiling.
pub const MODEL_ENV: &str = "COHERE_MODEL";

/// Cohere V2 chat endpoint. Override only for proxies or recording layers.
pub const DEFAULT_BASE_URL: &str = "https://api.cohere.com/v2/chat";

/// `COHERE_API_KEY` env var name. Read inside `complete` so a missing key
/// surfaces as `ModelError::Auth` at request time.
pub const API_KEY_ENV: &str = "COHERE_API_KEY";

/// HTTP-bound `ModelClient` for Cohere's V2 Chat API.
#[derive(Clone, Debug)]
pub struct CohereClient {
    http: reqwest::Client,
    model: String,
    base_url: String,
}

impl CohereClient {
    /// Build a client with the default model and base URL. The model id
    /// comes from `COHERE_MODEL` if set and non-empty, otherwise
    /// `DEFAULT_MODEL`. `with_model` still overrides whatever this picks.
    pub fn new() -> Self {
        let model = env::var(MODEL_ENV)
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());
        Self {
            http: reqwest::Client::new(),
            model,
            base_url: DEFAULT_BASE_URL.to_string(),
        }
    }

    /// Override the default model.
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    /// Override the base URL. Intended for proxies or recording layers; not
    /// exercised by the default tests.
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Read the configured model id.
    pub fn model(&self) -> &str {
        &self.model
    }
}

impl Default for CohereClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ModelClient for CohereClient {
    async fn complete(&self, req: CompleteRequest) -> Result<CompleteResponse, ModelError> {
        let api_key = env::var(API_KEY_ENV)
            .map_err(|_| ModelError::Auth(format!("{API_KEY_ENV} not set in environment")))?;

        let model = effective_model(&req, &self.model).to_string();
        let body = build_body(&req, &model);
        // Wall-clock around full request + body read, matching the
        // Anthropic adapter so the number is comparable.
        let started = Instant::now();
        let resp = self
            .http
            .post(&self.base_url)
            .header("authorization", format!("Bearer {api_key}"))
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| ModelError::Transport(e.to_string()))?;

        let status = resp.status().as_u16();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ModelError::Transport(e.to_string()))?;
        let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);

        if status != 200 {
            return Err(map_status_error(status, &bytes));
        }
        let parsed = parse_response(&bytes)?;
        let stats = CallStats {
            usage: parsed.usage,
            latency_ms,
            vendor: Vendor::Cohere,
            model,
        };
        Ok(CompleteResponse {
            content: parsed.content,
            tool_calls: parsed.tool_calls,
            usage: parsed.usage,
            stats,
        })
    }
}

/// Output of the pure `parse_response` helper.
///
/// `parse_response` is intentionally vendor-blind to anything not on the
/// wire — it does not know latency (the HTTP call hasn't happened yet from
/// its point of view), the vendor tag, or the model id (those live on the
/// `CohereClient`). `complete` composes a `ParsedComplete` with those
/// extras to mint the public `CompleteResponse` + `CallStats`.
///
/// Crate-private: it's an implementation detail of the parse/complete
/// split, not part of the `ModelClient` surface.
#[derive(Debug, PartialEq)]
pub(crate) struct ParsedComplete {
    pub content: Vec<ContentBlock>,
    pub tool_calls: Vec<ToolCall>,
    pub usage: Usage,
}

/// Build the request body Cohere expects. Pure: no I/O, no env access.
///
/// Each trait `Message` translates to one Cohere `messages[]` entry with
/// the one exception: `Role::Tool` messages emit one Cohere `tool` message
/// per `ToolResult` block they contain, so each carries its own
/// `tool_call_id`. Non-`ToolResult` blocks on a `Role::Tool` message are
/// dropped (the prompt renderer is responsible for well-formed tool turns).
pub fn build_body(req: &CompleteRequest, model: &str) -> Value {
    let mut messages: Vec<Value> = Vec::with_capacity(req.messages.len());

    for msg in &req.messages {
        match msg.role {
            Role::System => {
                messages.push(json!({
                    "role": "system",
                    "content": render_text_content(&msg.content),
                }));
            }
            Role::User => {
                messages.push(json!({
                    "role": "user",
                    "content": render_text_content(&msg.content),
                }));
            }
            Role::Assistant => {
                let mut entry = serde_json::Map::new();
                entry.insert("role".into(), json!("assistant"));

                let mut text_chunks: Vec<&str> = Vec::new();
                let mut tool_calls: Vec<Value> = Vec::new();
                for block in &msg.content {
                    match block {
                        ContentBlock::Text { text } => text_chunks.push(text.as_str()),
                        ContentBlock::ToolUse { id, name, input } => {
                            // `arguments` is a JSON-encoded string on the wire.
                            // `serde_json::to_string` on owned-Value can only fail
                            // if a Map key isn't a string, which Value never produces.
                            let args_str =
                                serde_json::to_string(input).unwrap_or_else(|_| "{}".to_string());
                            tool_calls.push(json!({
                                "id": id,
                                "type": "function",
                                "function": {
                                    "name": name,
                                    "arguments": args_str,
                                },
                            }));
                        }
                        ContentBlock::ToolResult { .. } => {
                            // Tool results don't belong on assistant turns; ignore.
                        }
                    }
                }
                // Cohere requires non-empty `content` OR `tool_calls` on
                // every assistant message (HTTP 400 otherwise). Empty-string
                // `content` counts as empty, so when there's no text we
                // omit the field entirely instead of sending `""`.
                let joined = text_chunks.join("");
                if !joined.is_empty() {
                    entry.insert("content".into(), json!(joined));
                }
                if !tool_calls.is_empty() {
                    entry.insert("tool_calls".into(), Value::Array(tool_calls));
                }
                messages.push(Value::Object(entry));
            }
            Role::Tool => {
                for block in &msg.content {
                    if let ContentBlock::ToolResult {
                        tool_use_id,
                        content,
                    } = block
                    {
                        messages.push(json!({
                            "role": "tool",
                            "tool_call_id": tool_use_id,
                            "content": content,
                        }));
                    }
                }
            }
        }
    }

    let mut body = serde_json::Map::new();
    body.insert("model".into(), json!(model));
    body.insert("stream".into(), json!(false));
    body.insert("max_tokens".into(), json!(req.options.max_tokens));
    if let Some(t) = req.options.temperature {
        body.insert("temperature".into(), json!(t));
    }
    // Cohere V2 defaults `citation_options.mode` to `"ACCURATE"`, which
    // wraps grounded spans in inline `<co>...</co: 0:[N]>` markers inside
    // assistant text. `"OFF"` disables that markup; the engine carries its
    // own provenance via `Output.evidence`.
    body.insert("citation_options".into(), json!({ "mode": "OFF" }));
    body.insert("messages".into(), Value::Array(messages));
    if !req.tools.is_empty() {
        let tools: Vec<Value> = req
            .tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.input_schema,
                    },
                })
            })
            .collect();
        body.insert("tools".into(), Value::Array(tools));
    }
    Value::Object(body)
}

/// Concatenate `Text` blocks into a single string. Cohere's `system` and
/// `user` messages accept either a string or a content-block array; we use
/// the simpler string form. Non-text blocks on these turns are dropped —
/// the prompt renderer is responsible for well-formedness.
fn render_text_content(blocks: &[ContentBlock]) -> String {
    blocks
        .iter()
        .filter_map(|b| match b {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

/// Parse a 200 OK response body into a `ParsedComplete`.
///
/// Vendor-blind on purpose: this function knows nothing about latency, the
/// vendor tag, or the model id. `complete` composes the result with those
/// extras to mint the public `CompleteResponse` + `CallStats`.
///
/// Synthesizes `ContentBlock::ToolUse` blocks from `message.tool_calls[]`
/// so `content` mirrors the Anthropic adapter's invariant (every call is
/// visible in both `content` and the flat `tool_calls` projection). Unknown
/// content-block types (`thinking`, future additions) are tolerated.
pub(crate) fn parse_response(body: &[u8]) -> Result<ParsedComplete, ModelError> {
    let v: Value = serde_json::from_slice(body)
        .map_err(|e| ModelError::Parse(format!("response body is not JSON: {e}")))?;

    let message = v
        .get("message")
        .ok_or_else(|| ModelError::Parse("response missing `message`".into()))?;

    let mut content: Vec<ContentBlock> = Vec::new();
    if let Some(arr) = message.get("content").and_then(|c| c.as_array()) {
        for block in arr {
            let ty = block
                .get("type")
                .and_then(|t| t.as_str())
                .ok_or_else(|| ModelError::Parse("content block missing string `type`".into()))?;
            if ty == "text" {
                let text = block
                    .get("text")
                    .and_then(|t| t.as_str())
                    .ok_or_else(|| ModelError::Parse("text block missing `text`".into()))?
                    .to_string();
                content.push(ContentBlock::Text { text });
            }
            // Tolerate `thinking` and any forward-compat block types.
        }
    }

    let mut tool_calls: Vec<ToolCall> = Vec::new();
    if let Some(arr) = message.get("tool_calls").and_then(|c| c.as_array()) {
        for tc in arr {
            let id = tc
                .get("id")
                .and_then(|t| t.as_str())
                .ok_or_else(|| ModelError::Parse("tool_call missing `id`".into()))?
                .to_string();
            let func = tc
                .get("function")
                .ok_or_else(|| ModelError::Parse("tool_call missing `function`".into()))?;
            let name = func
                .get("name")
                .and_then(|t| t.as_str())
                .ok_or_else(|| ModelError::Parse("tool_call missing `function.name`".into()))?
                .to_string();
            let args_str = func
                .get("arguments")
                .and_then(|t| t.as_str())
                .ok_or_else(|| {
                    ModelError::Parse("tool_call missing `function.arguments` string".into())
                })?;
            // Cohere encodes arguments as a JSON string; decode to a Value
            // to match the trait's `serde_json::Value` shape.
            let arguments: Value = serde_json::from_str(args_str).map_err(|e| {
                ModelError::Parse(format!("tool_call arguments are not valid JSON: {e}"))
            })?;

            content.push(ContentBlock::ToolUse {
                id: id.clone(),
                name: name.clone(),
                input: arguments.clone(),
            });
            tool_calls.push(ToolCall {
                id,
                name,
                arguments,
            });
        }
    }

    let usage_v = v
        .get("usage")
        .ok_or_else(|| ModelError::Parse("response missing `usage`".into()))?;
    let tokens = usage_v
        .get("tokens")
        .ok_or_else(|| ModelError::Parse("usage missing `tokens`".into()))?;
    let input_tokens = tokens
        .get("input_tokens")
        .and_then(|n| n.as_u64())
        .ok_or_else(|| ModelError::Parse("usage.tokens missing `input_tokens`".into()))?
        as u32;
    let output_tokens = tokens
        .get("output_tokens")
        .and_then(|n| n.as_u64())
        .ok_or_else(|| ModelError::Parse("usage.tokens missing `output_tokens`".into()))?
        as u32;

    Ok(ParsedComplete {
        content,
        tool_calls,
        usage: Usage {
            input_tokens,
            output_tokens,
        },
    })
}

/// Map a non-200 HTTP status + raw body to a `ModelError`. Categorization
/// is by status class — the body is included verbatim in the error string.
pub fn map_status_error(status: u16, body: &[u8]) -> ModelError {
    let snippet = String::from_utf8_lossy(body).into_owned();
    match status {
        401 | 403 => ModelError::Auth(format!("HTTP {status}: {snippet}")),
        429 => ModelError::RateLimit(format!("HTTP {status}: {snippet}")),
        500..=599 => ModelError::Transport(format!("HTTP {status}: {snippet}")),
        _ if is_invalid_tool_generation(body) => {
            ModelError::InvalidToolGeneration(format!("HTTP {status}: {snippet}"))
        }
        _ => ModelError::Other(format!("HTTP {status}: {snippet}")),
    }
}

/// True when an error body carries Cohere's `INVALID_TOOL_GENERATION`
/// discriminator — the provider rejected the model's tool call. Matched on
/// the machine-readable `error_type` field rather than the prose `message`
/// so it survives wording changes, and kept separate from `Other` because
/// it is resample-fixable (see [`ModelError::InvalidToolGeneration`]).
fn is_invalid_tool_generation(body: &[u8]) -> bool {
    let Ok(v) = serde_json::from_slice::<Value>(body) else {
        return false;
    };
    v.get("error_type").and_then(Value::as_str) == Some("INVALID_TOOL_GENERATION")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_client::{CompleteOptions, Message, ToolSpec};
    use serde_json::json;

    fn representative_request() -> CompleteRequest {
        CompleteRequest {
            messages: vec![
                Message::system("be terse"),
                Message::user("what's the weather?"),
                Message {
                    role: Role::Assistant,
                    content: vec![
                        ContentBlock::Text {
                            text: "checking".into(),
                        },
                        ContentBlock::ToolUse {
                            id: "call_01".into(),
                            name: "get_weather".into(),
                            input: json!({"location": "SF"}),
                        },
                    ],
                },
                Message {
                    role: Role::Tool,
                    content: vec![ContentBlock::ToolResult {
                        tool_use_id: "call_01".into(),
                        content: "72F".into(),
                    }],
                },
            ],
            tools: vec![ToolSpec {
                name: "get_weather".into(),
                description: "Get current weather for a location".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"location": {"type": "string"}},
                    "required": ["location"],
                }),
            }],
            model: None,
            options: CompleteOptions {
                max_tokens: 256,
                temperature: Some(0.0),
            },
        }
    }

    #[test]
    fn build_body_matches_cohere_wire_shape_golden() {
        let body = build_body(&representative_request(), DEFAULT_MODEL);
        let expected = json!({
            "model": DEFAULT_MODEL,
            "stream": false,
            "max_tokens": 256,
            "temperature": 0.0,
            "citation_options": {"mode": "OFF"},
            "messages": [
                {"role": "system", "content": "be terse"},
                {"role": "user", "content": "what's the weather?"},
                {
                    "role": "assistant",
                    "content": "checking",
                    "tool_calls": [{
                        "id": "call_01",
                        "type": "function",
                        "function": {
                            "name": "get_weather",
                            "arguments": "{\"location\":\"SF\"}",
                        },
                    }],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_01",
                    "content": "72F",
                },
            ],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "description": "Get current weather for a location",
                    "parameters": {
                        "type": "object",
                        "properties": {"location": {"type": "string"}},
                        "required": ["location"],
                    },
                },
            }],
        });
        assert_eq!(body, expected);
    }

    #[test]
    fn build_body_omits_optional_fields_when_unset() {
        let req = CompleteRequest {
            messages: vec![Message::user("hi")],
            tools: vec![],
            model: None,
            options: CompleteOptions {
                max_tokens: 32,
                temperature: None,
            },
        };
        let body = build_body(&req, "m");
        assert!(body.get("temperature").is_none(), "no temperature field");
        assert!(body.get("tools").is_none(), "no tools field");
        assert_eq!(body["max_tokens"], json!(32));
        assert_eq!(body["model"], json!("m"));
        assert_eq!(body["stream"], json!(false));
        // `citation_options` is unconditional, not gated on tools/temperature.
        assert_eq!(body["citation_options"], json!({"mode": "OFF"}));
    }

    #[test]
    fn build_body_disables_cohere_citation_markup() {
        // Cohere V2 defaults `citation_options.mode` to `"ACCURATE"`, which
        // wraps grounded spans in `<co>...</co: 0:[N]>` markers. The adapter
        // must set `mode == "OFF"` on every request to suppress that markup.
        let req = CompleteRequest {
            messages: vec![Message::user("hi")],
            tools: vec![],
            model: None,
            options: CompleteOptions {
                max_tokens: 8,
                temperature: None,
            },
        };
        let body = build_body(&req, "m");
        assert_eq!(
            body["citation_options"],
            json!({"mode": "OFF"}),
            "citation_options.mode must be the exact string \"OFF\"; got body: {body}"
        );
    }

    #[test]
    fn build_body_omits_content_on_assistant_turn_with_tool_calls_only() {
        // Cohere rejects assistant messages with an empty `content` string
        // that also carry tool_calls (HTTP 400). The adapter must omit
        // `content` entirely on a tool-call-only assistant turn.
        let req = CompleteRequest {
            messages: vec![
                Message::user("go"),
                Message {
                    role: Role::Assistant,
                    content: vec![ContentBlock::ToolUse {
                        id: "call_42".into(),
                        name: "echo".into(),
                        input: json!({"msg": "hi"}),
                    }],
                },
                Message {
                    role: Role::Tool,
                    content: vec![ContentBlock::ToolResult {
                        tool_use_id: "call_42".into(),
                        content: "hi".into(),
                    }],
                },
            ],
            tools: vec![],
            model: None,
            options: CompleteOptions {
                max_tokens: 32,
                temperature: None,
            },
        };
        let body = build_body(&req, "m");
        let msgs = body["messages"].as_array().unwrap();
        // user + assistant + tool
        assert_eq!(msgs.len(), 3);
        let assistant = &msgs[1];
        assert_eq!(assistant["role"], json!("assistant"));
        assert!(
            assistant.get("content").is_none(),
            "tool-call-only assistant turn must omit `content`, got: {assistant}"
        );
        assert!(
            assistant.get("tool_calls").is_some(),
            "tool-call-only assistant turn must keep `tool_calls`"
        );
    }

    #[test]
    fn build_body_keeps_content_on_assistant_turn_with_text_and_tool_calls() {
        // Mixed shape (text + tool_calls) keeps `content` populated. Pins
        // the boundary so a future refactor of the omission rule doesn't
        // accidentally drop useful text content.
        let req = CompleteRequest {
            messages: vec![Message {
                role: Role::Assistant,
                content: vec![
                    ContentBlock::Text {
                        text: "calling".into(),
                    },
                    ContentBlock::ToolUse {
                        id: "c1".into(),
                        name: "echo".into(),
                        input: json!({"msg": "hi"}),
                    },
                ],
            }],
            tools: vec![],
            model: None,
            options: CompleteOptions {
                max_tokens: 32,
                temperature: None,
            },
        };
        let body = build_body(&req, "m");
        let assistant = &body["messages"][0];
        assert_eq!(assistant["content"], json!("calling"));
        assert!(assistant.get("tool_calls").is_some());
    }

    #[test]
    fn build_body_omits_content_on_assistant_turn_with_neither_text_nor_tool_calls() {
        // Degenerate input: an assistant turn whose only block is a
        // ToolResult (dropped by the assistant arm). Result must still be
        // valid Cohere wire format; omitting `content` lets the
        // "non-empty content OR tool_calls" check surface server-side
        // rather than as a silent empty-string send.
        let req = CompleteRequest {
            messages: vec![Message {
                role: Role::Assistant,
                content: vec![ContentBlock::ToolResult {
                    tool_use_id: "x".into(),
                    content: "ignored".into(),
                }],
            }],
            tools: vec![],
            model: None,
            options: CompleteOptions {
                max_tokens: 8,
                temperature: None,
            },
        };
        let body = build_body(&req, "m");
        let assistant = &body["messages"][0];
        assert!(assistant.get("content").is_none());
        assert!(assistant.get("tool_calls").is_none());
    }

    #[test]
    fn build_body_emits_one_tool_message_per_tool_result_block() {
        let req = CompleteRequest {
            messages: vec![
                Message::user("go"),
                Message {
                    role: Role::Tool,
                    content: vec![
                        ContentBlock::ToolResult {
                            tool_use_id: "a".into(),
                            content: "1".into(),
                        },
                        ContentBlock::ToolResult {
                            tool_use_id: "b".into(),
                            content: "2".into(),
                        },
                    ],
                },
            ],
            tools: vec![],
            model: None,
            options: CompleteOptions {
                max_tokens: 32,
                temperature: None,
            },
        };
        let body = build_body(&req, "m");
        let msgs = body["messages"].as_array().unwrap();
        // user + 2 tool messages
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[1]["role"], json!("tool"));
        assert_eq!(msgs[1]["tool_call_id"], json!("a"));
        assert_eq!(msgs[2]["role"], json!("tool"));
        assert_eq!(msgs[2]["tool_call_id"], json!("b"));
    }

    #[test]
    fn parse_response_handles_text_only() {
        let raw = br#"{
            "id": "msg_1",
            "finish_reason": "COMPLETE",
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": "hello"}]
            },
            "usage": {"tokens": {"input_tokens": 10, "output_tokens": 5}}
        }"#;
        let r = parse_response(raw).unwrap();
        assert_eq!(
            r.content,
            vec![ContentBlock::Text {
                text: "hello".into()
            }]
        );
        assert!(r.tool_calls.is_empty());
        assert_eq!(r.usage.input_tokens, 10);
        assert_eq!(r.usage.output_tokens, 5);
    }

    #[test]
    fn parse_response_handles_tool_use_only() {
        // Cohere puts tool calls at message.tool_calls (NOT in content); the
        // adapter synthesizes a ContentBlock::ToolUse so the trait invariant
        // (calls visible in both content and tool_calls) holds.
        let raw = br#"{
            "finish_reason": "TOOL_CALL",
            "message": {
                "role": "assistant",
                "content": [],
                "tool_calls": [{
                    "id": "call_99",
                    "type": "function",
                    "function": {
                        "name": "echo",
                        "arguments": "{\"msg\": \"hi\"}"
                    }
                }]
            },
            "usage": {"tokens": {"input_tokens": 7, "output_tokens": 3}}
        }"#;
        let r = parse_response(raw).unwrap();
        assert_eq!(
            r.content,
            vec![ContentBlock::ToolUse {
                id: "call_99".into(),
                name: "echo".into(),
                input: json!({"msg": "hi"}),
            }]
        );
        assert_eq!(
            r.tool_calls,
            vec![ToolCall {
                id: "call_99".into(),
                name: "echo".into(),
                arguments: json!({"msg": "hi"}),
            }]
        );
        assert_eq!(r.usage.input_tokens, 7);
        assert_eq!(r.usage.output_tokens, 3);
    }

    #[test]
    fn parse_response_handles_mixed_text_and_tool_use_and_skips_thinking() {
        let raw = br#"{
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "thinking", "thinking": "ignored future block"},
                    {"type": "text", "text": "let me check"}
                ],
                "tool_calls": [{
                    "id": "t1",
                    "type": "function",
                    "function": {"name": "echo", "arguments": "{\"x\": 1}"}
                }]
            },
            "usage": {"tokens": {"input_tokens": 4, "output_tokens": 9}}
        }"#;
        let r = parse_response(raw).unwrap();
        // 1 text block + 1 synthesized tool_use block (thinking dropped).
        assert_eq!(r.content.len(), 2);
        assert!(matches!(&r.content[0], ContentBlock::Text { text } if text == "let me check"));
        assert!(matches!(&r.content[1], ContentBlock::ToolUse { id, .. } if id == "t1"));
        assert_eq!(r.tool_calls.len(), 1);
        assert_eq!(r.tool_calls[0].id, "t1");
        assert_eq!(r.tool_calls[0].arguments, json!({"x": 1}));
    }

    #[test]
    fn parse_response_rejects_malformed_json() {
        let raw = b"not json {{{";
        let err = parse_response(raw).unwrap_err();
        assert!(matches!(err, ModelError::Parse(_)));
    }

    #[test]
    fn parse_response_rejects_missing_usage() {
        let raw = br#"{"message": {"content": []}}"#;
        let err = parse_response(raw).unwrap_err();
        assert!(matches!(err, ModelError::Parse(_)));
    }

    #[test]
    fn parse_response_rejects_missing_message() {
        let raw = br#"{"usage": {"tokens": {"input_tokens": 1, "output_tokens": 1}}}"#;
        let err = parse_response(raw).unwrap_err();
        assert!(matches!(err, ModelError::Parse(_)));
    }

    #[test]
    fn parse_response_rejects_tool_call_with_invalid_arguments_json() {
        let raw = br#"{
            "message": {
                "content": [],
                "tool_calls": [{
                    "id": "x",
                    "type": "function",
                    "function": {"name": "f", "arguments": "not json {{"}
                }]
            },
            "usage": {"tokens": {"input_tokens": 1, "output_tokens": 1}}
        }"#;
        let err = parse_response(raw).unwrap_err();
        assert!(matches!(err, ModelError::Parse(_)));
        assert!(err.to_string().contains("arguments"));
    }

    #[test]
    fn map_status_401_is_auth() {
        let e = map_status_error(401, b"{\"message\":\"bad key\"}");
        assert!(matches!(e, ModelError::Auth(_)));
        assert!(e.to_string().contains("401"));
    }

    #[test]
    fn map_status_403_is_auth() {
        let e = map_status_error(403, b"forbidden");
        assert!(matches!(e, ModelError::Auth(_)));
    }

    #[test]
    fn map_status_429_is_rate_limit() {
        let e = map_status_error(429, b"too many");
        assert!(matches!(e, ModelError::RateLimit(_)));
        assert!(e.to_string().contains("429"));
    }

    #[test]
    fn map_status_500_is_transport() {
        let e = map_status_error(500, b"oops");
        assert!(matches!(e, ModelError::Transport(_)));
        assert!(e.to_string().contains("500"));
    }

    #[test]
    fn map_status_503_is_transport() {
        let e = map_status_error(503, b"unavailable");
        assert!(matches!(e, ModelError::Transport(_)));
    }

    #[test]
    fn map_status_400_is_other() {
        let e = map_status_error(400, b"{\"message\":\"bad request\"}");
        assert!(matches!(e, ModelError::Other(_)));
        assert!(e.to_string().contains("400"));
        assert!(e.to_string().contains("bad request"));
    }

    #[test]
    fn map_status_422_with_invalid_tool_generation_gets_its_own_variant() {
        // Cohere rejects the model's tool call server-side; the discriminator
        // rides `error_type`, not the prose message. This must NOT collapse
        // into `Other`, or the decide loop can't tell it apart from a
        // permanent 4xx and won't retry it.
        let body = br#"{"error_type":"INVALID_TOOL_GENERATION","id":"x","message":"invalid tool generation"}"#;
        let e = map_status_error(422, body);
        assert!(matches!(e, ModelError::InvalidToolGeneration(_)));
        assert!(e.to_string().contains("422"));
    }

    #[test]
    fn map_status_422_without_the_discriminator_stays_other() {
        // A 422 that isn't an invalid-tool-generation is just another 4xx.
        let e = map_status_error(422, b"{\"message\":\"unprocessable\"}");
        assert!(matches!(e, ModelError::Other(_)));
    }

    #[test]
    fn cohere_client_dyn_dispatch_compiles() {
        // Compile-time: CohereClient implements `ModelClient` and is
        // `Send + Sync`, so it can be stashed behind `Box<dyn ModelClient>`.
        let _: Box<dyn ModelClient> = Box::new(CohereClient::new());
    }

    #[tokio::test]
    async fn complete_returns_auth_error_when_api_key_missing() {
        // `remove_var` is unsafe because it isn't thread-safe with concurrent
        // env reads; safe here because there are no concurrent readers.
        unsafe {
            std::env::remove_var(API_KEY_ENV);
        }
        let client = CohereClient::new();
        let req = CompleteRequest {
            messages: vec![Message::user("hi")],
            tools: vec![],
            model: None,
            options: CompleteOptions {
                max_tokens: 32,
                temperature: None,
            },
        };
        let err = client.complete(req).await.unwrap_err();
        assert!(matches!(err, ModelError::Auth(_)), "got: {err:?}");
    }

    #[test]
    fn new_reads_cohere_model_env_for_unset_empty_and_value() {
        // One sequential test body avoids racing on the process-global env
        // when the harness runs tests concurrently. Empty must behave like
        // unset — `.envrc` defaults often ship as "" and we don't want an
        // empty string sent to the API.
        unsafe {
            std::env::remove_var(MODEL_ENV);
        }
        assert_eq!(
            CohereClient::new().model(),
            DEFAULT_MODEL,
            "unset → default"
        );

        unsafe {
            std::env::set_var(MODEL_ENV, "");
        }
        assert_eq!(
            CohereClient::new().model(),
            DEFAULT_MODEL,
            "empty → default"
        );

        unsafe {
            std::env::set_var(MODEL_ENV, "command-r-plus-08-2024");
        }
        assert_eq!(
            CohereClient::new().model(),
            "command-r-plus-08-2024",
            "value → that value"
        );

        unsafe {
            std::env::remove_var(MODEL_ENV);
        }
    }
}
