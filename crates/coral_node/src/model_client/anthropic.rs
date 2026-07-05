//! Anthropic Messages API implementation of `ModelClient`.
//!
//! Endpoint: `POST https://api.anthropic.com/v1/messages`. Auth via the
//! `x-api-key` header pulled from `ANTHROPIC_API_KEY`. Wire-format work
//! is split into pure `build_body` / `parse_response` / `map_status_error`
//! helpers so tests exercise the seams without making live HTTP calls.
//!
//! Role mapping: `system` turns concatenate into the top-level `system`
//! field; `user`/`assistant` pass through with content rewritten; `tool`
//! turns become `user` messages with a `tool_result` block, since
//! Anthropic's role system has no `tool` slot.

use std::env;
use std::time::Instant;

use serde_json::{json, Value};

use super::{
    effective_model, CallStats, CompleteRequest, CompleteResponse, ContentBlock, ModelClient,
    ModelError, Role, ToolCall, Usage, Vendor,
};

/// Default model identifier. Used when neither `MODEL_ENV` nor
/// `AnthropicClient::with_model` overrides it.
pub const DEFAULT_MODEL: &str = "claude-haiku-4-5";

/// `ANTHROPIC_MODEL` env var name. Read once at `new()` time; a non-empty
/// value wins over `DEFAULT_MODEL`, an explicit `with_model` call still
/// wins over the env. Lets ops swap models via `.envrc` without recompiling.
pub const MODEL_ENV: &str = "ANTHROPIC_MODEL";

/// Anthropic Messages API base URL. The impl POSTs directly to this URL;
/// override only if a proxy is in front of the public endpoint.
pub const DEFAULT_BASE_URL: &str = "https://api.anthropic.com/v1/messages";

/// Anthropic versioning header. Pinned to the published stable revision.
pub const ANTHROPIC_VERSION: &str = "2023-06-01";

/// `ANTHROPIC_API_KEY` env var name. Pulled inside `complete` so a missing
/// key surfaces as `ModelError::Auth` at request time rather than panicking
/// in the constructor.
pub const API_KEY_ENV: &str = "ANTHROPIC_API_KEY";

/// HTTP-bound `ModelClient` for Anthropic's Messages API.
///
/// `model` and `base_url` are configurable to allow swapping in a different
/// model (`claude-sonnet-*`, etc.) without code changes and to point tests
/// at a recording proxy if one ever lands. Neither is on the trait — both
/// are vendor-specific knobs the runtime configures per-instance.
#[derive(Clone, Debug)]
pub struct AnthropicClient {
    http: reqwest::Client,
    model: String,
    base_url: String,
}

impl AnthropicClient {
    /// Build a client with the default model and base URL. The model id
    /// comes from `ANTHROPIC_MODEL` if set and non-empty, otherwise
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

    /// Override the default model (e.g. `"claude-sonnet-4-5"`).
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

impl Default for AnthropicClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ModelClient for AnthropicClient {
    async fn complete(&self, req: CompleteRequest) -> Result<CompleteResponse, ModelError> {
        let api_key = env::var(API_KEY_ENV)
            .map_err(|_| ModelError::Auth(format!("{API_KEY_ENV} not set in environment")))?;

        let model = effective_model(&req, &self.model).to_string();
        let body = build_body(&req, &model);
        // End-to-end wall time including body read, matching wire observation.
        let started = Instant::now();
        let resp = self
            .http
            .post(&self.base_url)
            .header("x-api-key", api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
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
            vendor: Vendor::Anthropic,
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
/// `AnthropicClient`). `complete` composes a `ParsedComplete` with those
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

/// Build the request body Anthropic expects. Pure: no I/O, no env access.
///
/// Returns the JSON object that becomes the `POST` body. `system` turns
/// are pulled into the top-level `system` field (concatenated with `\n\n`
/// if multiple are present); `tool` turns are rewrapped as `user` messages
/// carrying a `tool_result` content block.
pub fn build_body(req: &CompleteRequest, model: &str) -> Value {
    let mut system_chunks: Vec<String> = Vec::new();
    let mut messages: Vec<Value> = Vec::with_capacity(req.messages.len());

    for msg in &req.messages {
        match msg.role {
            Role::System => {
                for block in &msg.content {
                    if let ContentBlock::Text { text } = block {
                        system_chunks.push(text.clone());
                    }
                    // Anthropic's system field is plain text; non-text blocks
                    // on a system turn are dropped. Upstream renderer is
                    // responsible for well-formed system content.
                }
            }
            Role::User => {
                messages.push(json!({
                    "role": "user",
                    "content": render_blocks(&msg.content),
                }));
            }
            Role::Assistant => {
                messages.push(json!({
                    "role": "assistant",
                    "content": render_blocks(&msg.content),
                }));
            }
            Role::Tool => {
                messages.push(json!({
                    "role": "user",
                    "content": render_blocks(&msg.content),
                }));
            }
        }
    }

    let mut body = serde_json::Map::new();
    body.insert("model".into(), json!(model));
    body.insert("max_tokens".into(), json!(req.options.max_tokens));
    if let Some(t) = req.options.temperature {
        body.insert("temperature".into(), json!(t));
    }
    if !system_chunks.is_empty() {
        body.insert("system".into(), json!(system_chunks.join("\n\n")));
    }
    body.insert("messages".into(), Value::Array(messages));
    if !req.tools.is_empty() {
        let tools: Vec<Value> = req
            .tools
            .iter()
            .map(|t| {
                json!({
                    "name": t.name,
                    "description": t.description,
                    "input_schema": t.input_schema,
                })
            })
            .collect();
        body.insert("tools".into(), Value::Array(tools));
        // `tool_choice` is intentionally omitted so the API default
        // (`auto`, parallel tools enabled) applies.
    }
    Value::Object(body)
}

/// Translate trait-shape content blocks into Anthropic wire-shape blocks.
///
/// `Text` and `ToolUse` map across with field renames only; `ToolResult`
/// becomes `{type: "tool_result", tool_use_id, content}`.
fn render_blocks(blocks: &[ContentBlock]) -> Value {
    let arr: Vec<Value> = blocks
        .iter()
        .map(|b| match b {
            ContentBlock::Text { text } => json!({"type": "text", "text": text}),
            ContentBlock::ToolUse { id, name, input } => json!({
                "type": "tool_use",
                "id": id,
                "name": name,
                "input": input,
            }),
            ContentBlock::ToolResult {
                tool_use_id,
                content,
            } => json!({
                "type": "tool_result",
                "tool_use_id": tool_use_id,
                "content": content,
            }),
        })
        .collect();
    Value::Array(arr)
}

/// Parse a 200 OK response body into a `ParsedComplete`.
///
/// Vendor-blind on purpose: this function knows nothing about latency, the
/// vendor tag, or the model id. `complete` composes the result with those
/// extras to mint the public `CompleteResponse` + `CallStats`.
///
/// Unknown content-block types are tolerated (skipped) so a future
/// `thinking` or `image` block doesn't break callers that only care about
/// `text` and `tool_use`. Unknown top-level fields are also ignored.
pub(crate) fn parse_response(body: &[u8]) -> Result<ParsedComplete, ModelError> {
    let v: Value = serde_json::from_slice(body)
        .map_err(|e| ModelError::Parse(format!("response body is not JSON: {e}")))?;

    let content_arr = v
        .get("content")
        .and_then(|c| c.as_array())
        .ok_or_else(|| ModelError::Parse("response missing `content` array".into()))?;

    let mut content = Vec::with_capacity(content_arr.len());
    let mut tool_calls = Vec::new();
    for block in content_arr {
        let ty = block
            .get("type")
            .and_then(|t| t.as_str())
            .ok_or_else(|| ModelError::Parse("content block missing string `type`".into()))?;
        match ty {
            "text" => {
                let text = block
                    .get("text")
                    .and_then(|t| t.as_str())
                    .ok_or_else(|| ModelError::Parse("text block missing `text`".into()))?
                    .to_string();
                content.push(ContentBlock::Text { text });
            }
            "tool_use" => {
                let id = block
                    .get("id")
                    .and_then(|t| t.as_str())
                    .ok_or_else(|| ModelError::Parse("tool_use missing `id`".into()))?
                    .to_string();
                let name = block
                    .get("name")
                    .and_then(|t| t.as_str())
                    .ok_or_else(|| ModelError::Parse("tool_use missing `name`".into()))?
                    .to_string();
                let input = block.get("input").cloned().unwrap_or(Value::Null);
                content.push(ContentBlock::ToolUse {
                    id: id.clone(),
                    name: name.clone(),
                    input: input.clone(),
                });
                tool_calls.push(ToolCall {
                    id,
                    name,
                    arguments: input,
                });
            }
            // Tolerate forward-compat block types (e.g. `thinking`).
            _ => {}
        }
    }

    let usage_v = v
        .get("usage")
        .ok_or_else(|| ModelError::Parse("response missing `usage`".into()))?;
    let input_tokens = usage_v
        .get("input_tokens")
        .and_then(|n| n.as_u64())
        .ok_or_else(|| ModelError::Parse("usage missing `input_tokens`".into()))?
        as u32;
    let output_tokens = usage_v
        .get("output_tokens")
        .and_then(|n| n.as_u64())
        .ok_or_else(|| ModelError::Parse("usage missing `output_tokens`".into()))?
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

/// Map a non-200 HTTP status + raw body to a `ModelError`.
///
/// Categorization is by status class — the body is included verbatim in
/// the error string so logs surface the upstream reason without forcing
/// callers to re-parse.
pub fn map_status_error(status: u16, body: &[u8]) -> ModelError {
    let snippet = String::from_utf8_lossy(body).into_owned();
    match status {
        401 | 403 => ModelError::Auth(format!("HTTP {status}: {snippet}")),
        429 => ModelError::RateLimit(format!("HTTP {status}: {snippet}")),
        500..=599 => ModelError::Transport(format!("HTTP {status}: {snippet}")),
        _ => ModelError::Other(format!("HTTP {status}: {snippet}")),
    }
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
                            id: "toolu_01".into(),
                            name: "get_weather".into(),
                            input: json!({"location": "SF"}),
                        },
                    ],
                },
                Message {
                    role: Role::Tool,
                    content: vec![ContentBlock::ToolResult {
                        tool_use_id: "toolu_01".into(),
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
            runtime_tools: Vec::new(),
            model: None,
            options: CompleteOptions {
                max_tokens: 256,
                temperature: Some(0.0),
            },
        }
    }

    #[test]
    fn build_body_matches_anthropic_wire_shape_golden() {
        let body = build_body(&representative_request(), DEFAULT_MODEL);
        let expected = json!({
            "model": DEFAULT_MODEL,
            "max_tokens": 256,
            "temperature": 0.0,
            "system": "be terse",
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "what's the weather?"}],
                },
                {
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "checking"},
                        {
                            "type": "tool_use",
                            "id": "toolu_01",
                            "name": "get_weather",
                            "input": {"location": "SF"},
                        },
                    ],
                },
                {
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": "toolu_01",
                        "content": "72F",
                    }],
                },
            ],
            "tools": [{
                "name": "get_weather",
                "description": "Get current weather for a location",
                "input_schema": {
                    "type": "object",
                    "properties": {"location": {"type": "string"}},
                    "required": ["location"],
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
            runtime_tools: Vec::new(),
            model: None,
            options: CompleteOptions {
                max_tokens: 32,
                temperature: None,
            },
        };
        let body = build_body(&req, "m");
        assert!(body.get("temperature").is_none(), "no temperature field");
        assert!(body.get("system").is_none(), "no system field");
        assert!(body.get("tools").is_none(), "no tools field");
        assert!(
            body.get("tool_choice").is_none(),
            "no tool_choice when tools list is empty"
        );
        assert_eq!(body["max_tokens"], json!(32));
        assert_eq!(body["model"], json!("m"));
    }

    #[test]
    fn build_body_does_not_set_tool_choice_when_tools_present() {
        // Tool-bearing requests must omit `tool_choice` so the Anthropic API
        // default (parallel tools enabled) applies.
        let req = CompleteRequest {
            messages: vec![Message::user("hi")],
            tools: vec![ToolSpec {
                name: "echo".into(),
                description: "echo".into(),
                input_schema: json!({"type": "object"}),
            }],
            runtime_tools: Vec::new(),
            model: None,
            options: CompleteOptions {
                max_tokens: 32,
                temperature: None,
            },
        };
        let body = build_body(&req, "m");
        assert!(
            body.get("tool_choice").is_none(),
            "tool_choice must be absent on tool-bearing requests, got: {body}"
        );
        assert!(body.get("tools").is_some(), "tools field should be present");
    }

    #[test]
    fn build_body_concatenates_multiple_system_messages() {
        let req = CompleteRequest {
            messages: vec![
                Message::system("first"),
                Message::system("second"),
                Message::user("hi"),
            ],
            tools: vec![],
            runtime_tools: Vec::new(),
            model: None,
            options: CompleteOptions {
                max_tokens: 32,
                temperature: None,
            },
        };
        let body = build_body(&req, "m");
        assert_eq!(body["system"], json!("first\n\nsecond"));
        // System messages do not appear in `messages[]`.
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn parse_response_handles_text_only() {
        let raw = br#"{
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": "hello"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 5}
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
        let raw = br#"{
            "id": "msg_2",
            "type": "message",
            "role": "assistant",
            "content": [{
                "type": "tool_use",
                "id": "toolu_99",
                "name": "echo",
                "input": {"msg": "hi"}
            }],
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 7, "output_tokens": 3}
        }"#;
        let r = parse_response(raw).unwrap();
        assert_eq!(
            r.content,
            vec![ContentBlock::ToolUse {
                id: "toolu_99".into(),
                name: "echo".into(),
                input: json!({"msg": "hi"}),
            }]
        );
        assert_eq!(
            r.tool_calls,
            vec![ToolCall {
                id: "toolu_99".into(),
                name: "echo".into(),
                arguments: json!({"msg": "hi"}),
            }]
        );
        assert_eq!(r.usage.input_tokens, 7);
        assert_eq!(r.usage.output_tokens, 3);
    }

    #[test]
    fn parse_response_handles_mixed_text_and_tool_use() {
        let raw = br#"{
            "content": [
                {"type": "text", "text": "let me check"},
                {"type": "tool_use", "id": "t1", "name": "echo", "input": {"x": 1}},
                {"type": "thinking", "thinking": "ignored future block"}
            ],
            "usage": {"input_tokens": 4, "output_tokens": 9}
        }"#;
        let r = parse_response(raw).unwrap();
        assert_eq!(r.content.len(), 2);
        assert_eq!(r.tool_calls.len(), 1);
        assert_eq!(r.tool_calls[0].id, "t1");
    }

    #[test]
    fn parse_response_rejects_malformed_json() {
        let raw = b"not json {{{";
        let err = parse_response(raw).unwrap_err();
        assert!(matches!(err, ModelError::Parse(_)));
    }

    #[test]
    fn parse_response_rejects_missing_usage() {
        let raw = br#"{"content": []}"#;
        let err = parse_response(raw).unwrap_err();
        assert!(matches!(err, ModelError::Parse(_)));
    }

    #[test]
    fn map_status_401_is_auth() {
        let e = map_status_error(401, b"{\"error\":\"bad key\"}");
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
        let e = map_status_error(429, b"{\"type\":\"rate_limit_error\"}");
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
        let e = map_status_error(400, b"{\"error\":\"bad request\"}");
        assert!(matches!(e, ModelError::Other(_)));
        assert!(e.to_string().contains("400"));
        assert!(e.to_string().contains("bad request"));
    }

    #[test]
    fn anthropic_client_dyn_dispatch_compiles() {
        // Compile-time: AnthropicClient implements `ModelClient` and is
        // `Send + Sync`, so it can be stashed behind `Box<dyn ModelClient>`.
        let _: Box<dyn ModelClient> = Box::new(AnthropicClient::new());
    }

    #[tokio::test]
    async fn complete_returns_auth_error_when_api_key_missing() {
        // `remove_var` is unsafe because it isn't thread-safe with concurrent
        // env reads; safe here because there are no concurrent readers.
        unsafe {
            std::env::remove_var(API_KEY_ENV);
        }
        let client = AnthropicClient::new();
        let req = CompleteRequest {
            messages: vec![Message::user("hi")],
            tools: vec![],
            runtime_tools: Vec::new(),
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
    fn new_reads_anthropic_model_env_for_unset_empty_and_value() {
        // One sequential test body avoids racing on the process-global env
        // when the harness runs tests concurrently. Empty must behave like
        // unset — `.envrc` defaults often ship as "" and we don't want an
        // empty string sent to the API.
        unsafe {
            std::env::remove_var(MODEL_ENV);
        }
        assert_eq!(
            AnthropicClient::new().model(),
            DEFAULT_MODEL,
            "unset → default"
        );

        unsafe {
            std::env::set_var(MODEL_ENV, "");
        }
        assert_eq!(
            AnthropicClient::new().model(),
            DEFAULT_MODEL,
            "empty → default"
        );

        unsafe {
            std::env::set_var(MODEL_ENV, "claude-sonnet-4-5");
        }
        assert_eq!(
            AnthropicClient::new().model(),
            "claude-sonnet-4-5",
            "value → that value"
        );

        unsafe {
            std::env::remove_var(MODEL_ENV);
        }
    }
}
