//! Vendor-neutral trait for "ask a model what to say next". Only the shapes
//! both vendors share live on the trait; vendor-specific knobs live on the
//! impls.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

#[cfg(feature = "llm-anthropic")]
pub mod anthropic;

#[cfg(feature = "llm-cohere")]
pub mod cohere;

/// One conversational turn handed to a `ModelClient`.
///
/// Roles match the four Anthropic-style buckets the prompt renderer
/// produces. Each impl is responsible for translating the trait shape into
/// its own wire format — for example, the Anthropic impl moves `system`
/// turns to a top-level `system` field and rewraps `tool` turns as `user`
/// messages carrying `tool_result` content blocks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: Vec<ContentBlock>,
}

impl Message {
    pub fn system(text: impl Into<String>) -> Self {
        Self {
            role: Role::System,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }

    pub fn user(text: impl Into<String>) -> Self {
        Self {
            role: Role::User,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }

    pub fn assistant(text: impl Into<String>) -> Self {
        Self {
            role: Role::Assistant,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }
}

/// Conversational role.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    System,
    User,
    Assistant,
    /// A tool's reply to a prior `assistant` `tool_use` block. Vendors
    /// disagree on how this is represented on the wire (Anthropic wraps it
    /// in a `user` message with a `tool_result` block); the trait shape
    /// keeps it as a first-class role and the impl does the rewriting.
    Tool,
}

/// One block of content inside a `Message`.
///
/// `tool_use` and `tool_result` are first-class so an `assistant` turn can
/// mix narration with a tool call, and a `tool` turn can reply to a
/// specific call by id. `id` correlation is the contract: a `ToolResult`'s
/// `tool_use_id` must match the `id` of the `ToolUse` it answers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
    },
}

/// A tool the model is allowed to call.
///
/// Field shape mirrors Anthropic's `tools[]` entry verbatim
/// (`name + description + input_schema`); Cohere's wire format is a near
/// trivial transformation of the same three fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

/// Vendor-neutral sampling knobs.
///
/// `max_tokens` is required because Anthropic requires it on the wire;
/// Cohere accepts but does not require it, so this field is the strictest
/// common contract. Keep this struct minimal — vendor-specific knobs live
/// on impl config, not here.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompleteOptions {
    pub max_tokens: u32,
    pub temperature: Option<f32>,
}

impl Default for CompleteOptions {
    fn default() -> Self {
        Self {
            max_tokens: 1024,
            temperature: None,
        }
    }
}

/// Input to `ModelClient::complete`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompleteRequest {
    pub messages: Vec<Message>,
    pub tools: Vec<ToolSpec>,
    pub options: CompleteOptions,
    /// Per-request model override. `None` (the default and the serialized
    /// shape when absent) falls back to the adapter's configured model.
    /// Carries a per-agent `Mandate.model` through to the vendor adapter;
    /// resolve it via [`effective_model`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// The model id a `complete` call should hit: the request's per-request
/// override when set, else the adapter's configured default. Both the wire
/// body and the reported [`CallStats::model`] must read this single value so
/// cost accounting names the model actually called.
pub fn effective_model<'a>(req: &'a CompleteRequest, default_model: &'a str) -> &'a str {
    req.model.as_deref().unwrap_or(default_model)
}

/// Output of `ModelClient::complete`.
///
/// `tool_calls` is a flat projection of `content` for callers that only
/// care about "did the model want to invoke a tool?". The same call appears
/// once in `content` (as a `ToolUse` block) and once in `tool_calls`; the
/// duplication is intentional.
///
/// `stats` carries the cost/latency accounting block, composing `usage`
/// plus the model id, vendor tag, and wall-clock latency measured around
/// the upstream call. Raw counts only; pricing translation lives elsewhere.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompleteResponse {
    pub content: Vec<ContentBlock>,
    pub tool_calls: Vec<ToolCall>,
    pub usage: Usage,
    pub stats: CallStats,
}

/// A single tool invocation projected out of `CompleteResponse::content`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

/// Token-count accounting from one `complete` call. Field names match
/// Anthropic's; Cohere returns the same two numbers under different keys
/// and the impl normalizes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u32,
    pub output_tokens: u32,
}

/// Which vendor produced a `CallStats`. Small closed enum rather than a
/// `String` so callers can match on it cheaply and tracing field rendering
/// stays allocation-free.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Vendor {
    Anthropic,
    Cohere,
}

impl Vendor {
    /// Stable string form, used for tracing fields and tests.
    pub const fn as_str(self) -> &'static str {
        match self {
            Vendor::Anthropic => "anthropic",
            Vendor::Cohere => "cohere",
        }
    }
}

/// Per-call cost + latency accounting block.
///
/// Composes `Usage` rather than duplicating the token counts so there is
/// exactly one source of truth for tokens-in / tokens-out. Ships only raw
/// inputs; pricing translation and budget enforcement live elsewhere.
///
/// No `Default` impl: a `CallStats` is only ever constructed by a vendor
/// adapter's `complete` method, which knows the real vendor, latency, and
/// model id. That keeps "stats with a placeholder vendor" unrepresentable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallStats {
    pub usage: Usage,
    /// Wall-clock latency around the `ModelClient::complete` HTTP call,
    /// measured by the vendor impl with `std::time::Instant`.
    pub latency_ms: u64,
    /// Vendor that produced this call. See `Vendor::as_str` for the
    /// stable tracing string.
    pub vendor: Vendor,
    /// Model id the call hit. Owned `String` because model ids are
    /// configurable via env vars and `with_model`.
    pub model: String,
}

impl CallStats {
    pub fn tokens_in(&self) -> u32 {
        self.usage.input_tokens
    }

    pub fn tokens_out(&self) -> u32 {
        self.usage.output_tokens
    }
}

/// Categorized failure mode from a model adapter.
///
/// The split exists so callers can decide what to do with the error
/// without parsing prose: rate-limit and transport errors are typically
/// retried, auth and parse errors are not, and `Other` is a catch-all for
/// the long tail of 4xx vendor-specific responses.
#[derive(Debug, thiserror::Error)]
pub enum ModelError {
    /// Response body could not be parsed as the expected shape.
    #[error("parse error: {0}")]
    Parse(String),
    /// Network failure or 5xx upstream.
    #[error("transport error: {0}")]
    Transport(String),
    /// Provider rate-limited the request (HTTP 429 or vendor equivalent).
    #[error("rate limit: {0}")]
    RateLimit(String),
    /// Missing or invalid credentials (HTTP 401 / 403).
    #[error("auth error: {0}")]
    Auth(String),
    /// The provider rejected the model's tool call as invalid (e.g. Cohere's
    /// HTTP 422 `INVALID_TOOL_GENERATION`). Kept distinct from `Other`
    /// because it is non-deterministic: a resample or a corrective nudge can
    /// clear it, so callers treat it like a parse failure, not a permanent
    /// 4xx. Collapsing it into `Other` would make it look un-retryable.
    #[error("invalid tool generation: {0}")]
    InvalidToolGeneration(String),
    /// Anything else — typically a 4xx with a vendor-specific shape.
    #[error("model error: {0}")]
    Other(String),
}

/// The trait every model adapter implements. Held as `Arc<dyn ModelClient>`
/// by callers — the substitution boundary across vendors.
#[async_trait::async_trait]
pub trait ModelClient: Send + Sync {
    async fn complete(&self, req: CompleteRequest) -> Result<CompleteResponse, ModelError>;
}

// `dyn ModelClient: Send + Sync` is load-bearing for sharing across tasks.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync + ?Sized>() {}
    assert_send_sync::<dyn ModelClient>();
};

/// Split a qualified model name `provider/model` into its two parts.
///
/// Splits on the *first* slash only, so a model id that itself contains
/// slashes (some local/self-hosted ids do) stays intact. Returns `Err` for
/// a missing slash or an empty provider/model half — the caller turns that
/// into a configuration error.
pub fn parse_qualified_model(qualified: &str) -> Result<(&str, &str), String> {
    let (provider, model) = qualified
        .split_once('/')
        .ok_or_else(|| format!("`{qualified}` is not a qualified `provider/model` name"))?;
    if provider.is_empty() {
        return Err(format!("`{qualified}` has an empty provider"));
    }
    if model.is_empty() {
        return Err(format!("`{qualified}` has an empty model"));
    }
    Ok((provider, model))
}

/// Maps a provider prefix to the [`ModelClient`] that serves it.
///
/// The worker boots a [`ModelRegistry::new`] map populated from whatever
/// provider keys exist, so any agent can name any available provider via its
/// `provider/model` config. Keys are the stable [`Vendor::as_str`] strings
/// (`"anthropic"`, `"cohere"`), so the prefix an operator writes, the client
/// that runs, and the vendor reported in [`CallStats`] all agree; a
/// `provider/` an operator names that isn't registered is an error.
///
/// [`ModelRegistry::single`] is the single-vendor convenience for the
/// in-process CLI and tests: it ignores the provider prefix and serves every
/// request from one client.
#[derive(Clone)]
pub struct ModelRegistry(Registry);

#[derive(Clone)]
enum Registry {
    /// One client serves everything; the provider prefix is ignored.
    Single(Arc<dyn ModelClient>),
    /// Strict prefix→client map. `default` names the provider used for
    /// `None`/unqualified models and must be a key of `clients`.
    Map {
        clients: HashMap<String, Arc<dyn ModelClient>>,
        default: String,
    },
}

impl ModelRegistry {
    /// Build a strict registry from `(provider, client)` pairs with
    /// `default` naming the provider used for unqualified / `None` model
    /// requests. Errors if `clients` is empty or `default` names a provider
    /// absent from the map.
    pub fn new(
        clients: impl IntoIterator<Item = (String, Arc<dyn ModelClient>)>,
        default: impl Into<String>,
    ) -> Result<Self, String> {
        let clients: HashMap<String, Arc<dyn ModelClient>> = clients.into_iter().collect();
        let default = default.into();
        if clients.is_empty() {
            return Err("model registry needs at least one provider".to_string());
        }
        if !clients.contains_key(&default) {
            return Err(format!("default provider `{default}` is not registered"));
        }
        Ok(Self(Registry::Map { clients, default }))
    }

    /// Single-client registry that serves every request from `client`,
    /// ignoring any provider prefix. The single-vendor in-process CLI and
    /// tests use this; the multi-provider worker boots [`ModelRegistry::new`].
    pub fn single(client: Arc<dyn ModelClient>) -> Self {
        Self(Registry::Single(client))
    }

    /// Resolve a `Mandate.model` value to the client that should serve it
    /// plus the bare model id to send in [`CompleteRequest::model`].
    ///
    /// - `None` → the default provider's client, no per-request model
    ///   override (the adapter's own default model).
    /// - `Some("provider/model")` → that provider's client and the bare
    ///   `model`; an unregistered provider is an error.
    /// - `Some("model")` (no slash) → the default provider's client and the
    ///   bare model.
    ///
    /// A [`ModelRegistry::single`] registry ignores the provider prefix and
    /// always serves from its one client, returning the bare model.
    pub fn resolve(
        &self,
        model: Option<&str>,
    ) -> Result<(Arc<dyn ModelClient>, Option<String>), String> {
        let bare = match model {
            None => None,
            Some(spec) if spec.contains('/') => Some(parse_qualified_model(spec)?.1.to_string()),
            Some(bare) => Some(bare.to_string()),
        };
        match &self.0 {
            Registry::Single(client) => Ok((client.clone(), bare)),
            Registry::Map { clients, default } => {
                let provider = match model {
                    Some(spec) if spec.contains('/') => parse_qualified_model(spec)?.0,
                    _ => default.as_str(),
                };
                let client = clients.get(provider).cloned().ok_or_else(|| {
                    let mut known: Vec<&str> = clients.keys().map(String::as_str).collect();
                    known.sort_unstable();
                    format!(
                        "unknown provider `{provider}`; registered providers: [{}]",
                        known.join(", ")
                    )
                })?;
                Ok((client, bare))
            }
        }
    }

    /// Registered provider prefixes, sorted. For boot logging. A
    /// single-client registry reports no named providers.
    pub fn providers(&self) -> Vec<&str> {
        match &self.0 {
            Registry::Single(_) => Vec::new(),
            Registry::Map { clients, .. } => {
                let mut names: Vec<&str> = clients.keys().map(String::as_str).collect();
                names.sort_unstable();
                names
            }
        }
    }

    /// The provider serving unqualified / `None` model requests, if this is
    /// a strict map registry.
    pub fn default_provider(&self) -> Option<&str> {
        match &self.0 {
            Registry::Single(_) => None,
            Registry::Map { default, .. } => Some(default),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn message_constructors_produce_single_text_block() {
        let m = Message::user("hi");
        assert_eq!(m.role, Role::User);
        assert_eq!(m.content, vec![ContentBlock::Text { text: "hi".into() }]);
    }

    #[test]
    fn role_serializes_as_snake_case() {
        let r = Role::Assistant;
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(s, "\"assistant\"");
        let back: Role = serde_json::from_str("\"tool\"").unwrap();
        assert_eq!(back, Role::Tool);
    }

    #[test]
    fn content_block_round_trip() {
        let blocks = vec![
            ContentBlock::Text { text: "hi".into() },
            ContentBlock::ToolUse {
                id: "toolu_1".into(),
                name: "echo".into(),
                input: json!({"msg": "hi"}),
            },
            ContentBlock::ToolResult {
                tool_use_id: "toolu_1".into(),
                content: "ok".into(),
            },
        ];
        let s = serde_json::to_string(&blocks).unwrap();
        let back: Vec<ContentBlock> = serde_json::from_str(&s).unwrap();
        assert_eq!(blocks, back);
    }

    #[test]
    fn complete_options_default_has_nonzero_max_tokens() {
        let o = CompleteOptions::default();
        assert!(o.max_tokens > 0);
        assert!(o.temperature.is_none());
    }

    #[test]
    fn effective_model_prefers_request_override_then_default() {
        let base = CompleteRequest {
            messages: vec![],
            tools: vec![],
            model: None,
            options: CompleteOptions::default(),
        };
        assert_eq!(effective_model(&base, "worker-default"), "worker-default");

        let overridden = CompleteRequest {
            model: Some("claude-opus-4-8".into()),
            ..base
        };
        assert_eq!(
            effective_model(&overridden, "worker-default"),
            "claude-opus-4-8"
        );
    }

    #[test]
    fn complete_request_omits_model_from_wire_when_none() {
        let req = CompleteRequest {
            messages: vec![],
            tools: vec![],
            model: None,
            options: CompleteOptions::default(),
        };
        let s = serde_json::to_string(&req).unwrap();
        assert!(
            !s.contains("model"),
            "model should be omitted when None: {s}"
        );
    }

    #[test]
    fn model_error_displays_categorized_message() {
        let e = ModelError::RateLimit("slow down".into());
        assert!(e.to_string().contains("rate limit"));
        assert!(e.to_string().contains("slow down"));
        let e = ModelError::Auth("bad key".into());
        assert!(e.to_string().contains("auth"));
    }

    #[test]
    fn model_error_implements_std_error() {
        fn assert_error<E: std::error::Error>() {}
        assert_error::<ModelError>();
    }

    struct StubClient;

    #[async_trait::async_trait]
    impl ModelClient for StubClient {
        async fn complete(&self, _req: CompleteRequest) -> Result<CompleteResponse, ModelError> {
            unreachable!("registry resolution tests never call complete()")
        }
    }

    fn stub() -> Arc<dyn ModelClient> {
        Arc::new(StubClient)
    }

    #[test]
    fn parse_qualified_model_splits_on_first_slash() {
        assert_eq!(
            parse_qualified_model("anthropic/claude-opus-4-8").unwrap(),
            ("anthropic", "claude-opus-4-8")
        );
        // First slash only: a model id may itself contain slashes.
        assert_eq!(
            parse_qualified_model("local/org/llama-3").unwrap(),
            ("local", "org/llama-3")
        );
    }

    #[test]
    fn parse_qualified_model_rejects_missing_or_empty_halves() {
        assert!(parse_qualified_model("claude-opus-4-8").is_err());
        assert!(parse_qualified_model("/claude-opus-4-8").is_err());
        assert!(parse_qualified_model("anthropic/").is_err());
    }

    #[test]
    fn registry_new_rejects_empty_or_unknown_default() {
        assert!(ModelRegistry::new(Vec::new(), "anthropic").is_err());
        assert!(ModelRegistry::new([("anthropic".to_string(), stub())], "cohere").is_err());
    }

    #[test]
    fn registry_resolves_qualified_name_to_its_provider() {
        let anthropic = stub();
        let cohere = stub();
        let reg = ModelRegistry::new(
            [
                ("anthropic".to_string(), anthropic.clone()),
                ("cohere".to_string(), cohere.clone()),
            ],
            "anthropic",
        )
        .unwrap();

        let (client, model) = reg.resolve(Some("cohere/command-a")).unwrap();
        assert!(Arc::ptr_eq(&client, &cohere));
        assert_eq!(model.as_deref(), Some("command-a"));

        let (client, model) = reg.resolve(Some("anthropic/claude-opus-4-8")).unwrap();
        assert!(Arc::ptr_eq(&client, &anthropic));
        assert_eq!(model.as_deref(), Some("claude-opus-4-8"));
    }

    #[test]
    fn registry_routes_none_and_bare_to_default_provider() {
        let anthropic = stub();
        let cohere = stub();
        let reg = ModelRegistry::new(
            [
                ("anthropic".to_string(), anthropic.clone()),
                ("cohere".to_string(), cohere.clone()),
            ],
            "anthropic",
        )
        .unwrap();

        let (client, model) = reg.resolve(None).unwrap();
        assert!(Arc::ptr_eq(&client, &anthropic));
        assert!(model.is_none());

        let (client, model) = reg.resolve(Some("claude-opus-4-8")).unwrap();
        assert!(Arc::ptr_eq(&client, &anthropic));
        assert_eq!(model.as_deref(), Some("claude-opus-4-8"));
    }

    #[test]
    fn registry_errors_on_unknown_provider() {
        let reg = ModelRegistry::new([("anthropic".to_string(), stub())], "anthropic").unwrap();
        let err = match reg.resolve(Some("local/llama-3")) {
            Ok(_) => panic!("expected unknown-provider error"),
            Err(e) => e,
        };
        assert!(err.contains("unknown provider `local`"), "got: {err}");
        assert!(err.contains("anthropic"), "should list registered: {err}");
    }

    #[test]
    fn single_registry_serves_every_request_from_one_client() {
        let only = stub();
        let reg = ModelRegistry::single(only.clone());
        // Provider prefix is ignored; the bare model still rides through.
        for (spec, want_model) in [
            (None, None),
            (Some("claude-opus-4-8"), Some("claude-opus-4-8")),
            (Some("cohere/command-a"), Some("command-a")),
        ] {
            let (client, model) = reg.resolve(spec).unwrap();
            assert!(Arc::ptr_eq(&client, &only));
            assert_eq!(model.as_deref(), want_model);
        }
        assert_eq!(reg.default_provider(), None);
        assert!(reg.providers().is_empty());
    }
}
