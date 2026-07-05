//! Tool trait, registry, and built-in echo tool. Calling a tool through
//! the registry produces an `EvidenceRecord` whose id is the sha256 of
//! the canonical JSON of `(name, args, result)`, so identical calls
//! collapse to one record on disk.

use crate::evidence::EvidenceRecord;
use crate::model_client::ToolSpec;
use anyhow::{anyhow, bail};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A single side-effecting capability the runtime can invoke by name.
///
/// Implementors must be `Send + Sync` so the registry can hand out `Arc<dyn
/// Tool>` across tasks. `call` takes `&self` — tools that need mutable state
/// own their own interior synchronization.
#[async_trait]
pub trait Tool: Send + Sync {
    /// Stable identifier used for registry lookup and evidence hashing.
    /// Two distinct tools must not share a name.
    fn name(&self) -> &str;

    /// Execute the tool with `args` and return the JSON result. Errors
    /// bubble up as `anyhow::Error`; the registry does not wrap them in an
    /// `EvidenceRecord` (failures are not evidence).
    async fn call(&self, args: Value) -> anyhow::Result<Value>;

    /// The tool's argument schema as a JSON Schema object, used to offer the
    /// tool to a model as a first-class typed function on the flatten path.
    /// The default is a permissive open object; a tool offered under a
    /// grammar-constrained decoder (Cohere `strict_tools`) must override this
    /// with a closed, strict-compatible schema (typed properties, at least one
    /// required field, no free-form objects or `oneOf`).
    fn input_schema(&self) -> Value {
        json!({ "type": "object", "properties": {}, "additionalProperties": true })
    }

    /// One-line description shown to the model when the tool is offered
    /// first-class. Defaults to the tool's name.
    fn description(&self) -> String {
        self.name().to_string()
    }
}

/// Registry of `Tool` implementations keyed by `Tool::name`.
///
/// Cheap to construct, cheap to clone the inner `Arc`s. Dispatch is by the
/// string name so that adding a `call_batch(&self, calls)` extension later
/// does not require changing existing tools.
///
/// `owners` maps each registered tool's advertised name to the operator
/// def ids that contributed it (`graph.yaml` `tools[].id`). One def (one
/// MCP server) advertises several names; two defs that dedup to the same
/// server share all of those names, so the value is a *set*. Per-agent
/// dispatch scoping ([`Self::is_call_allowed`]) resolves a call's advertised
/// name through this map and checks the owning defs against the caller's
/// assigned set.
pub struct ToolRegistry {
    tools: HashMap<String, Arc<dyn Tool>>,
    owners: HashMap<String, HashSet<String>>,
}

impl ToolRegistry {
    /// Build an empty registry.
    pub fn new() -> Self {
        Self {
            tools: HashMap::new(),
            owners: HashMap::new(),
        }
    }

    /// Record that the tool advertised as `name` is owned by `def_id`. The
    /// worker calls this for each name an MCP server (or builtin) registered,
    /// so dispatch can later map the name back to its owning def(s). Safe to
    /// call repeatedly for the same `(name, def_id)`; the def set is unioned.
    pub fn record_owner(&mut self, name: &str, def_id: &str) {
        self.owners
            .entry(name.to_string())
            .or_default()
            .insert(def_id.to_string());
    }

    /// Is a call to the tool advertised as `name` permitted for an agent
    /// assigned `allowed_defs`? True iff some def that owns `name` is in the
    /// assigned set. An unknown name (no recorded owner) is rejected — the
    /// agent may only call tools whose owning def it was granted.
    pub fn is_call_allowed(&self, name: &str, allowed_defs: &[String]) -> bool {
        self.owners
            .get(name)
            .is_some_and(|defs| defs.iter().any(|d| allowed_defs.iter().any(|a| a == d)))
    }

    /// Insert `tool` keyed on its `name()`. Returns `Err` if a tool is
    /// already registered under that name — wiring bugs (two tools, same
    /// name) should surface at startup, not silently shadow each other.
    pub fn register(&mut self, tool: Arc<dyn Tool>) -> anyhow::Result<()> {
        let name = tool.name().to_string();
        if self.tools.contains_key(&name) {
            bail!("tool {name:?} is already registered");
        }
        self.tools.insert(name, tool);
        Ok(())
    }

    /// Predicate: is a tool registered under `name`?
    ///
    /// Lets the agent run loop discriminate "model emitted `CallTool` for
    /// a tool that does not exist" (an apply-time correction case) from
    /// "the tool itself errored" (a real call failure) without having to
    /// string-match `tools.call`'s error message.
    pub fn contains(&self, name: &str) -> bool {
        self.tools.contains_key(name)
    }

    /// The typed [`ToolSpec`]s of every registered tool an agent assigned
    /// `allowed_defs` may call. Used to offer a Cohere agent's granted runtime
    /// tools as first-class typed functions on the flatten path. Sorted by
    /// name so the result is deterministic across a Temporal replay (the
    /// backing map iterates in arbitrary order).
    pub fn specs_for(&self, allowed_defs: &[String]) -> Vec<ToolSpec> {
        let mut specs: Vec<ToolSpec> = self
            .tools
            .values()
            .filter(|t| self.is_call_allowed(t.name(), allowed_defs))
            .map(|t| ToolSpec {
                name: t.name().to_string(),
                description: t.description(),
                input_schema: t.input_schema(),
            })
            .collect();
        specs.sort_by(|a, b| a.name.cmp(&b.name));
        specs
    }

    /// Look up the named tool, invoke it with `args`, and wrap the
    /// `(name, args, result)` triple into an `EvidenceRecord`.
    ///
    /// Returns `Err` if no tool is registered under `name` or if the tool
    /// itself errors. The `EvidenceRecord.id` is deterministic in
    /// `(name, args, result)` (see `EvidenceId::new`); `created_at` is the
    /// wall clock at the time of the call and is not part of the id.
    pub async fn call(&self, name: &str, args: Value) -> anyhow::Result<EvidenceRecord> {
        let tool = self
            .tools
            .get(name)
            .ok_or_else(|| anyhow!("no tool registered under name {name:?}"))?
            .clone();
        let result = tool.call(args.clone()).await?;
        Ok(EvidenceRecord::new(name, args, result, Utc::now()))
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Trivial built-in tool that echoes its `args` back inside an envelope.
/// Used by the run-loop integration test and the `node-run` smoke binary
/// to exercise the dispatch path without depending on any external system.
pub struct EchoTool;

#[async_trait]
impl Tool for EchoTool {
    fn name(&self) -> &str {
        "echo"
    }

    async fn call(&self, args: Value) -> anyhow::Result<Value> {
        Ok(json!({ "echoed": args }))
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "msg": { "type": "string", "description": "Text to echo back." }
            },
            "required": ["msg"]
        })
    }

    fn description(&self) -> String {
        "Echo the given `msg` back — a stand-in evidence-minting tool.".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Test double: a tool with a configurable name and constant result,
    /// useful for exercising registry routing without colliding with
    /// `EchoTool`'s name.
    struct ConstTool {
        name: String,
        result: Value,
    }

    #[async_trait]
    impl Tool for ConstTool {
        fn name(&self) -> &str {
            &self.name
        }
        async fn call(&self, _args: Value) -> anyhow::Result<Value> {
            Ok(self.result.clone())
        }
    }

    #[test]
    fn specs_for_returns_typed_specs_of_granted_tools_sorted() {
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(EchoTool)).unwrap();
        reg.register(Arc::new(ConstTool {
            name: "zeta".into(),
            result: json!(0),
        }))
        .unwrap();
        reg.record_owner("echo", "echo-def");
        reg.record_owner("zeta", "zeta-def");

        // Only the granted def's tool is returned, with its typed schema.
        let specs = reg.specs_for(&["echo-def".to_string()]);
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].name, "echo");
        assert_eq!(specs[0].input_schema["required"], json!(["msg"]));

        // Granting both defs returns both, sorted by name (deterministic
        // across a Temporal replay).
        let both = reg.specs_for(&["echo-def".to_string(), "zeta-def".to_string()]);
        let names: Vec<&str> = both.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["echo", "zeta"]);

        // An unknown/ungranted def resolves to nothing.
        assert!(reg.specs_for(&["nope".to_string()]).is_empty());
    }

    #[tokio::test]
    async fn echo_tool_returns_args_under_echoed_key() {
        let echo = EchoTool;
        let out = echo.call(json!({"msg": "hi"})).await.unwrap();
        assert_eq!(out, json!({"echoed": {"msg": "hi"}}));

        // Non-object args are echoed verbatim too — the envelope is
        // independent of the args' JSON shape.
        let out = echo.call(json!(42)).await.unwrap();
        assert_eq!(out, json!({"echoed": 42}));
    }

    #[tokio::test]
    async fn registry_routes_call_by_name_to_the_right_tool() {
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(EchoTool)).unwrap();
        reg.register(Arc::new(ConstTool {
            name: "always_seven".into(),
            result: json!(7),
        }))
        .unwrap();

        let ev = reg.call("echo", json!({"msg": "hi"})).await.unwrap();
        assert_eq!(ev.tool, "echo");
        assert_eq!(ev.result, json!({"echoed": {"msg": "hi"}}));

        let ev = reg.call("always_seven", json!(null)).await.unwrap();
        assert_eq!(ev.tool, "always_seven");
        assert_eq!(ev.result, json!(7));
    }

    #[tokio::test]
    async fn registry_call_with_unknown_name_returns_err() {
        let reg = ToolRegistry::new();
        let err = reg.call("nope", json!({})).await.unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("nope"),
            "error should mention the missing tool name, got: {msg}"
        );
    }

    #[test]
    fn duplicate_register_is_rejected() {
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(EchoTool)).unwrap();
        let err = reg.register(Arc::new(EchoTool)).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("echo") && msg.contains("already"),
            "duplicate register error should name the tool, got: {msg}"
        );
    }

    #[test]
    fn is_call_allowed_checks_owning_def_against_assignment() {
        let mut reg = ToolRegistry::new();
        // One MCP def (`web-search`) advertises two names.
        reg.record_owner("web_search_exa", "web-search");
        reg.record_owner("web_fetch_exa", "web-search");
        reg.record_owner("post_x", "x-search");

        let assigned = vec!["web-search".to_string()];
        // Any advertised name of an assigned def is callable.
        assert!(reg.is_call_allowed("web_search_exa", &assigned));
        assert!(reg.is_call_allowed("web_fetch_exa", &assigned));
        // A name owned only by an unassigned def is not.
        assert!(!reg.is_call_allowed("post_x", &assigned));
        // An unknown advertised name is rejected even with a non-empty grant.
        assert!(!reg.is_call_allowed("rm_rf", &assigned));
        // No assignment → nothing is callable.
        assert!(!reg.is_call_allowed("web_search_exa", &[]));
    }

    #[test]
    fn is_call_allowed_accepts_a_name_shared_by_two_defs() {
        // Two defs that dedup to the same server both own its advertised
        // names; a grant of either def authorizes the call.
        let mut reg = ToolRegistry::new();
        reg.record_owner("web_search_exa", "web-search");
        reg.record_owner("web_search_exa", "web-search-alt");

        assert!(reg.is_call_allowed("web_search_exa", &["web-search-alt".to_string()]));
        assert!(reg.is_call_allowed("web_search_exa", &["web-search".to_string()]));
        assert!(!reg.is_call_allowed("web_search_exa", &["unrelated".to_string()]));
    }

    #[tokio::test]
    async fn registry_call_produces_deterministic_evidence_id() {
        // `EvidenceId` hashes only `(tool, args, result)` — `created_at`
        // is not part of the digest (see `evidence::EvidenceId::new`), so
        // two calls with identical inputs must yield equal ids even when
        // their timestamps differ.
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(EchoTool)).unwrap();

        let args = json!({"msg": "hi"});
        let a = reg.call("echo", args.clone()).await.unwrap();
        let b = reg.call("echo", args).await.unwrap();
        assert_eq!(a.id, b.id);
    }
}
