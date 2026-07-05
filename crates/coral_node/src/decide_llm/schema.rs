//! Tool-use schema + parser for `Decision`.
//!
//! One tool per `Decision` variant: the model sees a tool list whose
//! names map one-to-one to the variant tags. Variant is fixed by the
//! tool name, so the model can't confuse the discriminator with a
//! payload field, and per-tool `input_schema`s give provider-side
//! validation per variant.
//!
//! The parser leans on `Decision`'s existing
//! `#[serde(tag = "type", rename_all = "snake_case")]`: it injects the
//! variant tag and feeds the resulting JSON object through
//! `serde_json::from_value`. The schema cannot silently drift from
//! `decision.rs` without a compile or test failure.

use crate::decision::{ClaimSeed, Decision, ToolCall as DecisionToolCall};
use crate::model_client::{ToolCall, ToolSpec};
use serde_json::{json, Value};
use thiserror::Error;

/// Tool name → matching `Decision` variant tag (`#[serde(tag = "type")]`
/// value). Listed in the same order as the `Decision` enum so a reviewer
/// can scan the two side by side.
const TOOL_NAMES: &[&str] = &[
    "call_tool",
    "write_output",
    "rewrite_fs",
    "read",
    "list",
    "search",
    "idle",
    "spawn_child",
    "reconcile_children",
    "retire_child",
    "replace_child",
];

/// Build the `ToolSpec` list to publish to the model via
/// `CompleteRequest::tools`.
///
/// Schemas are deliberately loose where `Decision`'s serde derive already
/// validates the shape (e.g. `FsOp`'s internally-tagged `op` field): we
/// describe the field as `object` and let `parse_decision` surface a
/// structured error if the model emits the wrong inner shape. Tighter JSON
/// Schema would duplicate `decision.rs` and drift from it under change.
pub fn decision_tools() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "call_tool".into(),
            description: "Express the decision to invoke a runtime tool. The runtime — \
                 not this call — performs the dispatch; this tool only \
                 records the agent's intent for the next tick."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the runtime tool to invoke."
                    },
                    "args": {
                        "description": "JSON arguments for the tool. Shape \
                                        is tool-specific."
                    },
                    "claim_seed": {
                        "type": "string",
                        "description": "Opaque seed the agent picks so the \
                                        resulting evidence can be linked \
                                        back to the claim it supports."
                    }
                },
                "required": ["name", "args", "claim_seed"]
            }),
        },
        ToolSpec {
            name: "write_output".into(),
            description: "Write your single, kept-current Output. `body` (prose) replaces \
                 your canonical output; `citations` are the evidence file paths it rests \
                 on — the handles returned in your tool-call and reconcile observations. \
                 Every path in `citations` must be an existing file under `evidence/`; the \
                 runtime will refuse to persist the output otherwise."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "body": { "type": "string" },
                    "citations": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Evidence file paths the output cites, e.g. \
                            \"evidence/tsmc-cowos-a1b2c3d4.json\"."
                    }
                },
                "required": ["body", "citations"]
            }),
        },
        ToolSpec {
            name: "rewrite_fs".into(),
            description: "Express the decision to mutate the per-agent filesystem. \
                 Each op writes or deletes one file; on a delete, `content` is \
                 ignored."
                .into(),
            // `FsOp` is an internally-tagged enum (`{op, path, content?}`).
            // Its schema is a hand-written flat struct rather than a `oneOf`
            // union so it survives grammar-constrained decoding (`strict_tools`
            // rejects `oneOf`). `content` is always required for the same
            // reason — strict cannot express per-variant optionality — and a
            // `delete_file` simply carries an ignored `content` (serde drops the
            // extra field). The flat shape still matches `FsOp`'s serde form,
            // so `RewriteFs` deserializes unchanged.
            input_schema: json!({
                "type": "object",
                "properties": {
                    "ops": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "op": { "type": "string", "enum": ["write_file", "delete_file"] },
                                "path": { "type": "string" },
                                "content": { "type": "string" }
                            },
                            "required": ["op", "path", "content"]
                        }
                    }
                },
                "required": ["ops"]
            }),
        },
        ToolSpec {
            name: "read".into(),
            description: "Read the full contents of one file in your own filesystem \
                 (or a descendant agent's, read-only). The file body comes back as the \
                 observation for your next step. Use this to pull a file the index named, \
                 rather than expecting its contents to be handed to you."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path relative to the agent root, e.g. \
                                        `notes/plan.md`."
                    }
                },
                "required": ["path"]
            }),
        },
        ToolSpec {
            name: "list".into(),
            description: "List the entries directly under a directory in your own \
                 filesystem (or a descendant's, read-only). Files appear as names; \
                 subdirectories as `name/`."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Directory path relative to the agent root, e.g. `notes/`."
                    }
                },
                "required": ["path"]
            }),
        },
        ToolSpec {
            name: "search".into(),
            description: "Substring-search file contents under an optional path scope \
                 (your whole filesystem when omitted). Returns matching files and the \
                 first matching line in each."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "path": {
                        "type": "string",
                        "description": "Optional directory scope, e.g. `notes/`. \
                                        Omit to search the whole filesystem."
                    }
                },
                "required": ["query"]
            }),
        },
        ToolSpec {
            name: "idle".into(),
            description: "Express the decision to idle. The scheduler waits at least \
                 `next_after` milliseconds before the next idle wake."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "next_after": {
                        "type": "integer",
                        "description": "Milliseconds to wait (non-negative)."
                    }
                },
                "required": ["next_after"]
            }),
        },
        // Parent-child topology decision tools. Each is a terminal
        // singleton (same shape contract as `idle`): mixing with
        // `call_tool` or another terminal fails parsing. Inner shapes
        // (`mandate`, `sources`, `child_ref`, ...) are described as plain
        // `object` and left to serde validation, mirroring the
        // `rewrite_fs.ops` precedent — hand-rolling JSON Schema for
        // `Mandate` / `AgentRef` / friends would duplicate `decision.rs`
        // + `mandate.rs` + `agent_ref.rs` and drift on every kernel-shape
        // change.
        ToolSpec {
            name: "spawn_child".into(),
            description: "Express the decision to spawn a child agent. The runtime \
                 allocates the child's agent_id deterministically and \
                 instantiates a child workflow under \
                 graphs/<graph_id>/agents/<new_agent_id>. The variant \
                 carries the agent's logical name + mandate. The mandate's \
                 `tools` list is the child's assigned tools (definition ids \
                 from this graph's tools); you may grant only tools this \
                 graph defines — spawning with an undefined tool is rejected."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "agent_name": { "type": "string" },
                    "mandate": { "type": "object" }
                },
                "required": ["agent_name", "mandate"]
            }),
        },
        ToolSpec {
            name: "reconcile_children".into(),
            description: "Express the decision to fold N child outputs into the \
                 parent's context as synthetic evidence; optionally record \
                 a conflict if the children disagree. Each source becomes \
                 one synthetic evidence record in the parent's evidence/ \
                 directory; the parent's next write_output can cite those \
                 synthetic ids."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "sources": {
                        "type": "array",
                        "items": { "type": "object" },
                        "description": "Each: {child_ref, output_id}."
                    },
                    "conflict": {
                        "type": "object",
                        "description": "Optional. Set iff the children disagree. \
                                        Carries {alternatives: [..], resolution?}."
                    }
                },
                "required": ["sources"]
            }),
        },
        ToolSpec {
            name: "retire_child".into(),
            description: "Express the decision to terminate a child agent. The \
                 workflow host signals the child's existing retire arm via \
                 signal_external_workflow. No replacement is spawned; use \
                 replace_child for that."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "child_ref": {
                        "type": "object",
                        "description": "Stable handle: {workflow_id, agent_id}."
                    },
                    "reason": { "type": "string" }
                },
                "required": ["child_ref", "reason"]
            }),
        },
        ToolSpec {
            name: "replace_child".into(),
            description: "Express the decision to retire a child and spawn a \
                 replacement with a new mandate. The replacement gets a \
                 fresh agent_id + workflow id — not an in-place mandate \
                 swap on the existing child."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "child_ref": {
                        "type": "object",
                        "description": "Stable handle: {workflow_id, agent_id}."
                    },
                    "new_mandate": { "type": "object" }
                },
                "required": ["child_ref", "new_mandate"]
            }),
        },
    ]
}

/// Structured failure mode from `parse_decision`. Each variant carries
/// enough context that the correction loop can quote the failure back to
/// the model in a corrective system message.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecisionParseError {
    /// The model returned no tool call where at least one was expected.
    #[error("expected at least one tool call, got none")]
    NoCalls,
    /// The model returned multiple tool calls that mix `call_tool` (the
    /// parallel-tool path) with one of the terminal decision tools
    /// (`write_output`, `rewrite_fs`, `idle`). Terminal decisions cannot
    /// batch with other calls in the same tick.
    #[error("mixed decision tools in one response: {names:?}")]
    MixedDecisionTools { names: Vec<String> },
    /// The model returned multiple instances of a terminal decision tool
    /// (e.g. two `idle` blocks in the same response). Terminal
    /// decisions are singular by construction.
    #[error("expected exactly one `{tool}` call, got {count}")]
    DuplicateTerminalTool { tool: String, count: usize },
    /// The tool name does not correspond to any `Decision` variant.
    #[error("unknown tool: {0}")]
    UnknownTool(String),
    /// `arguments` was not a JSON object.
    #[error("tool {tool}: arguments must be a JSON object")]
    ArgumentsNotObject { tool: String },
    /// A required field on the tool's input schema was absent.
    #[error("tool {tool}: missing required field `{field}`")]
    MissingField { tool: String, field: String },
    /// A required field was present but failed to deserialize into the
    /// expected `Decision` variant shape.
    #[error("tool {tool}: invalid value for `{field}`: {reason}")]
    InvalidValue {
        tool: String,
        field: String,
        reason: String,
    },
}

/// Convert a vendor-normalized list of tool calls into a `Decision`.
///
/// `ToolCall` is already vendor-neutral (Anthropic and Cohere both reduce
/// to `{ id, name, arguments }`); vendor-specific normalization is the
/// responsibility of the `ModelClient` impl, not this layer.
///
/// Multi-call shape: when every entry names `call_tool`, the parser
/// folds them into a single `Decision::CallTools`. The vendor
/// `ToolCall.id` (the `tool_use.id` on the wire) propagates into each
/// `decision::ToolCall.tool_use_id` so the run loop can stage the paired
/// `tool_result` blocks for the next prompt bundle. Terminal decision
/// tools (`write_output`, `rewrite_fs`, `idle`) remain singular: a
/// response that includes any terminal tool alongside another call fails
/// as `MixedDecisionTools` rather than silently discarding the extras.
pub fn parse_decision(calls: &[ToolCall]) -> Result<Decision, DecisionParseError> {
    if calls.is_empty() {
        return Err(DecisionParseError::NoCalls);
    }

    // Validate every name up front so an unknown tool name surfaces as
    // `UnknownTool` regardless of where it sits in the batch (the
    // single-call path used to do this implicitly).
    for c in calls {
        if !TOOL_NAMES.contains(&c.name.as_str()) {
            return Err(DecisionParseError::UnknownTool(c.name.clone()));
        }
    }

    // Mixed-shape detection. If any call is `call_tool` and at least one
    // other call is a terminal tool — or vice versa — the batch is
    // malformed: terminals are singular, and mixing them with parallel
    // calls is never a valid single-tick decision.
    let any_call_tool = calls.iter().any(|c| c.name == "call_tool");
    let any_terminal = calls.iter().any(|c| c.name != "call_tool");
    if any_call_tool && any_terminal {
        let names: Vec<String> = calls.iter().map(|c| c.name.clone()).collect();
        return Err(DecisionParseError::MixedDecisionTools { names });
    }
    if any_terminal && calls.len() > 1 {
        // Two terminals (e.g. two `idle` blocks) — also invalid.
        return Err(DecisionParseError::DuplicateTerminalTool {
            tool: calls[0].name.clone(),
            count: calls.len(),
        });
    }

    if any_call_tool {
        // Parallel-call path: fold every `call_tool` into a single
        // `Decision::CallTools(vec![...])`. Vendor `ToolCall.id` becomes
        // `decision::ToolCall.tool_use_id` so the dispatch layer can
        // emit paired `tool_result` blocks next tick.
        let mut tool_calls = Vec::with_capacity(calls.len());
        for c in calls {
            let args = parse_call_tool_args(c)?;
            tool_calls.push(DecisionToolCall::with_tool_use_id(
                args.name,
                args.args,
                args.claim_seed,
                c.id.clone(),
            ));
        }
        return Ok(Decision::CallTools { calls: tool_calls });
    }

    // Single terminal-tool path.
    let call = &calls[0];
    let Value::Object(args) = &call.arguments else {
        return Err(DecisionParseError::ArgumentsNotObject {
            tool: call.name.clone(),
        });
    };

    // Required-field check is up-front so the structured "missing field"
    // error fires cleanly before serde gets a chance to complain about
    // shape. Lists below mirror the variant fields in `decision.rs`.
    let required: &[&str] = match call.name.as_str() {
        "write_output" => &["body", "citations"],
        "rewrite_fs" => &["ops"],
        "read" => &["path"],
        "list" => &["path"],
        "search" => &["query"],
        "idle" => &["next_after"],
        // Parent-child topology variants — terminal singletons. Inner-
        // shape validation (e.g. `mandate` field structure) falls through
        // to serde via the `tagged.insert("type", ...)` re-tagging below.
        "spawn_child" => &["agent_name", "mandate"],
        "reconcile_children" => &["sources"],
        "retire_child" => &["child_ref", "reason"],
        "replace_child" => &["child_ref", "new_mandate"],
        _ => unreachable!("guarded by mixed/any_call_tool checks above"),
    };
    for field in required {
        if !args.contains_key(*field) {
            return Err(DecisionParseError::MissingField {
                tool: call.name.clone(),
                field: (*field).into(),
            });
        }
    }

    // Re-tag the arguments as a `Decision` and let serde do the actual
    // shape validation. The `type` injection mirrors the `#[serde(tag =
    // "type")]` on `Decision`; tool name == variant tag by construction
    // for the terminal tools.
    let mut tagged = args.clone();
    tagged.insert("type".into(), Value::String(call.name.clone()));

    serde_json::from_value::<Decision>(Value::Object(tagged)).map_err(|e| {
        DecisionParseError::InvalidValue {
            tool: call.name.clone(),
            field: serde_path_or(&e),
            reason: e.to_string(),
        }
    })
}

/// Inner shape of one `call_tool` decision-tool invocation, extracted from
/// the model's tool-use payload. The fields mirror
/// `decision::ToolCall` minus the vendor `tool_use_id` (which the caller
/// pulls from `ToolCall.id`).
struct ParsedCallToolArgs {
    name: String,
    args: serde_json::Value,
    claim_seed: ClaimSeed,
}

fn parse_call_tool_args(call: &ToolCall) -> Result<ParsedCallToolArgs, DecisionParseError> {
    let Value::Object(args) = &call.arguments else {
        return Err(DecisionParseError::ArgumentsNotObject {
            tool: call.name.clone(),
        });
    };
    for field in ["name", "args", "claim_seed"] {
        if !args.contains_key(field) {
            return Err(DecisionParseError::MissingField {
                tool: call.name.clone(),
                field: field.into(),
            });
        }
    }
    let name = args
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| DecisionParseError::InvalidValue {
            tool: call.name.clone(),
            field: "name".into(),
            reason: "expected string".into(),
        })?
        .to_string();
    let inner_args = args.get("args").cloned().unwrap_or(Value::Null);
    let claim_seed_str = args
        .get("claim_seed")
        .and_then(Value::as_str)
        .ok_or_else(|| DecisionParseError::InvalidValue {
            tool: call.name.clone(),
            field: "claim_seed".into(),
            reason: "expected string".into(),
        })?
        .to_string();
    Ok(ParsedCallToolArgs {
        name,
        args: inner_args,
        claim_seed: ClaimSeed::new(claim_seed_str),
    })
}

/// Best-effort field name to attach to an `InvalidValue` error.
///
/// `serde_json::Error` does not surface a structured path; the message
/// usually reads `... at line L column C` or names the offending field
/// via `invalid type: ... expected ...`. Without re-parsing the message
/// we can't reliably extract the field, so we fall back to `<unknown>`
/// and let `reason` carry the detail. The correction loop only needs
/// `reason` to be human-parseable.
fn serde_path_or(_e: &serde_json::Error) -> String {
    "<unknown>".into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decision::{ClaimSeed, FsOp, ToolCall as DecisionToolCall};
    use serde_json::json;
    use std::time::Duration;

    fn call(name: &str, arguments: Value) -> ToolCall {
        ToolCall {
            id: format!("toolu_{name}"),
            name: name.into(),
            arguments,
        }
    }

    // ---- decision_tools() metadata --------------------------------------

    #[test]
    fn decision_tools_has_one_entry_per_variant() {
        let tools = decision_tools();
        assert_eq!(tools.len(), TOOL_NAMES.len());
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, TOOL_NAMES);
    }

    #[test]
    fn decision_tools_have_nonempty_descriptions_and_object_schemas() {
        for t in decision_tools() {
            assert!(
                !t.description.is_empty(),
                "{} has empty description",
                t.name
            );
            assert_eq!(
                t.input_schema.get("type"),
                Some(&json!("object")),
                "{} input_schema is not an object",
                t.name
            );
        }
    }

    // ---- happy-path round trips: one per Decision variant ---------------

    #[test]
    fn parse_single_call_tool_round_trips_as_one_element_call_tools() {
        let tc = call(
            "call_tool",
            json!({
                "name": "echo",
                "args": {"msg": "hi"},
                "claim_seed": "seed-1"
            }),
        );
        // The vendor `ToolCall.id` is "toolu_call_tool" (set by `call`); the
        // parser must carry it through as the decision-side `tool_use_id`.
        let d = parse_decision(&[tc]).unwrap();
        assert_eq!(
            d,
            Decision::CallTools {
                calls: vec![DecisionToolCall::with_tool_use_id(
                    "echo",
                    json!({"msg": "hi"}),
                    ClaimSeed::new("seed-1"),
                    "toolu_call_tool",
                )]
            }
        );
    }

    #[test]
    fn parse_k_call_tool_blocks_folds_into_one_call_tools_decision() {
        // K parallel call_tool blocks → single Decision::CallTools. The
        // wire-side `id` on each becomes the per-call `tool_use_id`,
        // load-bearing for the next prompt's `tool_result` pairing.
        let calls = vec![
            ToolCall {
                id: "toolu_a".into(),
                name: "call_tool".into(),
                arguments: json!({
                    "name": "read",
                    "args": {"path": "a.md"},
                    "claim_seed": "seed-a",
                }),
            },
            ToolCall {
                id: "toolu_b".into(),
                name: "call_tool".into(),
                arguments: json!({
                    "name": "read",
                    "args": {"path": "b.md"},
                    "claim_seed": "seed-b",
                }),
            },
            ToolCall {
                id: "toolu_c".into(),
                name: "call_tool".into(),
                arguments: json!({
                    "name": "read",
                    "args": {"path": "c.md"},
                    "claim_seed": "seed-c",
                }),
            },
        ];
        let d = parse_decision(&calls).unwrap();
        match d {
            Decision::CallTools { calls: tc } => {
                assert_eq!(tc.len(), 3);
                assert_eq!(tc[0].name, "read");
                assert_eq!(tc[0].args, json!({"path": "a.md"}));
                assert_eq!(tc[0].claim_seed, ClaimSeed::new("seed-a"));
                assert_eq!(tc[0].tool_use_id.as_deref(), Some("toolu_a"));
                assert_eq!(tc[2].tool_use_id.as_deref(), Some("toolu_c"));
            }
            other => panic!("expected CallTools, got {other:?}"),
        }
    }

    #[test]
    fn parse_write_output_round_trips() {
        let cite = "evidence/echo-aabbccdd.json";
        let tc = call(
            "write_output",
            json!({
                "body": "hello",
                "citations": [cite]
            }),
        );
        let d = parse_decision(&[tc]).unwrap();
        assert_eq!(
            d,
            Decision::WriteOutput {
                body: "hello".into(),
                citations: vec![cite.to_string()],
            }
        );
    }

    #[test]
    fn parse_write_output_accepts_empty_citations() {
        let tc = call("write_output", json!({"body": "draft", "citations": []}));
        let d = parse_decision(&[tc]).unwrap();
        assert_eq!(
            d,
            Decision::WriteOutput {
                body: "draft".into(),
                citations: vec![],
            }
        );
    }

    #[test]
    fn parse_rewrite_fs_round_trips() {
        let tc = call(
            "rewrite_fs",
            json!({
                "ops": [
                    {"op": "write_file", "path": "notes/a.md", "content": "hi"},
                    {"op": "delete_file", "path": "notes/old.md"}
                ]
            }),
        );
        let d = parse_decision(&[tc]).unwrap();
        assert_eq!(
            d,
            Decision::RewriteFs {
                ops: vec![
                    FsOp::WriteFile {
                        path: "notes/a.md".into(),
                        content: "hi".into(),
                    },
                    FsOp::DeleteFile {
                        path: "notes/old.md".into(),
                    },
                ],
            }
        );
    }

    #[test]
    fn parse_idle_round_trips_ms() {
        let tc = call("idle", json!({"next_after": 2500}));
        let d = parse_decision(&[tc]).unwrap();
        assert_eq!(
            d,
            Decision::Idle {
                next_after: Duration::from_millis(2500),
            }
        );
    }

    #[test]
    fn parse_read_round_trips() {
        let d = parse_decision(&[call("read", json!({"path": "notes/a.md"}))]).unwrap();
        assert_eq!(
            d,
            Decision::Read {
                path: "notes/a.md".into()
            }
        );
    }

    #[test]
    fn parse_list_round_trips() {
        let d = parse_decision(&[call("list", json!({"path": "notes/"}))]).unwrap();
        assert_eq!(
            d,
            Decision::List {
                path: "notes/".into()
            }
        );
    }

    #[test]
    fn parse_search_round_trips_with_and_without_path() {
        let scoped =
            parse_decision(&[call("search", json!({"query": "tsmc", "path": "notes/"}))]).unwrap();
        assert_eq!(
            scoped,
            Decision::Search {
                query: "tsmc".into(),
                path: Some("notes/".into()),
            }
        );
        // `path` is optional — a query-only search is valid.
        let unscoped = parse_decision(&[call("search", json!({"query": "tsmc"}))]).unwrap();
        assert_eq!(
            unscoped,
            Decision::Search {
                query: "tsmc".into(),
                path: None,
            }
        );
    }

    #[test]
    fn retire_is_not_in_the_model_vocabulary() {
        // Persistence is universal: a model cannot self-terminate. `retire`
        // is neither advertised nor parseable; a `retire` tool call is an
        // unknown tool.
        assert!(
            !decision_tools().iter().any(|t| t.name == "retire"),
            "`retire` must not be advertised to the model"
        );
        assert!(!TOOL_NAMES.contains(&"retire"));
        let err = parse_decision(&[call("retire", json!({"reason": "done"}))]).unwrap_err();
        assert_eq!(err, DecisionParseError::UnknownTool("retire".into()));
    }

    #[test]
    fn every_decision_tools_name_parses() {
        // Round-trip the metadata: every name `decision_tools()` advertises
        // is one `parse_decision` accepts. Catches drift between the schema
        // list and the parser's allowed-names table. `call_tool` folds into
        // the `call_tools` variant tag (the parallel-tool shape); every
        // other tool maps one-to-one with its own snake_case variant tag.
        let agent_ref_minimal = json!({
            "workflow_id": "graphs/g1/agents/c1",
            "agent_id": "550e8400-e29b-41d4-a716-446655440000",
        });
        let mandate_minimal = json!({
            "text": "",
            "idle_period": 0,
        });
        for spec in decision_tools() {
            let minimal: Value = match spec.name.as_str() {
                "call_tool" => json!({
                    "name": "noop",
                    "args": {},
                    "claim_seed": "s"
                }),
                "write_output" => json!({"body": "", "citations": []}),
                "rewrite_fs" => json!({"ops": []}),
                "read" => json!({"path": "notes/a.md"}),
                "list" => json!({"path": "notes/"}),
                "search" => json!({"query": "q"}),
                "idle" => json!({"next_after": 0}),
                "spawn_child" => json!({
                    "agent_name": "child",
                    "mandate": mandate_minimal,
                }),
                "reconcile_children" => json!({
                    "sources": [
                        {"child_ref": agent_ref_minimal, "output_id": "ab".repeat(32)},
                    ],
                }),
                "retire_child" => json!({
                    "child_ref": agent_ref_minimal,
                    "reason": "no longer needed",
                }),
                "replace_child" => json!({
                    "child_ref": agent_ref_minimal,
                    "new_mandate": mandate_minimal,
                }),
                other => panic!("unhandled tool {other}"),
            };
            let tc = call(&spec.name, minimal);
            let d = parse_decision(&[tc]).expect(&spec.name);
            let back = serde_json::to_value(&d).unwrap();
            let expected_variant = if spec.name == "call_tool" {
                "call_tools"
            } else {
                spec.name.as_str()
            };
            assert_eq!(
                back.get("type").and_then(Value::as_str),
                Some(expected_variant),
                "tool {} should parse to variant {expected_variant}",
                spec.name,
            );
        }
    }

    // ---- per-variant parse round-trips (parent-child topology) ----------

    #[test]
    fn parse_spawn_child_round_trips() {
        let mandate = json!({
            "text": "fetch foo",
            "idle_period": 500,
        });
        let tc = call(
            "spawn_child",
            json!({"agent_name": "fetcher", "mandate": mandate}),
        );
        let d = parse_decision(&[tc]).unwrap();
        match d {
            Decision::SpawnChild {
                agent_name,
                mandate,
            } => {
                assert_eq!(agent_name, "fetcher");
                assert_eq!(mandate.text, "fetch foo");
                assert_eq!(mandate.idle_period, Some(Duration::from_millis(500)));
            }
            other => panic!("expected SpawnChild, got {other:?}"),
        }
    }

    #[test]
    fn parse_reconcile_children_round_trips_with_no_conflict() {
        let agent_ref = json!({
            "workflow_id": "graphs/g1/agents/c1",
            "agent_id": "550e8400-e29b-41d4-a716-446655440000",
        });
        let oid = "ab".repeat(32);
        let tc = call(
            "reconcile_children",
            json!({"sources": [{"child_ref": agent_ref, "output_id": oid}]}),
        );
        let d = parse_decision(&[tc]).unwrap();
        match d {
            Decision::ReconcileChildren { sources, conflict } => {
                assert_eq!(sources.len(), 1);
                assert!(conflict.is_none());
                assert_eq!(sources[0].child_ref.workflow_id, "graphs/g1/agents/c1");
            }
            other => panic!("expected ReconcileChildren, got {other:?}"),
        }
    }

    #[test]
    fn parse_reconcile_children_round_trips_with_conflict_and_resolution() {
        let agent_ref_a = json!({
            "workflow_id": "graphs/g1/agents/c1",
            "agent_id": "550e8400-e29b-41d4-a716-446655440000",
        });
        let agent_ref_b = json!({
            "workflow_id": "graphs/g1/agents/c2",
            "agent_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        });
        let oid_a = "ab".repeat(32);
        let oid_b = "cd".repeat(32);
        let tc = call(
            "reconcile_children",
            json!({
                "sources": [
                    {"child_ref": agent_ref_a, "output_id": oid_a},
                    {"child_ref": agent_ref_b, "output_id": oid_b},
                ],
                "conflict": {
                    "alternatives": [
                        {
                            "source_child": agent_ref_a,
                            "source_output_id": oid_a,
                            "claim": "value is 42",
                        },
                        {
                            "source_child": agent_ref_b,
                            "source_output_id": oid_b,
                            "claim": "value is 43",
                        },
                    ],
                    "resolution": {
                        "chosen_alternative_idx": 0,
                        "reasoning": "primary source",
                    },
                },
            }),
        );
        let d = parse_decision(&[tc]).unwrap();
        match d {
            Decision::ReconcileChildren { sources, conflict } => {
                assert_eq!(sources.len(), 2);
                let c = conflict.expect("expected conflict block");
                assert_eq!(c.alternatives.len(), 2);
                assert_eq!(c.alternatives[0].claim, "value is 42");
                let r = c.resolution.expect("expected resolution");
                assert_eq!(r.chosen_alternative_idx, 0);
                assert_eq!(r.reasoning, "primary source");
            }
            other => panic!("expected ReconcileChildren, got {other:?}"),
        }
    }

    #[test]
    fn parse_retire_child_round_trips() {
        let agent_ref = json!({
            "workflow_id": "graphs/g1/agents/c1",
            "agent_id": "550e8400-e29b-41d4-a716-446655440000",
        });
        let tc = call(
            "retire_child",
            json!({"child_ref": agent_ref, "reason": "stop"}),
        );
        let d = parse_decision(&[tc]).unwrap();
        match d {
            Decision::RetireChild { child_ref, reason } => {
                assert_eq!(child_ref.workflow_id, "graphs/g1/agents/c1");
                assert_eq!(reason, "stop");
            }
            other => panic!("expected RetireChild, got {other:?}"),
        }
    }

    #[test]
    fn parse_replace_child_round_trips() {
        let agent_ref = json!({
            "workflow_id": "graphs/g1/agents/c1",
            "agent_id": "550e8400-e29b-41d4-a716-446655440000",
        });
        let mandate = json!({
            "text": "v2",
            "idle_period": 100,
        });
        let tc = call(
            "replace_child",
            json!({"child_ref": agent_ref, "new_mandate": mandate}),
        );
        let d = parse_decision(&[tc]).unwrap();
        match d {
            Decision::ReplaceChild {
                child_ref,
                new_mandate,
            } => {
                assert_eq!(child_ref.workflow_id, "graphs/g1/agents/c1");
                assert_eq!(new_mandate.text, "v2");
                assert_eq!(new_mandate.idle_period, Some(Duration::from_millis(100)));
            }
            other => panic!("expected ReplaceChild, got {other:?}"),
        }
    }

    #[test]
    fn parse_spawn_child_missing_mandate_errors_with_structured_missing_field() {
        let tc = call("spawn_child", json!({"agent_name": "fetcher"}));
        let err = parse_decision(&[tc]).unwrap_err();
        assert_eq!(
            err,
            DecisionParseError::MissingField {
                tool: "spawn_child".into(),
                field: "mandate".into(),
            }
        );
    }

    #[test]
    fn parse_reconcile_children_mixed_with_terminal_errors() {
        // `reconcile_children` is a terminal singleton — pairing it with
        // another terminal in the same response is a mixed-shape error.
        let agent_ref = json!({
            "workflow_id": "graphs/g1/agents/c1",
            "agent_id": "550e8400-e29b-41d4-a716-446655440000",
        });
        let err = parse_decision(&[
            call(
                "reconcile_children",
                json!({"sources": [{"child_ref": agent_ref, "output_id": "ab".repeat(32)}]}),
            ),
            call("idle", json!({"next_after": 0})),
        ])
        .unwrap_err();
        assert!(
            matches!(
                err,
                DecisionParseError::DuplicateTerminalTool { count: 2, .. }
            ),
            "got {err:?}"
        );
    }

    // ---- vendor-shape fixture (Cohere) ----------------------------------

    #[test]
    fn parse_accepts_cohere_shaped_tool_call() {
        // Cohere's tool-call wire shape reduces to the same vendor-neutral
        // `{id, name, arguments}` the trait surface uses; vendor adapters
        // do field-name normalization (e.g. Cohere's `parameters` →
        // `arguments`) before this layer sees the call. The fixture is
        // shaped the way a Cohere `ModelClient` impl would emit it; the
        // parser stays vendor-agnostic because its input is already
        // normalized.
        let cohere_style = ToolCall {
            id: "tool_call_abc".into(),
            name: "idle".into(),
            arguments: json!({"next_after": 1000}),
        };
        let d = parse_decision(&[cohere_style]).unwrap();
        assert_eq!(
            d,
            Decision::Idle {
                next_after: Duration::from_millis(1000),
            }
        );
    }

    // ---- error variants -------------------------------------------------

    #[test]
    fn parse_no_calls_errors() {
        let err = parse_decision(&[]).unwrap_err();
        assert_eq!(err, DecisionParseError::NoCalls);
    }

    #[test]
    fn parse_mixed_call_tool_and_terminal_errors() {
        // `call_tool` is the parallel-call shape; mixing it with any
        // terminal decision (`write_output`, `rewrite_fs`, `idle`) is never
        // a valid single-tick decision.
        let err = parse_decision(&[
            call(
                "call_tool",
                json!({"name": "echo", "args": {}, "claim_seed": "s"}),
            ),
            call("idle", json!({"next_after": 0})),
        ])
        .unwrap_err();
        assert!(
            matches!(err, DecisionParseError::MixedDecisionTools { ref names }
                if names == &["call_tool".to_string(), "idle".to_string()]),
            "got {err:?}"
        );
    }

    #[test]
    fn parse_duplicate_terminal_tools_errors() {
        // Two `idle` blocks in one response — terminals are singular.
        let err = parse_decision(&[
            call("idle", json!({"next_after": 1})),
            call("idle", json!({"next_after": 2})),
        ])
        .unwrap_err();
        assert!(
            matches!(err, DecisionParseError::DuplicateTerminalTool { ref tool, count: 2 }
                if tool == "idle"),
            "got {err:?}"
        );
    }

    #[test]
    fn parse_unknown_tool_errors() {
        let err = parse_decision(&[call("send_email", json!({}))]).unwrap_err();
        assert_eq!(err, DecisionParseError::UnknownTool("send_email".into()));
    }

    #[test]
    fn parse_missing_field_errors() {
        let err = parse_decision(&[call("idle", json!({}))]).unwrap_err();
        assert_eq!(
            err,
            DecisionParseError::MissingField {
                tool: "idle".into(),
                field: "next_after".into(),
            }
        );
    }

    #[test]
    fn parse_invalid_value_errors() {
        // `next_after` must deserialize as an integer; a string is a shape
        // error that surfaces as `InvalidValue`.
        let err = parse_decision(&[call("idle", json!({"next_after": "soon"}))]).unwrap_err();
        match err {
            DecisionParseError::InvalidValue { tool, reason, .. } => {
                assert_eq!(tool, "idle");
                assert!(!reason.is_empty(), "reason should carry serde detail");
            }
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn parse_arguments_not_object_errors() {
        let bogus = ToolCall {
            id: "x".into(),
            name: "idle".into(),
            arguments: json!([1, 2, 3]),
        };
        let err = parse_decision(&[bogus]).unwrap_err();
        assert_eq!(
            err,
            DecisionParseError::ArgumentsNotObject {
                tool: "idle".into(),
            }
        );
    }

    #[test]
    fn parse_call_tool_missing_inner_field_errors() {
        // The inner `{name, args, claim_seed}` block carries its own
        // required-field check inside `parse_call_tool_args`. A missing
        // `claim_seed` must surface as `MissingField` with the inner
        // field name, not as a serde shape error against the outer
        // CallTools variant.
        let err = parse_decision(&[call(
            "call_tool",
            json!({"name": "echo", "args": {"msg": "hi"}}),
        )])
        .unwrap_err();
        assert!(
            matches!(err, DecisionParseError::MissingField { ref tool, ref field }
                if tool == "call_tool" && field == "claim_seed"),
            "got {err:?}"
        );
    }

    #[test]
    fn parse_invalid_inner_fs_op_errors() {
        // `ops[0].op` is required by `FsOp`'s internally-tagged enum.
        // Missing it is a shape error that surfaces via serde, not via
        // our up-front field check (which only inspects the top level).
        let tc = call("rewrite_fs", json!({"ops": [{"path": "x"}]}));
        let err = parse_decision(&[tc]).unwrap_err();
        assert!(
            matches!(err, DecisionParseError::InvalidValue { ref tool, .. } if tool == "rewrite_fs"),
            "got {err:?}"
        );
    }
}
