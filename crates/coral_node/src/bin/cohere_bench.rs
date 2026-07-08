//! Decision-generation benchmark for a single model.
//!
//! Isolates one question the live graph can't answer cleanly: *how reliably
//! does a given model emit a valid `Decision` for our tool-calling protocol?*
//! It sends the real prompt (`render`) and the real tool schema
//! (`decision_tools`) for a synthetic agent wake, then runs the response
//! through the real parser (`parse_decision`) — the exact path a live agent
//! takes, minus Temporal, MCP, and the retry loop, so the base success rate
//! is visible per call.
//!
//! Use it to iterate on what makes a weak model succeed: tweak the mandate
//! (`--mandate-file`), the sampling (`--temperature`, `--max-tokens`), the
//! system prompt (`decide_llm/system_prompt.md`), or the tool schema
//! (`decide_llm/schema.rs`) and re-run to watch the rate move.
//!
//! ```text
//! COHERE_API_KEY=... cargo run --features llm-cohere --bin cohere-bench -- \
//!   --model north-mini-code-1-0 --trials 10 --verbose
//! ```
//!
//! Reports, over N trials: how many produced a valid decision, how many
//! parsed but were malformed, and how many failed at the API (e.g. Cohere's
//! `INVALID_TOOL_GENERATION`). `--verbose` dumps each trial's raw output.

use std::time::Duration;

use std::str::FromStr;

use coral_node::agent_ref::{AgentId, AgentRef};
use coral_node::decide_llm::{decision_tools, parse_decision, render};
use coral_node::decision::{Decision, FsIndex, Observation, ReconcileSource, Seed, Session};
use coral_node::mandate::{Mandate, OutputId};
use coral_node::model_client::cohere::CohereClient;
use coral_node::model_client::{
    CompleteOptions, CompleteRequest, ContentBlock, ModelClient, ToolSpec,
};
use coral_node::trigger::Trigger;
use serde_json::{json, Value};

/// Default mandate: a narrow child researcher, the shape that failed live.
const DEFAULT_MANDATE: &str = "\
You argue one side of a technical decision: the case for choosing Rust to \
build a high-throughput API backend. Make the strongest honest case — \
predictable tail latency, memory safety without a runtime, compiler-checked \
concurrency — and name the real costs (compile times, the learning curve). \
Stay strictly on the Rust side; your deliverable is a short, decisive brief.";

struct Args {
    model: String,
    trials: u32,
    temperature: Option<f32>,
    max_tokens: u32,
    mandate: String,
    scenario: String,
    tools: Vec<String>,
    only_tools: Vec<String>,
    call_tool_variant: String,
    flat: bool,
    flatten: bool,
    verbose: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut model = "north-mini-code-1-0".to_string();
    let mut trials = 5u32;
    let mut temperature = None;
    let mut max_tokens = 1024u32;
    let mut mandate = DEFAULT_MANDATE.to_string();
    let mut scenario = "kickoff".to_string();
    let mut tools = Vec::new();
    let mut only_tools = Vec::new();
    let mut call_tool_variant = "baseline".to_string();
    let mut flat = false;
    let mut flatten = false;
    let mut verbose = false;

    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        let mut val = || {
            argv.next()
                .ok_or_else(|| format!("flag {flag} needs a value"))
        };
        match flag.as_str() {
            "--model" => model = val()?,
            "--trials" => trials = val()?.parse().map_err(|e| format!("--trials: {e}"))?,
            "--temperature" => {
                temperature = Some(val()?.parse().map_err(|e| format!("--temperature: {e}"))?)
            }
            "--max-tokens" => max_tokens = val()?.parse().map_err(|e| format!("--max-tokens: {e}"))?,
            "--mandate-file" => {
                let path = val()?;
                mandate = std::fs::read_to_string(&path)
                    .map_err(|e| format!("reading {path}: {e}"))?;
            }
            "--scenario" => scenario = val()?,
            "--tools" => {
                tools = val()?
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
            "--only-tools" => {
                only_tools = val()?
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
            "--call-tool-variant" => call_tool_variant = val()?,
            "--flat" => flat = true,
            "--flatten" => flatten = true,
            "--verbose" | "-v" => verbose = true,
            "--help" | "-h" => return Err("usage: cohere-bench [--model ID] [--trials N] [--temperature F] [--max-tokens N] [--mandate-file PATH] [--scenario kickoff|reconciled-parent] [--tools a,b,c] [--only-tools a,b,c] [--call-tool-variant baseline|typed-args|no-claim-seed|lean] [--verbose]".to_string()),
            other => return Err(format!("unknown flag: {other}")),
        }
    }
    Ok(Args {
        model,
        trials,
        temperature,
        max_tokens,
        mandate,
        scenario,
        tools,
        only_tools,
        call_tool_variant,
        flat,
        flatten,
        verbose,
    })
}

/// A typed, strict-compatible runtime tool spec used to exercise the adapter's
/// real flatten path (`--flatten`): populated into `CompleteRequest.runtime_tools`
/// so `build_body` offers it first-class and turns on `strict_tools`.
fn flatten_runtime_tools() -> Vec<ToolSpec> {
    vec![ToolSpec {
        name: "web_search".into(),
        description: "Search the web and return sources for a query.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "query": { "type": "string", "description": "The search query." }
            },
            "required": ["query"]
        }),
    }]
}

/// Build a `call_tool` input schema for one experiment variant, so a run can
/// A/B what makes Cohere's tool-generation validator reject the call. The
/// production schema (`decision_tools`) is the `baseline`; the others drop or
/// tighten one field at a time to isolate the 422 trigger.
fn call_tool_schema(variant: &str) -> Value {
    let name = json!({
        "type": "string",
        "description": "Name of the runtime tool to invoke."
    });
    let claim_seed = json!({
        "type": "string",
        "description": "Opaque seed linking the resulting evidence to the claim."
    });
    let args_untyped =
        json!({ "description": "JSON arguments for the tool. Shape is tool-specific." });
    let args_typed = json!({ "type": "object", "description": "JSON arguments for the tool." });
    match variant {
        "typed-args" => json!({
            "type": "object",
            "properties": { "name": name, "args": args_typed, "claim_seed": claim_seed },
            "required": ["name", "args", "claim_seed"]
        }),
        "no-claim-seed" => json!({
            "type": "object",
            "properties": { "name": name, "args": args_untyped },
            "required": ["name", "args"]
        }),
        "lean" => json!({
            "type": "object",
            "properties": { "name": name, "args": args_typed },
            "required": ["name", "args"]
        }),
        // baseline: the production shape verbatim (untyped args + claim_seed).
        _ => json!({
            "type": "object",
            "properties": { "name": name, "args": args_untyped, "claim_seed": claim_seed },
            "required": ["name", "args", "claim_seed"]
        }),
    }
}

/// Build the prompt a fresh child sees on its kickoff wake: its mandate, its
/// granted tools, a kickoff trigger, and an empty file index.
///
/// The tool grant is load-bearing for fidelity: with no tools the model
/// cannot call anything, cannot mint evidence, and so cannot write a sourced
/// Output — leaving `read`/`list` as the only non-failing moves. A realistic
/// grant is what makes "gather evidence, then write" an available path.
fn kickoff_session(mandate_text: &str, tools: &[String]) -> Session {
    let mut mandate = Mandate::new(mandate_text, Duration::from_secs(60), None);
    mandate.tools = tools.to_vec();
    let kickoff = Trigger::External {
        kind: "kickoff".to_string(),
        payload: json!({}),
    };
    Session::new(Seed::new(mandate, vec![kickoff], FsIndex::default()))
}

/// The synthesist parent's mandate: a two-sided judgment that folds each of
/// two named children's briefs as they arrive. Names both children so the
/// reconciled-parent scenario reproduces the exact confusion the live parent
/// hit — one child folded, the *other* not yet reported.
const SYNTHESIST_MANDATE: &str = "\
You own a two-sided technical decision: Rust vs Go for a high-throughput API \
backend. Two researchers report to you, each arguing one side: `rust-advocate` \
(the case for Rust) and `go-advocate` (the case for Go). They already exist and \
send you their briefs on their own; your role is to weigh what arrives. As each \
brief comes in, fold it into a single recommendation that commits to a clear \
default choice and names the condition under which the other language wins.";

/// Build the prompt a parent sees at the decision point where the live wander
/// began: it has just folded ONE of its two children (`go-advocate`) via a
/// `ReconcileChildren` step, and the observation names the citable synthetic
/// evidence record. The *other* child (`rust-advocate`) has not reported yet —
/// its brief arrives later as a `ChildOutput` signal, not as a file on disk.
///
/// The convergent move here is to `write_output` (an honest single-sided view
/// citing the folded brief) and `idle`, or to `idle` and wait for the sibling.
/// The live model instead looped `list evidence` / `search rust-advocate`,
/// hunting its own filesystem for a brief that isn't there. This scenario makes
/// that fork measurable over N trials.
fn reconciled_parent_session() -> Session {
    let mut mandate = Mandate::new(SYNTHESIST_MANDATE, Duration::from_secs(60), None);
    mandate.tools = Vec::new();
    let child_ref = AgentRef::new(
        "graphs/demo/agents/0c37c003-90b5-464a-8aa6-856a4562fd8d",
        AgentId::from_str("0c37c003-90b5-464a-8aa6-856a4562fd8d").expect("valid child uuid"),
    );
    let output_id = OutputId::from_hex("ab".repeat(32));
    let wake = Trigger::ChildOutput {
        child_ref: child_ref.clone(),
        agent_name: "go-advocate".to_string(),
        output_id: output_id.clone(),
    };
    let mut session = Session::new(Seed::new(mandate, vec![wake], FsIndex::default()));
    session.push(
        Decision::ReconcileChildren {
            sources: vec![ReconcileSource {
                child_ref,
                output_id,
            }],
            conflict: None,
        },
        Observation::ok(
            "Reconciled go-advocate's output into \
             evidence/reconcile-go-advocate-8000b19a.json. go-advocate argued: Go is the \
             optimal foundation for high-throughput API backends — developer velocity (small \
             language, fast compiles), concurrency built into the language (goroutines and \
             channels), and a battle-tested net/http stack; the honest cost is GC latency \
             tails under heavy load. To emit an Output, cite \
             evidence/reconcile-go-advocate-8000b19a.json in write_output.",
        ),
    );
    session
}

fn content_summary(content: &[ContentBlock]) -> String {
    if content.is_empty() {
        return "<empty>".to_string();
    }
    content
        .iter()
        .map(|b| match b {
            ContentBlock::Text { text } => format!("text({} chars)", text.len()),
            ContentBlock::ToolUse { name, .. } => format!("tool_use({name})"),
            ContentBlock::ToolResult { .. } => "tool_result".to_string(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn decision_kind(d: &Decision) -> &'static str {
    match d {
        Decision::CallTools { .. } => "call_tools",
        Decision::WriteOutput { .. } => "write_output",
        Decision::RewriteFs { .. } => "rewrite_fs",
        Decision::Read { .. } => "read",
        Decision::List { .. } => "list",
        Decision::Search { .. } => "search",
        Decision::SpawnChild { .. } => "spawn_child",
        Decision::ReconcileChildren { .. } => "reconcile_children",
        Decision::RetireChild { .. } => "retire_child",
        Decision::ReplaceChild { .. } => "replace_child",
        Decision::Idle { .. } => "idle",
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            return std::process::ExitCode::from(2);
        }
    };
    if std::env::var("COHERE_API_KEY").is_err() {
        eprintln!("COHERE_API_KEY is not set");
        return std::process::ExitCode::from(2);
    }

    let client = CohereClient::new().with_model(&args.model);
    let is_parent = args.scenario == "reconciled-parent";
    let session = if is_parent {
        reconciled_parent_session()
    } else {
        kickoff_session(&args.mandate, &args.tools)
    };
    let messages = render(&session);
    // The parent scenario grants no runtime tools, so `call_tool` is not
    // offered (matches the live parent); the kickoff scenario keeps the full
    // decision surface.
    let mut tools = decision_tools(!is_parent);
    // Restrict the offered decision-tool set, to concentrate `call_tool`
    // attempts (fewer navigation escape hatches) and to probe whether a
    // smaller surface itself changes the rejection rate.
    if !args.only_tools.is_empty() {
        tools.retain(|t| args.only_tools.contains(&t.name));
    }
    // Swap the `call_tool` schema for the chosen variant so a run can A/B
    // what the tool-generation validator rejects.
    if let Some(ct) = tools
        .iter_mut()
        .find(|t: &&mut ToolSpec| t.name == "call_tool")
    {
        ct.input_schema = call_tool_schema(&args.call_tool_variant);
    }
    // Flatten mode: replace the whole decision-tool surface with a single
    // first-class, fully-typed runtime tool. Tests whether the model emits a
    // constrained tool call cleanly when there is no generic `call_tool`
    // wrapper — the shape `strict_tools` (grammar-constrained decoding) needs.
    if args.flat {
        tools = vec![ToolSpec {
            name: "web_search".into(),
            description: "Search the web and return sources for a query.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "The search query." }
                },
                "required": ["query"],
                "additionalProperties": false
            }),
        }];
    }
    let options = CompleteOptions {
        max_tokens: args.max_tokens,
        temperature: args.temperature,
    };
    // `--flatten` engages the adapter's real flatten path: the runtime tool is
    // offered first-class and `strict_tools` is turned on, so the whole offered
    // decision set is validated by Cohere. A 400 names any non-strict-compatible
    // tool; clean generation collapses back to `call_tool`.
    let runtime_tools = if args.flatten {
        flatten_runtime_tools()
    } else {
        Vec::new()
    };

    println!(
        "model={} scenario={} trials={} max_tokens={} temperature={:?} flatten={}",
        args.model, args.scenario, args.trials, args.max_tokens, args.temperature, args.flatten
    );
    let granted = if args.tools.is_empty() {
        "(none)".to_string()
    } else {
        args.tools.join(", ")
    };
    let offered = tools
        .iter()
        .map(|t| t.name.as_str())
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "decision tools offered: {} [{offered}] | call_tool variant: {} | tools granted: {granted}",
        tools.len(),
        args.call_tool_variant,
    );

    let mut valid = 0u32;
    let mut parse_failed = 0u32;
    let mut api_failed = 0u32;
    let mut kinds: std::collections::BTreeMap<&'static str, u32> =
        std::collections::BTreeMap::new();

    for trial in 1..=args.trials {
        let req = CompleteRequest {
            messages: messages.clone(),
            tools: tools.clone(),
            options: options.clone(),
            runtime_tools: runtime_tools.clone(),
            model: None,
        };
        match client.complete(req).await {
            Ok(resp) => {
                if args.verbose {
                    println!(
                        "  [{trial}] content=[{}] tool_calls={} in={} out={}",
                        content_summary(&resp.content),
                        resp.tool_calls.len(),
                        resp.usage.input_tokens,
                        resp.usage.output_tokens,
                    );
                    for tc in &resp.tool_calls {
                        println!("       call {} {}", tc.name, tc.arguments);
                    }
                }
                match parse_decision(&resp.tool_calls) {
                    Ok(d) => {
                        valid += 1;
                        *kinds.entry(decision_kind(&d)).or_default() += 1;
                        println!("  [{trial}] OK  -> {}", decision_kind(&d));
                    }
                    Err(e) => {
                        parse_failed += 1;
                        println!("  [{trial}] PARSE FAIL -> {e}");
                    }
                }
            }
            Err(e) => {
                api_failed += 1;
                println!("  [{trial}] API FAIL -> {e}");
            }
        }
    }

    println!("\n--- summary ---");
    println!(
        "valid decisions: {valid}/{}  |  parse failures: {parse_failed}  |  API failures: {api_failed}",
        args.trials
    );
    if !kinds.is_empty() {
        let breakdown = kinds
            .iter()
            .map(|(k, n)| format!("{k}={n}"))
            .collect::<Vec<_>>()
            .join(", ");
        println!("chosen decisions: {breakdown}");
    }
    // For a parent that has already folded a child, the convergent moves are
    // `write_output` (an honest single-sided view) and `idle`; the wander is
    // re-inspecting the filesystem for a sibling brief that isn't there yet.
    // Report the split so a prompt/tool variant's effect is a single number.
    if is_parent {
        let converged: u32 = ["write_output", "idle"]
            .iter()
            .filter_map(|k| kinds.get(k))
            .sum();
        let wander: u32 = ["read", "list", "search"]
            .iter()
            .filter_map(|k| kinds.get(k))
            .sum();
        println!(
            "parent decision split: converged(write_output+idle)={converged}  \
             wander(read+list+search)={wander}  of {valid} valid"
        );
    }
    std::process::ExitCode::SUCCESS
}
