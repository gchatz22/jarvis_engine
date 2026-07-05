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

use coral_node::decide_llm::{decision_tools, parse_decision, render};
use coral_node::decision::{Decision, FsIndex, Seed, Session};
use coral_node::mandate::Mandate;
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
    tools: Vec<String>,
    only_tools: Vec<String>,
    call_tool_variant: String,
    flat: bool,
    verbose: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut model = "north-mini-code-1-0".to_string();
    let mut trials = 5u32;
    let mut temperature = None;
    let mut max_tokens = 1024u32;
    let mut mandate = DEFAULT_MANDATE.to_string();
    let mut tools = Vec::new();
    let mut only_tools = Vec::new();
    let mut call_tool_variant = "baseline".to_string();
    let mut flat = false;
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
            "--verbose" | "-v" => verbose = true,
            "--help" | "-h" => return Err("usage: cohere-bench [--model ID] [--trials N] [--temperature F] [--max-tokens N] [--mandate-file PATH] [--tools a,b,c] [--only-tools a,b,c] [--call-tool-variant baseline|typed-args|no-claim-seed|lean] [--verbose]".to_string()),
            other => return Err(format!("unknown flag: {other}")),
        }
    }
    Ok(Args {
        model,
        trials,
        temperature,
        max_tokens,
        mandate,
        tools,
        only_tools,
        call_tool_variant,
        flat,
        verbose,
    })
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
    let session = kickoff_session(&args.mandate, &args.tools);
    let messages = render(&session);
    let mut tools = decision_tools();
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

    println!(
        "model={} trials={} max_tokens={} temperature={:?}",
        args.model, args.trials, args.max_tokens, args.temperature
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
    std::process::ExitCode::SUCCESS
}
