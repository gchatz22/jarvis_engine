//! `model-call` — one-shot DX CLI for exercising a `ModelClient` impl.
//!
//! Companion to `compute-evidence-id`: a hand-runnable binary for sanity-
//! checking `ModelClient` adapters against a real vendor without writing a
//! custom test or booting the agent loop. Useful for prompt iteration,
//! vendor parity, and confirming auth + wire format end-to-end. Not part
//! of the agent runtime.
//!
//! # Usage
//!
//! ```text
//! model-call --vendor <anthropic|cohere> [--model <id>] [--system <text>] \
//!     [--max-tokens N] (--prompt <text> | --from-stdin)
//! ```
//!
//! * `--vendor anthropic` — requires the `llm-anthropic` feature.
//! * `--vendor cohere` — requires the `llm-cohere` feature.
//! * `--model <id>` — overrides the impl's default model. Defaults are
//!   whatever the vendor adapter selects (`AnthropicClient` →
//!   `claude-haiku-4-5`; `CohereClient` → `command-a-03-2025`), unless
//!   `ANTHROPIC_MODEL` / `COHERE_MODEL` is set in the environment.
//! * `--system <text>` — optional system prompt.
//! * `--max-tokens N` — sampling cap, defaults to 1024.
//! * `--prompt <text>` / `--from-stdin` — exactly one is required. Stdin
//!   mode reads to EOF and uses the entire blob as the user message.
//!
//! Auth: `ANTHROPIC_API_KEY` for Anthropic; `COHERE_API_KEY` for Cohere.
//! The adapters bubble a `ModelError::Auth` if missing, surfaced verbatim.
//!
//! # Feature gating
//!
//! Cargo's `required-features` does not support OR, so the per-vendor
//! dispatch arms below are gated with `#[cfg(feature = "llm-...")]`; a
//! build with neither feature still compiles the binary but every
//! `--vendor` choice errors at runtime with a "rebuild with --features
//! ..." hint.
//!
//! # Output split
//!
//! Stdout stays clean for shell pipelines: it contains the model's text
//! content blocks (joined with newlines) and, if any tool_calls came back,
//! a labelled `=== tool_calls ===` section with pretty-printed JSON. Usage
//! counts, the model id, and request latency go to stderr.

use std::io::{self, Read};
use std::process::ExitCode;
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};

use coral_node::model_client::{CompleteOptions, CompleteRequest, Message};
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use coral_node::model_client::{ContentBlock, ModelClient, Usage};

#[cfg(feature = "llm-anthropic")]
use coral_node::model_client::anthropic::AnthropicClient;
#[cfg(feature = "llm-cohere")]
use coral_node::model_client::cohere::CohereClient;

const USAGE: &str = "\
model-call — one-shot DX CLI for ad-hoc ModelClient inference.

USAGE:
    model-call --vendor <anthropic|cohere> [--model <id>] [--system <text>]
               [--max-tokens N] (--prompt <text> | --from-stdin)

ARGS:
    --vendor <name>     Required. `anthropic` (build with --features llm-anthropic)
                        or `cohere` (build with --features llm-cohere).
    --model <id>        Override the impl's default model id.
    --system <text>     Optional system prompt.
    --max-tokens N      Sampling cap (default: 1024).
    --prompt <text>     One-shot user prompt (mutually exclusive with --from-stdin).
    --from-stdin        Read the user prompt from stdin until EOF.

ENV:
    ANTHROPIC_API_KEY   Required for --vendor anthropic.
    COHERE_API_KEY      Required for --vendor cohere.
    ANTHROPIC_MODEL     Optional. Overrides the Anthropic default model id
                        when --model is not given.
    COHERE_MODEL        Optional. Overrides the Cohere default model id
                        when --model is not given.

OUTPUT:
    stdout              Joined text content blocks; tool_calls section if any.
    stderr              model id, latency_ms, input/output tokens.
";

const DEFAULT_MAX_TOKENS: u32 = 1024;

/// Vendors the CLI surface understands. The set is intentionally closed —
/// new vendors get added here as their impls land.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Vendor {
    Anthropic,
    Cohere,
}

/// Where the user prompt comes from. Resolved at parse time so the test
/// path can introspect the choice without touching real stdin.
#[derive(Clone, Debug, PartialEq, Eq)]
enum PromptSource {
    Literal(String),
    Stdin,
}

/// Parsed CLI arguments, before stdin resolution.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedArgs {
    vendor: Vendor,
    model: Option<String>,
    system: Option<String>,
    max_tokens: u32,
    prompt_source: PromptSource,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("model-call: {e:#}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<()> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.iter().any(|a| a == "-h" || a == "--help") {
        print!("{USAGE}");
        return Ok(());
    }
    let parsed = parse_args(&argv)?;
    let prompt = read_prompt(&parsed, &mut io::stdin().lock())?;
    let request = build_request(&parsed, prompt)?;

    match parsed.vendor {
        Vendor::Anthropic => run_anthropic(&parsed, request),
        Vendor::Cohere => run_cohere(&parsed, request),
    }
}

#[cfg(feature = "llm-anthropic")]
fn run_anthropic(parsed: &ParsedArgs, request: CompleteRequest) -> Result<()> {
    let client = match parsed.model.as_deref() {
        Some(m) => AnthropicClient::new().with_model(m),
        None => AnthropicClient::new(),
    };
    dispatch(&client, client.model().to_string(), request)
}

#[cfg(not(feature = "llm-anthropic"))]
fn run_anthropic(_: &ParsedArgs, _: CompleteRequest) -> Result<()> {
    Err(anyhow!(
        "vendor 'anthropic' is not built into this binary; \
         rebuild with --features llm-anthropic"
    ))
}

#[cfg(feature = "llm-cohere")]
fn run_cohere(parsed: &ParsedArgs, request: CompleteRequest) -> Result<()> {
    let client = match parsed.model.as_deref() {
        Some(m) => CohereClient::new().with_model(m),
        None => CohereClient::new(),
    };
    dispatch(&client, client.model().to_string(), request)
}

#[cfg(not(feature = "llm-cohere"))]
fn run_cohere(_: &ParsedArgs, _: CompleteRequest) -> Result<()> {
    Err(anyhow!(
        "vendor 'cohere' is not built into this binary; \
         rebuild with --features llm-cohere"
    ))
}

/// Vendor-agnostic submit: build a single-thread Tokio runtime, await the
/// model call, print response + diagnostics. Only compiled when at least
/// one vendor adapter is enabled (it would be dead code otherwise).
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
fn dispatch(client: &dyn ModelClient, model_id: String, request: CompleteRequest) -> Result<()> {
    // Single-thread runtime so the rest of the binary stays sync and
    // unit-testable without a runtime. Latency is measured around the await.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;
    let started = Instant::now();
    let response = rt
        .block_on(client.complete(request))
        .map_err(|e| anyhow!("{e}"))?;
    let elapsed = started.elapsed();

    print_response(&response);
    print_diagnostics(&model_id, elapsed, response.usage);
    Ok(())
}

/// Parse argv (without the binary name) into a `ParsedArgs`.
///
/// Hand-rolled to match `compute-evidence-id` style — no `clap`. Each
/// known flag must appear at most once; unknown flags are a hard error.
fn parse_args(argv: &[String]) -> Result<ParsedArgs> {
    let mut vendor: Option<Vendor> = None;
    let mut model: Option<String> = None;
    let mut system: Option<String> = None;
    let mut max_tokens: Option<u32> = None;
    let mut prompt_literal: Option<String> = None;
    let mut from_stdin = false;

    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--vendor" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--vendor requires a value"))?;
                if vendor.is_some() {
                    return Err(anyhow!("--vendor specified more than once"));
                }
                vendor = Some(parse_vendor(v)?);
            }
            "--model" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--model requires a value"))?;
                if model.is_some() {
                    return Err(anyhow!("--model specified more than once"));
                }
                model = Some(v.clone());
            }
            "--system" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--system requires a value"))?;
                if system.is_some() {
                    return Err(anyhow!("--system specified more than once"));
                }
                system = Some(v.clone());
            }
            "--max-tokens" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--max-tokens requires a value"))?;
                if max_tokens.is_some() {
                    return Err(anyhow!("--max-tokens specified more than once"));
                }
                let n: u32 = v
                    .parse()
                    .with_context(|| format!("parsing --max-tokens value `{v}`"))?;
                if n == 0 {
                    return Err(anyhow!("--max-tokens must be > 0"));
                }
                max_tokens = Some(n);
            }
            "--prompt" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--prompt requires a value"))?;
                if prompt_literal.is_some() {
                    return Err(anyhow!("--prompt specified more than once"));
                }
                prompt_literal = Some(v.clone());
            }
            "--from-stdin" => {
                if from_stdin {
                    return Err(anyhow!("--from-stdin specified more than once"));
                }
                from_stdin = true;
            }
            other => {
                return Err(anyhow!("unknown argument `{other}`"));
            }
        }
    }

    let vendor = vendor.ok_or_else(|| anyhow!("--vendor is required"))?;
    let prompt_source = match (prompt_literal, from_stdin) {
        (Some(p), false) => PromptSource::Literal(p),
        (None, true) => PromptSource::Stdin,
        (Some(_), true) => {
            return Err(anyhow!("--prompt and --from-stdin are mutually exclusive"));
        }
        (None, false) => {
            return Err(anyhow!("one of --prompt or --from-stdin is required"));
        }
    };

    Ok(ParsedArgs {
        vendor,
        model,
        system,
        max_tokens: max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
        prompt_source,
    })
}

fn parse_vendor(s: &str) -> Result<Vendor> {
    match s {
        "anthropic" => Ok(Vendor::Anthropic),
        "cohere" => Ok(Vendor::Cohere),
        other => Err(anyhow!(
            "unknown vendor `{other}` (expected `anthropic` or `cohere`)"
        )),
    }
}

/// Resolve `parsed.prompt_source` into a concrete prompt string. The
/// `reader` seam lets the unit test feed bytes without touching real stdin.
fn read_prompt<R: Read>(parsed: &ParsedArgs, reader: &mut R) -> Result<String> {
    match &parsed.prompt_source {
        PromptSource::Literal(s) => Ok(s.clone()),
        PromptSource::Stdin => {
            let mut buf = String::new();
            reader
                .read_to_string(&mut buf)
                .context("reading prompt from stdin")?;
            if buf.is_empty() {
                return Err(anyhow!("--from-stdin received empty input"));
            }
            Ok(buf)
        }
    }
}

/// Translate parsed args + resolved prompt into a `CompleteRequest`. The
/// shape is vendor-agnostic; the per-vendor dispatch in `run` is what
/// chooses which adapter consumes it.
fn build_request(parsed: &ParsedArgs, prompt: String) -> Result<CompleteRequest> {
    let mut messages = Vec::with_capacity(2);
    if let Some(sys) = &parsed.system {
        messages.push(Message::system(sys));
    }
    messages.push(Message::user(prompt));

    Ok(CompleteRequest {
        messages,
        tools: vec![],
        runtime_tools: Vec::new(),
        model: None,
        options: CompleteOptions {
            max_tokens: parsed.max_tokens,
            temperature: None,
        },
    })
}

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
fn print_response(response: &coral_node::model_client::CompleteResponse) {
    let text: Vec<&str> = response
        .content
        .iter()
        .filter_map(|b| match b {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    if !text.is_empty() {
        println!("{}", text.join("\n"));
    }
    if !response.tool_calls.is_empty() {
        println!("=== tool_calls ===");
        // Pretty-printing a list of tool_calls is the only structured
        // output the binary emits; serde_json::to_string_pretty cannot
        // fail on owned values built from valid JSON, but unwrap-or-fallback
        // anyway so a serialization bug never panics the binary.
        let rendered = serde_json::to_string_pretty(&response.tool_calls)
            .unwrap_or_else(|_| format!("{:?}", response.tool_calls));
        println!("{rendered}");
    }
}

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
fn print_diagnostics(model: &str, elapsed: Duration, usage: Usage) {
    eprintln!(
        "model={model} latency_ms={lat} input_tokens={i} output_tokens={o}",
        lat = elapsed.as_millis(),
        i = usage.input_tokens,
        o = usage.output_tokens,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn parse_args_required_anthropic_with_prompt() {
        let p = parse_args(&s(&["--vendor", "anthropic", "--prompt", "hi"])).unwrap();
        assert_eq!(p.vendor, Vendor::Anthropic);
        assert_eq!(p.prompt_source, PromptSource::Literal("hi".into()));
        assert_eq!(p.max_tokens, DEFAULT_MAX_TOKENS);
        assert!(p.model.is_none());
        assert!(p.system.is_none());
    }

    #[test]
    fn parse_args_max_tokens_override_changes_field() {
        let p = parse_args(&s(&[
            "--vendor",
            "anthropic",
            "--max-tokens",
            "256",
            "--prompt",
            "hi",
        ]))
        .unwrap();
        assert_eq!(p.max_tokens, 256);
        let req = build_request(&p, "hi".into()).unwrap();
        assert_eq!(req.options.max_tokens, 256);
    }

    #[test]
    fn parse_args_system_override_populates_field() {
        let p = parse_args(&s(&[
            "--vendor",
            "anthropic",
            "--system",
            "be terse",
            "--prompt",
            "hi",
        ]))
        .unwrap();
        assert_eq!(p.system.as_deref(), Some("be terse"));
        let req = build_request(&p, "hi".into()).unwrap();
        // System message should be the first message in the request.
        assert_eq!(req.messages.len(), 2);
        assert_eq!(
            req.messages[0],
            Message::system("be terse"),
            "first message should be the system prompt"
        );
        assert_eq!(req.messages[1], Message::user("hi"));
    }

    #[test]
    fn parse_args_model_override_passes_through() {
        let p = parse_args(&s(&[
            "--vendor",
            "anthropic",
            "--model",
            "claude-sonnet-4-5",
            "--prompt",
            "hi",
        ]))
        .unwrap();
        assert_eq!(p.model.as_deref(), Some("claude-sonnet-4-5"));
    }

    #[test]
    fn parse_args_from_stdin_marks_source() {
        let p = parse_args(&s(&["--vendor", "anthropic", "--from-stdin"])).unwrap();
        assert_eq!(p.prompt_source, PromptSource::Stdin);
    }

    #[test]
    fn parse_args_rejects_both_prompt_and_stdin() {
        let err = parse_args(&s(&[
            "--vendor",
            "anthropic",
            "--prompt",
            "hi",
            "--from-stdin",
        ]))
        .unwrap_err();
        assert!(format!("{err:#}").contains("mutually exclusive"));
    }

    #[test]
    fn parse_args_rejects_neither_prompt_nor_stdin() {
        let err = parse_args(&s(&["--vendor", "anthropic"])).unwrap_err();
        assert!(format!("{err:#}").contains("--prompt"));
    }

    #[test]
    fn parse_args_rejects_missing_vendor() {
        let err = parse_args(&s(&["--prompt", "hi"])).unwrap_err();
        assert!(format!("{err:#}").contains("--vendor is required"));
    }

    #[test]
    fn parse_args_rejects_unknown_vendor() {
        let err = parse_args(&s(&["--vendor", "openai", "--prompt", "hi"])).unwrap_err();
        assert!(format!("{err:#}").contains("openai"));
    }

    #[test]
    fn parse_args_rejects_unknown_flag() {
        let err =
            parse_args(&s(&["--vendor", "anthropic", "--prompt", "hi", "--tools"])).unwrap_err();
        assert!(format!("{err:#}").contains("--tools"));
    }

    #[test]
    fn parse_args_rejects_zero_max_tokens() {
        let err = parse_args(&s(&[
            "--vendor",
            "anthropic",
            "--max-tokens",
            "0",
            "--prompt",
            "hi",
        ]))
        .unwrap_err();
        assert!(format!("{err:#}").contains("--max-tokens"));
    }

    #[test]
    fn parse_args_accepts_cohere_vendor() {
        let p = parse_args(&s(&["--vendor", "cohere", "--prompt", "hi"])).unwrap();
        assert_eq!(p.vendor, Vendor::Cohere);
    }

    #[test]
    fn build_request_is_vendor_agnostic() {
        // Same parsed args differing only in vendor produce identical
        // CompleteRequests — adapter-specific shaping happens in the
        // adapter, not here.
        let p_a = ParsedArgs {
            vendor: Vendor::Anthropic,
            model: None,
            system: Some("be terse".into()),
            max_tokens: 64,
            prompt_source: PromptSource::Literal("hi".into()),
        };
        let p_c = ParsedArgs {
            vendor: Vendor::Cohere,
            ..p_a.clone()
        };
        let r_a = build_request(&p_a, "hi".into()).unwrap();
        let r_c = build_request(&p_c, "hi".into()).unwrap();
        assert_eq!(r_a, r_c);
        assert_eq!(r_a.messages[0], Message::system("be terse"));
        assert_eq!(r_a.messages[1], Message::user("hi"));
        assert_eq!(r_a.options.max_tokens, 64);
    }

    #[test]
    fn build_request_anthropic_no_system_has_one_user_message() {
        let p = ParsedArgs {
            vendor: Vendor::Anthropic,
            model: None,
            system: None,
            max_tokens: 1024,
            prompt_source: PromptSource::Literal("hi".into()),
        };
        let req = build_request(&p, "hi".into()).unwrap();
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.messages[0], Message::user("hi"));
        assert!(req.tools.is_empty());
        assert_eq!(req.options.max_tokens, 1024);
        assert!(req.options.temperature.is_none());
    }

    #[test]
    fn build_request_cohere_succeeds() {
        // Cohere goes through the same vendor-agnostic request builder
        // as anthropic.
        let p = ParsedArgs {
            vendor: Vendor::Cohere,
            model: None,
            system: None,
            max_tokens: 1024,
            prompt_source: PromptSource::Literal("hi".into()),
        };
        let req = build_request(&p, "hi".into()).unwrap();
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.messages[0], Message::user("hi"));
    }

    #[test]
    fn read_prompt_literal_returns_value() {
        let p = ParsedArgs {
            vendor: Vendor::Anthropic,
            model: None,
            system: None,
            max_tokens: 1024,
            prompt_source: PromptSource::Literal("hello".into()),
        };
        let mut empty: &[u8] = &[];
        let got = read_prompt(&p, &mut empty).unwrap();
        assert_eq!(got, "hello");
    }

    #[test]
    fn read_prompt_stdin_reads_to_eof() {
        let p = ParsedArgs {
            vendor: Vendor::Anthropic,
            model: None,
            system: None,
            max_tokens: 1024,
            prompt_source: PromptSource::Stdin,
        };
        let mut bytes: &[u8] = b"piped prompt body";
        let got = read_prompt(&p, &mut bytes).unwrap();
        assert_eq!(got, "piped prompt body");
    }

    #[test]
    fn read_prompt_stdin_rejects_empty_input() {
        let p = ParsedArgs {
            vendor: Vendor::Anthropic,
            model: None,
            system: None,
            max_tokens: 1024,
            prompt_source: PromptSource::Stdin,
        };
        let mut empty: &[u8] = &[];
        let err = read_prompt(&p, &mut empty).unwrap_err();
        assert!(format!("{err:#}").contains("empty"));
    }

    /// Compile-time check that the request a `ModelClient` can consume is
    /// what `build_request` actually produces.
    #[allow(dead_code)]
    fn _build_request_yields_modelclient_input(
        client: &dyn coral_node::model_client::ModelClient,
        req: CompleteRequest,
    ) {
        let _fut = client.complete(req);
    }

    #[test]
    #[cfg(not(feature = "llm-anthropic"))]
    fn run_anthropic_without_feature_errors_with_helpful_hint() {
        let p = ParsedArgs {
            vendor: Vendor::Anthropic,
            model: None,
            system: None,
            max_tokens: 1024,
            prompt_source: PromptSource::Literal("hi".into()),
        };
        let req = build_request(&p, "hi".into()).unwrap();
        let err = run_anthropic(&p, req).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("llm-anthropic"), "msg: {msg}");
    }

    #[test]
    #[cfg(not(feature = "llm-cohere"))]
    fn run_cohere_without_feature_errors_with_helpful_hint() {
        let p = ParsedArgs {
            vendor: Vendor::Cohere,
            model: None,
            system: None,
            max_tokens: 1024,
            prompt_source: PromptSource::Literal("hi".into()),
        };
        let req = build_request(&p, "hi".into()).unwrap();
        let err = run_cohere(&p, req).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("llm-cohere"), "msg: {msg}");
    }
}
