# Flatten call_tool → first-class typed Cohere tools + strict_tools

## Why (diagnosis, all evidence-backed via cohere-bench)

north-mini-code cannot reliably emit the generic `call_tool` decision:
- Live: parent + Rust child wandered (never attempted call_tool); Go child
  died on a double-422 (`INVALID_TOOL_GENERATION`).
- Bench: forcing call_tool → nests `claim_seed` inside `args`, and 422s.
- **The wall:** `strict_tools` (Cohere grammar-constrained decoding) is what a
  weak model needs, and it is *structurally incompatible* with `call_tool`'s
  free-form `args`. A first-class typed tool + strict_tools = **10/10 clean**;
  first-class typed tool *without* strict_tools = **9/10 hallucinated**. So
  strict_tools is mandatory.

## The three strict_tools rules (empirical, request-global)

Every offered function must satisfy all of:
1. every property has a `type`;
2. every `object` declares `properties`;
3. every `object` has ≥1 `required` field.
No free-form objects. `{properties:{}, additionalProperties:true}` is rejected
(rule 3). strict_tools is a request-level boolean → **all** offered tools must
comply, or the whole request 400s.

## Design (Cohere-adapter-scoped; kernel Decision model unchanged)

Claude adapter untouched (it handles `call_tool` fine). Cohere adapter only:

- **Request build:** replace the generic `call_tool` function with the agent's
  *granted runtime tools* as first-class typed Cohere functions (their own
  schemas), keep the other decision tools (all made strict-compatible), set
  `strict_tools: true`.
- **Response parse:** collapse a first-class runtime-tool call
  `web_search({query})` back into the kernel's `call_tool`-shaped
  `model_client::ToolCall` — `{name:"call_tool", arguments:{name, args, claim_seed}}`
  — before `parse_decision` runs. `parse_decision` and `Decision::CallTools`
  are untouched.

## Making the whole surface strict-compatible: schemars-derived schemas

`decision_tools()` has loose `{type:object}` fields mapping to kernel types.
Maintainer decision: **derive schemas with `schemars`** (already a workspace
dep in coral_graph, =1.2.1) so schema == type, no hand-roll drift.

| tool | loose field | kernel type | user |
|---|---|---|---|
| rewrite_fs | ops | `FsOp` (enum union) | leaf + parent |
| spawn_child | mandate | `Mandate` | parent |
| reconcile_children | sources, conflict | `{child_ref,output_id}`, `ConflictRecordIntent` | parent |
| retire_child / replace_child | child_ref, new_mandate | `AgentRef`, `Mandate` | parent |

Already strict-compatible: write_output, read, list, search, idle.

**RISK #1 — SPIKED & RESOLVED (2026-07-05):** schemars emits `oneOf` for every
enum (FsOp is internally-tagged). **Cohere strict_tools REJECTS oneOf**:
`"composition constraints not supported: oneOf"`. So schemars-derive works for
**plain structs only**; **union/enum types must be hand-flattened**. Verified
the flatten works: FsOp as a single struct `{op: {type:string, enum:[write_file,
delete_file]}, path, content}` with ALL fields required → **3/3 clean generation
under strict_tools**, and that shape IS FsOp's serde form (`#[serde(tag="op")]`),
so the model's output deserializes straight into `FsOp` (delete_file ignores the
required-but-empty `content`). strict SUPPORTS string `enum`; does NOT support
`oneOf`/`anyOf`/`allOf`. **Refined schema policy:** schemars-derive for plain
structs (Mandate?/AgentRef? if struct); hand-authored flat strict schema for
each union (FsOp; audit ConflictRecordIntent/Trigger/any enum field). A small
`strict_normalize(Value)` helper may still be needed to strip schemars'
`$schema`/`title`/`$ref` and enforce additionalProperties — audit per type.

## Plumbing (verified: not adapter-local)

- `CompleteRequest.tools` = decision tools only; `LlmDecide` has NO ToolRegistry;
  `mandate.tools: Vec<String>` = names; `Tool` trait exposes only `name()`/`call()`.
- Need: `Tool::input_schema() -> Value` (MCP tools already retain server schema;
  builtins like echo return a typed schema). Thread granted runtime tools'
  ToolSpecs (name + schema) from the registry (worker `TOOL_REGISTRY_PROVIDER`,
  reachable in `decide_step` via graph_id) down to the Cohere adapter via a new
  `CompleteRequest.runtime_tools: Vec<ToolSpec>` (serde default empty →
  Anthropic/existing path unchanged).

## claim_seed seam (maintainer's call delegated to me → runtime-mint)

A first-class `web_search({query})` carries no claim_seed. The collapse seam
**mints** one (opaque, e.g. derived from the vendor tool_use id). Model cites
evidence by PATH (post #177), so the seed value is non-load-bearing; evidence
filename becomes less human-readable — accepted tradeoff.

## Build order

1. **Spike:** schemars on FsOp → strict-compat check vs Cohere. Decide normalizer.
2. schemars-derive closed schemas for FsOp/Mandate/AgentRef/ConflictRecordIntent;
   embed in `decision_tools()`; keep parse_decision green.
3. `CompleteRequest.runtime_tools` + Cohere adapter expand/collapse + strict_tools
   (only when runtime_tools present). Hermetic adapter tests (expand shape,
   collapse→call_tool shape, minted claim_seed, strict rules).
4. `Tool::input_schema()` + plumb granted schemas registry→decide_step→request.
5. Live re-run of the demo graph; verify a child completes orient→act→write→idle.

## NOT solved by this (out of scope, flagged)

- **Wandering** (parent + Rust child never attempted call_tool) — prompt/behavioral.
- **Loop completion** unproven until the live re-run.
