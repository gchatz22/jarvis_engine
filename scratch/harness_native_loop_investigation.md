# Why north-mini-code gets lost in our harness (and how to fix it)

Maintainer's frame: the *same* model (Cohere `north-mini-code-1-0`) stays in the
loop reliably in OpenCode, so the model is capable — **our harness is the
problem.** This is the investigation into what's wrong and which fix earns its
cost. Iterated with real Cohere runs, measured, cheap→expensive.

## The defect (from live run 15)

At the point where the parent has folded ONE of its two children, it never
produces — it loops `read`/`list`/`search`/re-`reconcile`, hunting its own
filesystem for the sibling brief, never writes a two-sided Output, never idles,
runs until killed (40 steps, 29× `List evidence`, read its own
`decisions/1-16.jsonl`). Children converge fine (~20 steps, idle clean).

## Root cause: the harness is not a native agentic loop

`decide_llm::render` rebuilds a **stateless** prompt every step:
`[system, user(wake), user(index), user("# Steps so far this cycle")]`. The
model's own prior actions are flattened into a **third-person user-narrative**
(`"1. list evidence -> <result>"`). There are **no native `assistant(ToolUse)`
turns and no `tool(ToolResult)` turns**, and the model's reasoning between steps
is discarded. OpenCode (and every standard agent harness) keeps the native loop:
the conversation grows as `assistant(tool_call)` / `tool(result)` in the model's
own voice. north-mini-code is a *code* model trained on that native loop; we
took it away (for Temporal replay determinism) and it flails.

Smoking gun: the parent **read its own `decisions/` log** — it went hunting the
filesystem for what it had already done, because the harness stripped its native
memory of its own actions.

The `Message` type **already supports** native turns (`Role::Tool`,
`ContentBlock::ToolUse{id,name,input}`, `ContentBlock::ToolResult{tool_use_id,
content}`), and **both vendor adapters already map them to the wire**
(`cohere.rs::build_body` has explicit `Role::Assistant`+ToolUse →`tool_calls[]`
and `Role::Tool`+ToolResult →native `tool` message arms). Native threading is
**used by nothing** — `render()` just doesn't emit it. So the fix is localized
to `render()` + a `Decision`→`ToolUse` mapper; **zero adapter work.**

## Measurement rig

Extended `cohere-bench` with `--scenario reconciled-parent`: a synthetic
`Session` where the parent has folded `go-advocate` (ChildOutput wake +
`ReconcileChildren` step + observation naming a citable evidence path) and
`rust-advocate` has not reported. Bench measures the **next decision over N
trials** and reports the converged/wander split. It doesn't execute decisions,
so it sidesteps the in-process `ReconcileChildren` `unimplemented!()` blocker
(the reason `node-run-llm` — a child-only rig — can't reproduce the parent bug).

Metric at this fork: `write_output` = converged (honest single-sided Output);
`idle` = acceptable; `read`/`list`/`search` = wander; `reconcile_children` =
re-doing a completed action (churn).

## Results (N=20 each, north-mini-code-1-0)

| Variant | converged (write_output+idle) | wander (read+list+search) | re-reconcile | idle |
|---|---|---|---|---|
| **Baseline** | **0/20** | 19/20 (read 16, list 3) | 1 | 0 |
| **V1** (index doesn't invite exploration) | 2/20 | 16/20 | 2 | 0 |
| **V3** (V1 + ChildOutput wake-briefing: siblings are future signals, one-sided Output is correct) | 5/20 | 8/20 | 7 | 0 |
| **Native threading + V1+V2** (run A) | 8/18 | 10 | **0** | 0 |
| **Native threading + V1+V2** (run B, verbose) | **11/17** | 6 | **0** | 0 |

Native message threading (`render()` emits `assistant(ToolUse)`/`tool(ToolResult)`
turns instead of a user-narrative) **~doubles the write rate to ~45–55%** and
**eliminates the re-`reconcile` churn (7→0)** — the model now sees its step-1
reconcile as its own completed action and doesn't redo it. The write_outputs are
**correct**: each reconciles go-advocate's brief, gives a provisional Go
recommendation with the condition under which Rust wins, and explicitly notes it
awaits rust-advocate's brief and will refresh — the exact interim-Output
behavior the design wants. The residual `read`s are all of the *reconcile
evidence file* (one-time content verification that, under native memory, won't
loop the way the live parent's 29× `List` did — the single-decision bench
under-measures this). New minor mode: ~3/20 parse-fails where the model replies
in prose instead of a tool call; the real decide path retries on parse failure.

Cheap prompt fixes alone **help but plateau at ~25%**. Two structural tells survive
every variant:
- **`idle` chosen 0/20 in ALL variants.** The cycle-ending primitive is
  essentially never selected — `idle`-as-a-tool is foreign; a native loop ends a
  turn by *stopping*, not by calling a stop-tool.
- **Re-`reconcile` rose to 7/20 under V3** — told to "reconcile it," the model
  re-folds the child it already folded, because it can't see its step-1 action
  as its own completed work.

Both are the same root: no native memory of its own actions. Prompt wording can't
fix a structural gap. (V1+V2 prompt edits are in `prompt.rs` now, unfinalized —
they're strict improvements but break the render snapshot tests, to be updated
only if kept.)

## The fix to build + measure next

Rewrite `render()` to emit a **native tool-calling conversation** reconstructed
from `Session.steps`: `system` → `user(wake+index)` → for each step
`assistant(ToolUse{id: step-i, name, input})` + `tool(ToolResult{tool_use_id:
step-i, content: observation})`. Scope (per advisor):
- **Reformat-only.** Reconstruct from existing `Step` data; don't persist model
  reasoning (that perturbs continue-as-new). Stays replay-deterministic
  (synthesized ids from step index).
- **One step per turn** unchanged — don't confound turn-multiplicity in the same
  experiment.
- Needs a `Decision`→`ContentBlock::ToolUse` mapper (inverse of
  `parse_decision`); check `schema.rs` for an existing serializer first.
- Rewrite the `render` snapshot tests to the native shape.

Measure the SAME `reconciled-parent` scenario, N=20. Success = converged jumps
well past the 5/20 prompt-only plateau AND `idle`/re-`reconcile` churn drops
(the native-memory tells resolve). Then validate end-to-end with a full Temporal
run of the rust-vs-go graph (parent folds both, writes two-sided synthesis,
idles, quiescent).

Fallback if native threading underdelivers: the last-resort deterministic
circuit-breaker (force write-or-idle after N non-productive steps) — explicitly
deprioritized by the maintainer.

## End-to-end validation (run 16, native threading) — CONVERGED ✅

Full `rust-vs-go-minimal-16` graph, worker built with native threading. The graph
**converged and went quiescent** — the exact thing run 15 never did:

- **Children tightened ~4×.** rust-advocate idled at **4 steps**, go-advocate at
  **5 steps** (run 15: ~20 each), each with a correctly-sided brief. The
  re-inspection that padded run 15's leaves is gone — native memory means the
  model doesn't re-read what it already read.
- **Parent folded BOTH children and idled.** Tick 1: folded rust-advocate →
  wrote an interim single-sided Output → idled. Tick 2: woke on go-advocate's
  ChildOutput → folded it (recs=2) → wrote a **full two-sided synthesis** (Rust
  case, Go case, "Go is the default", "choose Rust instead when …") → **idled at
  step 12**. Run 15: wandered to step 40, folded one child, never two-sided,
  killed.
- **Stable + quiescent.** No step growth across polls; the `never` cadence held
  (model's `idle` next_after ignored, no self-wake, no re-wander). The merged
  quiescence-GC reaper would retire these once quiescent past its wave margin —
  the two features compose.

Verdict: native message threading is the fix. The structural change (loop is
natively agentic) resolved the parent non-termination AND made the children
efficient — a single localized `render()` rewrite, zero adapter work.

## Left to finalize (not yet done)

- Rewrite the `render` snapshot tests to the native shape (~4 tests assert the
  old 4-message narrative / "# Steps so far"; delete the `action_label` test —
  that helper is gone). Then run the full gates (`cargo test`, `clippy -D
  warnings`, `fmt --check`).
- Decide whether to keep the V1+V2 prompt edits (`render_index` wording +
  `ChildOutput` wake-briefing). They're strict improvements and were in the
  validated run, but each breaks a render snapshot test too. Recommend: keep
  (they're aligned with the system prompt and cost nothing), fold their test
  updates into the same pass.
- The prose-instead-of-tool-call parse-fail (~3/20 in the bench) is handled by
  the live decide-retry loop; watch it but it isn't a blocker.
- Then file the issue + PR per DEVELOPMENT.md (this is a real feature change to
  core rendering, not just an experiment).
