# Continuous monitor — execution plan

*Short-term goal: run the NVIDIA supply-chain graph as a **continuous monitor** (agents that wake on
a cadence, refresh narrow research, and emit updated outputs over time) on the durable Temporal
path — not the current hardcoded "emit one Output → retire" lifecycle.*

Tracked on `gchatz22/coral_engine` under parent **[#114]** with 7 native sub-issues.

## Framing: no second lifecycle — one persistence bit

The runtime loop is **already continuous** (`wait_for_tick → drain → assemble → decide → dispatch →
loop`, running until `Decision::Retire` or a retire signal). What makes today's agents one-shot is a
single hardcoded paragraph in the prompt (`decide_llm/prompt.rs:50-51`: "after emitting any Output,
retire; never re-emit"). Cadence (`idle_period`) and budget (`max_ticks`) already exist.

So this is **not** a new "mode" or graph type. The only thing not derivable from existing knobs is
one bit of intent: *may an agent terminate itself, or must it persist?* We add exactly that — a
per-agent **`persistent`** flag (default `false` = today's behavior). Self-termination authority is
genuinely binary; the spectrum (how often, how many cycles) stays in `idle_period` / `max_ticks`.

Why a structured bit, not mandate prose: the **stop contract**. A monitor's value is longevity; if
`Retire` stays a free model decision, one bad inference ends it permanently. Only the runtime can
enforce "this agent may not self-terminate" — and to do that it must *know* the agent is persistent.
VISION assigns lifecycle/scheduling to the kernel, so termination policy belongs there.

## Architecture: in-agent idle-loop (not external re-seeding)

Agents idle-and-refresh in-process rather than being re-seeded externally — VISION wants
continuous-not-episodic, the re-wake machinery already exists (`Decision::Idle` → `next_wake` →
`ctx.timer`), and re-emit is prompt-only (`emit_output` has no `last_output_id` guard). External
re-seeding would fight the architecture and lose per-agent durable-memory continuity.

## Verified current-state facts shaping the work

- `max_ticks` is **silently ignored on the Temporal path** (enforced only in-process,
  `coral_node/src/agent.rs:192`; zero `.max_ticks` reads in `coral_temporal`). A bug, independent of
  persistence — no kernel circuit breaker today.
- An **idle-timer wake yields an empty trigger list** (`ScheduledWake` is synthesized only in-process,
  never in the workflow loop). A bug exposed by persistence — anything that idles wakes blind.
- Reconcile already reads child content into the parent's evidence store (`activities.rs:824`);
  repeated reconciliation is mechanically supported.

## Stop contract (load-bearing)

A persistent agent's only legitimate stops: (a) a retire **signal** (human/parent override,
`workflow.rs:460`), or (b) **guardrail exhaustion** (`max_ticks`/budget). A model-emitted `Retire` is
demoted to `Idle` when `persistent`. Spans CM-2 + CM-3.

## Sub-issues (dependency-ordered)

| # | Issue | Scope | Deps |
|---|-------|-------|------|
| [#115] | **CM-1** `persistent` flag | One per-agent bool (default `false`): mandate + YAML + DB column/migration + persist + schema regen. No behavior change. | — |
| [#116] | **CM-2** Stop contract | Enforce `max_ticks` in the workflow loop (latent bug) **+** demote model `Retire`→`Idle` for persistent agents. | CM-1 |
| [#117] | **CM-3** Wake + refresh | Synthesize `ScheduledWake` on idle wake **+** refresh prompt invariants for persistent agents. | CM-1 |
| [#118] | **CM-4** Persistent-parent re-reconcile | Fold newer child outputs into refreshed reports; already-seen handling; reject degenerate persistent-parent/non-persistent-children at apply. | CM-1, CM-3 |
| [#119] | **CM-5** Per-agent model | Stronger model for the reconciling parent. Likely required. | CM-1 |
| [#120] | **CM-6** E2E + live smoke | Reduced persistent graph, N cycles; asserts ≥2 distinct outputs + a re-reconciliation + guardrail stop. | CM-1–5 |
| [#121] | **CM-7** *(optional)* | Per-graph cost/cycle budget beyond `max_ticks`. | CM-2 |

**Execution order:** CM-1 → {CM-2, CM-3, CM-5 in parallel} → CM-4 → CM-6; CM-7 deferrable.

## Load-bearing notes

- The net new surface is **one boolean + two loop-bug fixes + prompt work** — not a mode system.
- **CM-2 + CM-3 are one contract** ("what stops a monitor" + "what wakes it"). Candidate to merge.
- **[#107] (tool-naming / surface tool catalog) is a parallel prerequisite** for a *useful* run —
  not part of this work and not blocked by it, but children can't call real web-search MCP tools
  without it.
- **Loop viability is the only real unknown.** CM-1/CM-2 build cleanly and their hermetic
  (MockDecide) tests pass without proving a real model can drive the parent loop. That answer first
  arrives at CM-6 (live); if the parent needs a stronger model or more prompt scaffolding, it
  surfaces in CM-5's status and CM-3's prompt wording. (A live spike was offered and declined in
  favor of filing now.)

## Status

Issue set filed and nested under #114; **awaiting maintainer review before implementation** (the
DEVELOPMENT.md "stop before coding" gate). CM-1 is the unblocker for everything else.

[#114]: https://github.com/gchatz22/coral_engine/issues/114
[#115]: https://github.com/gchatz22/coral_engine/issues/115
[#116]: https://github.com/gchatz22/coral_engine/issues/116
[#117]: https://github.com/gchatz22/coral_engine/issues/117
[#118]: https://github.com/gchatz22/coral_engine/issues/118
[#119]: https://github.com/gchatz22/coral_engine/issues/119
[#120]: https://github.com/gchatz22/coral_engine/issues/120
[#121]: https://github.com/gchatz22/coral_engine/issues/121
[#107]: https://github.com/gchatz22/coral_engine/issues/107
