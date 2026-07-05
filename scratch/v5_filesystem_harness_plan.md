# V5 — Filesystem harness: pull-navigation + cycle loop (combined #145 + #146)

Maintainer chose **combine #145 + #146** into one PR (2026-06-24). V5.3 #147 (write_output
`{body, citations→DB}` + outputs/evidence reslug) stays a separate follow-up PR.

This is the realignment's single biggest build: it replaces the single-decision-per-tick loop
with a multi-step inner ReAct loop, drops `ContextPolicy`/the fat `assemble_context`, adds
FS-navigation tools, and reshapes the `Decision` enum.

## What the design fixes as decided (not open)

- **tick = one full execution of the unit of mandate work** (Concern 8, decided 2026-06-13).
- **Two nested loops.** Outer = wake boundary (`wait_for_tick`). Inner = the unit of work:
  thin seed (once) → loop { decide → act → observe → … } → terminal (idle) → outer wait.
- **Each inner step is a journaled Temporal activity; the pure workflow body orchestrates the
  loop deterministically.** (NOT one long-running activity running the whole ReAct session.)
- **In-cycle context is an ephemeral conversation discarded at cycle end; cross-cycle continuity
  is the FS.** `notes/` is the primary continuity mechanism.
- **No model self-Retire** (already true post-V3.1). Termination = signal / step_cap.
- **`staged_correction` cross-tick machinery dissolves** — tool failures become inner-loop
  observations the model adapts to within the same cycle.
- **Read scope = own FS ∪ entire descendant subtree (read-only), A6.**
- **Code-execution deferred** (move 6 / D1). Near-term Act = direct tool calls, results in-context.

## Current architecture (mapped)

- `Decide::decide(ctx: ContextBundle) -> Decision` — one decision per call.
- Temporal outer loop (`workflow.rs:449`): wake → drain buckets → `assemble_context` (activity) →
  `decide_next_action` (activity) → `log_decision` → dispatch match arm (→ activity / SDK cmd) →
  `tick += 1` → CAN check.
- In-process loop (`agent.rs:170 run()`): wake → drain → `assemble_context` → `decide` → `dispatch`
  → handle `DispatchOutcome {Continue, NeedsCorrection, ToolError}` → loop; step_cap retire at top.
- `Decision` enum (`decision.rs:68`): `CallTools{calls}`, `EmitOutput{content,evidence}`,
  `RewriteFs{ops}`, `Idle{next_after}`, `SpawnChild`, `ReconcileChildren`, `RetireChild`,
  `ReplaceChild`.
- `ContextBundle` (`decision.rs:337`): mandate, triggers, recent_outputs, recent_evidence,
  open_claims, correction.
- `ContextPolicy` (`mandate.rs:169`): recent_outputs / recent_evidence / open_claims windows.
- LLM: `decision_tools()` advertises 8 tools; `parse_decision` folds parallel `call_tool` →
  `CallTools`, single terminal → re-tag+serde. Prompt = system(mandate+catalog+invariants) +
  optional [correction, triggers, outputs, evidence, open_claims]. 7 INVARIANTS.
- `file_index` has schema + `recent_files` (orders by `updated_at DESC`) but **NO production
  writer** (only tests). `_tail.json` is today's recency source.

## Proposed new architecture

### The cycle (both paths)

```
outer: loop {
  wait_for_tick(never)                      // unchanged: signal | idle timer
  step_cap retire check at top              // unchanged (bounds the whole run)
  drain buckets → triggers                  // unchanged
  let seed = build_seed(fs, mandate, triggers, db_index)   // THIN: replaces assemble_context
  let mut session = Session::new(seed)
  inner: loop {
    let action = decide_step(&session)      // activity (Temporal) / Decide::decide (in-proc)
    match action {
      Terminal(Idle{next_after}) => { set next_wake; break inner }
      Repertoire(a) => {
        let observation = act(a)            // activity: read/list/search/call_tool/write/reconcile/spawn/retire
        session.push(action, observation)   // grows the ephemeral conversation
        // failure => observation carries the error; model adapts next step (no staged_correction)
      }
    }
    if session.total_steps >= CYCLE_RUNAWAY_FUSE { force_idle; break inner }  // ~50k fuse across rollovers; NOT a per-cycle cap
    // (Temporal only) between steps: if continue_as_new_suggested() { pack session → carryover; CAN; resume SAME cycle on rehydrate }
  }
  tick += 1                                  // tick counts CYCLES; increments only at a TRUE idle end, not mid-cycle rollover
  CAN check at cycle boundary                // also CAN between cycles, unchanged location
}
```

> **Reconciled (A+C, authoritative):** there is **no per-cycle `INNER_STEP_CAP`**. The only
> in-cycle bound is `CYCLE_RUNAWAY_FUSE` (~50k steps counted across rollovers). Earlier drafts
> of this doc said `INNER_STEP_CAP`; A+C removed it — build from the fuse, not the cap.

### `Decision` → `Action` (the enum collapse) — FORK, see below

Two-level split mirroring "research repertoire + thin terminal/topology surface":

- **Repertoire (continue the inner loop, observation appended):**
  - `Read{path}`, `List{path}`, `Search{query, path?}` — NEW FS-nav (own FS ∪ subtree, read-only)
  - `CallTool{name, args, claim_seed}` — keep (MCP; results in-context now, not next tick)
  - `WriteNote{...}` — today's `RewriteFs{ops}` (already notes-scoped per V1.3)
  - `WriteOutput{content, evidence}` — today's `EmitOutput`; the `{body, citations→DB}` rewrite is #147
  - `Reconcile{sources, conflict}` — today's `ReconcileChildren`
  - `SpawnChild{name, mandate}`, `RetireChild{ref, reason}` — topology, now mid-cycle actions
- **Terminal (break inner loop → outer wait):**
  - `Idle{next_after}` — the SOLE terminal (= set_cadence/idle). After emitting outputs, idle.

`ReplaceChild` = retire + spawn; drop as a primitive (kernel-level it already is two ops) OR keep.

### Seed (replaces ContextBundle)

`Seed { mandate, triggers, index, correction? }` where `index` = pointers (filenames) to
`notes/` + `outputs/`, NOT contents. Everything else is pulled via Read/List/Search.
- Recency for the index: **keep `_tail.json`** (writer for `file_index` lands in #147/#150;
  listing the dir loses time-order since slugs sort alphabetically). Honest deferral, same vein
  as #138/#139.

### Decide trait

`async fn decide(&self, session: &Session) -> Result<Action>` (was `decide(ctx: ContextBundle)`).
- `MockDecide` keeps popping a script FIFO — a script is now a sequence of *inner steps* ending
  at a terminal. Most existing scripts port with assertion changes (ticks→cycles/steps), not
  rewrites.
- `LlmDecide`: render(seed + session.observations) → model → parse one `Action`. The existing
  parse-retry conversation accumulation generalizes to the ReAct observation accumulation.

### Subtree read (A6) mechanism

`read/list/search` are activities (Temporal) that take a target agent path. Own-FS = direct.
Subtree = validate the target is within the descendant subtree using `child_handles`
(workflow state) — read-only. `fs.rs` (coral_node) gets the path-scoped read primitives;
the subtree *authorization* (topology) lives in the activity/workflow where child_handles is.

### FS-nav tools are an always-available kernel surface

NOT routed through `execute_tool`'s deny-by-default (`is_call_allowed`) — that path is for
MCP/builtin tools gated by `Mandate.tools`. Read/list/search are kernel FS ops, always available;
otherwise an agent with `tools: []` could not navigate and pull-nav dies on arrival.

## Scope boundaries

- **In:** thin seed; FS-nav (own + subtree, read-only); inner ReAct loop both paths; enum
  reshape; prompt/schema reshape; `staged_correction` dissolution; CAN at cycle boundary;
  `CYCLE_RUNAWAY_FUSE` (~50k, across rollovers); mid-cycle CAN session-carry (A+C, Temporal-only,
  built LAST so it is the natural stack point); subtree-read tests (Temporal); tool-failure-
  adapted-within-cycle test.
- **Out (→ #147):** `write_output {body, citations→DB}` + the rejection check; outputs/evidence
  reslug; `file_index` writer; `_tail.json` removal + DB recent_files swap.
- **Out (→ budget #12):** hard context-size bound; per-cycle $/token budget (`CYCLE_RUNAWAY_FUSE`
  is the interim backstop).
- **Out (→ D1):** run_code / sandbox.

## Resolved (advisor pass 2026-06-24) — not maintainer forks

- **Enum shape = FLAT.** Keep a flat `Action` enum (Read/List/Search added; Idle sole terminal);
  the repertoire-vs-terminal split lives in the loop logic, not the type. Nested
  `Research|Topology|Terminal` is internal structure with a conventional smallest-diff default —
  picked, not asked.
- **`tick` counts cycles** — this is the decided AC ("tick = one unit of mandate work"), not a fork.
- **KEEP `Idle{next_after}`** — grep shows ~30 test sites depend on it incl. assertions and
  `u64::MAX` (sleep-forever). It is the terminal's per-wake cadence override; dropping it (the
  advisor's conditional lean) is both bigger-diff and breaks live tests. `mandate.idle_period`
  is the default when the model doesn't override.
- **One driver, two executors.** The inner-loop *driver* (session accumulation, terminal
  detection, `CYCLE_RUNAWAY_FUSE`) lives in `agent_core.rs`, parameterized over how an action
  executes (direct call in-process vs Temporal activity). Avoids hand-duplicating the multi-step
  loop across the two paths (we already carry an in-proc/Temporal divergence; don't double it).
- **Keep the type name `Decision`** (advisor, 2026-06-24). "Decision → Action" in this doc is
  shorthand for the *reshape* (FS-nav variants added, trait takes `Session`, `Idle` sole terminal),
  NOT a type-wide rename. Smallest-correct-diff is binding and the variant/trait/schema churn is
  already large; a rename touches every reference across 4 crates and drowns the semantic diff in
  review noise. The type stays `Decision`; `Session` is the new accumulating context type.

## Build phases (advisor reframe, 2026-06-24): coral_node is the leaf crate → two contained phases

`coral_temporal → coral_node` and `coral_graph → both`, so **nothing sits below `coral_node`**.
`cargo {build,clippy,test} -p coral_node` is fully isolatable while the upper crates are mid-break.

- **Phase A — drive `coral_node` GREEN as an isolated unit** (steps 1–5: `Decision` reshape +
  `Session` + `Decide` trait + `MockDecide` + inner-loop driver in `agent_core` + FS-nav primitives
  in `fs.rs` + in-process executor + `loop_smoke` migration + prompt/schema reshape). **Green =**
  default `cargo build/clippy/test -p coral_node` **AND `--features coral_node/llm-anthropic`**
  (decide_llm is gated — a plain build never compiles schema/prompt/llm_decide) **AND** schemars
  golden regen (`ContextPolicy` drops off `Mandate`) **AND** `cargo fmt --check`. Hard checkpoint.
- **Phase B — the Temporal path** (step 6: `decide_step`/act activities + subtree auth via
  `child_handles` + A+C CAN/carry/fuse built LAST) then step 9 (migrate Temporal/worker/graph tests).
- **PR-split decision happens AT the Phase-A checkpoint**, with real diff numbers — not pre-split,
  not bundled blindly. A+C (mid-cycle session-carry CAN) is built last so it is the clean stack cut
  if the diff balloons: PR1 "core loop (pull-nav + inner ReAct + enum reshape)", PR2 "mid-cycle CAN
  session-carry" (isolates the replay-risky part). Maintainer chose combine+A+C → flag-and-ask with
  numbers if splitting.

## Phase A COMPLETE — checkpoint (2026-06-24)

`coral_node` (the leaf crate) is fully reshaped and **green on all three feature sets**:
default, `--features llm-anthropic`, `--features llm-cohere` — build + clippy (`-D warnings`,
clean) + test + `cargo fmt --check`. (Isolated runs pass `--features uuid/v4` to stand in for the
documented workspace feature-unification quirk; the lib itself never needs it.)

- **Diff so far:** ~1980 insertions / 2386 deletions across 10 `coral_node` files. Net deletion —
  the pull/seed model is smaller than the fat-bundle windows it replaces.
- **What landed:** `Decision` reshape (+`Read`/`List`/`Search`, `idle_after()` terminal helper);
  `Seed`/`FsIndex`/`Session`/`Step`/`Observation`; `Decide::decide(&Session)`; FS-nav primitives
  (`read_file`/`list_dir`/`search`/`recent_output_filenames`/`clean_relpath`) read-only own-FS;
  `agent_core` `build_seed`/`execute_step`/`StepOutcome`/`StepFailure`/`CYCLE_RUNAWAY_FUSE`;
  the two-loop `Agent::run` with per-cycle health accounting + `record_cycle_failure`;
  `ContextBundle`/`CorrectionContext`/`ContextPolicy`/`assemble_context` removed;
  `loop_smoke` migrated (21 tests); prompt/schema reshaped + snapshots.
- **Decision: shared functions + inline loops, NOT a driver trait** (advisor-confirmed). The loop
  *substance* is shared (`execute_step`/`build_seed`/`Session`/fuse); the ~15-line skeleton appears
  in each host because the two paths genuinely diverge (direct calls vs activity orchestration +
  CAN). A forced async-trait over `WorkflowContext` would fight both. Do not revisit in Phase B.

### PR split (advisor-confirmed) — flag-and-ask the maintainer
`coral_node` alone is **not mergeable** (temporal/graph/worker now broken → workspace won't build).
So the real seam is:
- **PR1 = Phase A + Phase B-core** — temporal/graph/worker reshape, **CAN at cycle boundary only
  (today's behavior)**; workspace-green, all tests pass. A cycle that fits one Temporal run works
  fine without A+C, so PR1 is a coherent shippable intermediate.
- **PR2 = A+C** (mid-cycle CAN session-carry) stacked on PR1 — isolates the replay-risky part:
  the "mid-cycle" carryover marker, session pack/rehydrate, the ~256KB/2MB size guard. ALL of A+C
  lives in PR2.

### Review items to surface at the checkpoint (green tests cannot vet these)
1. **FIXED:** `build_seed` now caps the notes index (`SEED_INDEX_NOTES=32`, lexicographic tail) —
   was unbounded, which defeated the thin-seed pivot.
2. **Known limitation (follow-up, not PR1):** `search` with `path: None` LISTs + `get_many`s the
   whole FS (incl. content-addressed `evidence/`) into memory per call — a scaling cliff vs the
   millions-of-subagents target. Bound it (scope default, cap, or index) in a later pass.
3. **Maintainer-review + owed live run:** the reworked prompt invariants, and rendering session
   history as **text** rather than native `tool_use`/`tool_result` blocks. Snapshots only prove the
   bytes match what I wrote — not that a real model loops well with prose-summarized history (native
   blocks usually drive ReAct better). Vet on the owed live README run.
4. **Reasoned-only:** the `CYCLE_RUNAWAY_FUSE` force-idle path (50k) is untested (never bites in
   hermetic runs).

## Phase B COMPLETE — PR1 ready (2026-06-25)

Maintainer approved the split: **PR1 = Phase A + Phase B-core (cycle-boundary CAN only); PR2 = A+C
stacked.** PR1 is done and the **whole workspace is green**: `cargo build --workspace`,
`cargo test --workspace` (with local Postgres: **0 failures**; coral_node 252 lib + loop_smoke 21,
coral_temporal 81 lib + all integration, coral_graph 95, coral_worker — all pass), `cargo clippy
--workspace --all-targets` (default + all llm features, clean), `cargo fmt --check` clean. schemars
golden `graph_schema_json_matches_schemars_derive` **passes unchanged** (ContextPolicy was never in
the YAML schema, so no regen).

- **Temporal inner loop:** `workflow.rs::run` now builds a thin `Seed` (the `build_seed` activity),
  makes a LOCAL `Session`, and runs `decide_step → execute_action → push(observation)` until `Idle`
  (sole terminal) or `CYCLE_RUNAWAY_FUSE`. `execute_action` returns an `Observation` per repertoire
  step (CallTools/EmitOutput/RewriteFs/Read/List/Search/Spawn/Reconcile/Retire/Replace); failures
  are failure-observations the model adapts to (no `staged_correction` — DELETED from `Carryover`,
  `AgentWorkflow`, encode/hydrate, drain). `tick` = cycle counter; CAN stays at the cycle boundary.
- **Replay invariant held:** the session is rebuilt only from journaled activity results in the
  workflow body; never a live FS read.
- **Activities:** `assemble_context`→`build_seed` (returns `Seed`), `decide_next_action`→`decide_step`
  (takes `Session`), new `read_fs` (Read/List/Search via `agent_core::execute_step`, own-FS,
  byte-identical to in-process). Decision log keyed `decisions/<tick>-<step>.jsonl` (+`step` field).
- **Subtree read SCOPED OUT of PR1:** `read_fs` is **own-FS only** on both paths (matches in-process
  Phase A). A6 "own ∪ descendant subtree (read-only)" deferred to a follow-up (needs topology/
  child_handles auth + a path scheme). Flag to maintainer; the plan's Temporal subtree test moves to
  that follow-up.
- **Test migration:** the ~11 Temporal/graph/worker integration tests (all `TEMPORAL_LIVE_TEST`-
  gated → no-op hermetically) migrated by 3 parallel subagents to cycle/step/cap semantics; I
  reviewed the script recomputes. **Two test-logic changes worth the maintainer's eye on the live
  run:** `multi_agent.rs` and `persistent_monitor_live.rs` parents lost the `recent_evidence` window
  the deleted bundle provided, so the migration interposes a real `List { "evidence/" }` step after
  reconcile and parses the synthetic `EvidenceId`s from the listing to cite in `EmitOutput`. This is
  a faithful pull-model adaptation but it changed test *semantics*, not just API — verify on the
  owed live `TEMPORAL_LIVE_TEST=1` run. (All Temporal live runs remain reasoned-only, consistent
  with the #143/#144 live-run debt.)

## Replay-determinism invariant (held sacred in Phase B)

In the Temporal inner loop, rebuild the session **only from journaled activity results held in
workflow state** — never re-read the FS or recompute it in the workflow body. Local Temporal suites
are reasoned-only/non-live (live runs already owed), so a non-deterministic session rebuild passes
locally and only blows up on replay in production.

## Cycle bounding — A+C (maintainer, 2026-06-24): no fixed cap; mid-cycle CAN carries the session

Maintainer chose **no fixed step cap**, with a **mid-cycle continue-as-new that carries the
in-flight session** so a long cycle resumes seamlessly after a rollover. This deliberately
overrides the design's "ephemeral session discarded at cycle end" default — the model's ability
to do long uninterrupted work wins over keeping the carryover minimal.

- **No `INNER_STEP_CAP`.** The inner loop runs until the model chooses `idle` (or a retire signal /
  budget #12). A cycle is bounded by the model's own judgment, not a number.
- **A cycle decouples from a Temporal run.** One unit of mandate work may span multiple runs via
  mid-cycle CAN. `tick` (the cycle counter) increments only at a *true* cycle end (idle), NOT at a
  mid-cycle rollover.
- **Trigger:** between inner steps the Temporal orchestrator checks `continue_as_new_suggested()`.
  False → keep stepping. True → suspend the cycle, pack the in-flight session into carryover, CAN;
  the new run rehydrates the session and **resumes the same logical cycle** (not a fresh seed).
- **Carryover marker:** a "mid-cycle" flag distinguishes resume (rehydrate session, continue inner
  loop) from a normal between-cycles start (build a fresh seed).
- **One-driver-two-executors fit:** the driver returns `CyclePaused(session)` when the (Temporal)
  executor signals "history filling"; the workflow body packs it + CANs. The in-process executor
  never signals suspend (no CAN exists in-process), so the driver always runs to a natural
  terminal — **A+C complexity is Temporal-only**; the in-process path stays simple.
- **Session size guard (my call):** carry the full session when it fits a safe Temporal payload
  threshold (~256KB warn / ~2MB hard). Keep growth bounded by carrying FS references for
  already-persisted evidence rather than re-embedding blobs. If a session ever exceeds the safe
  threshold, fall back *for that one rollover* to FS-resume (force-idle; next wake rebuilds from
  FS) so we never risk a payload-too-large failure. Seamless continuation is the norm; the
  fallback is the rare safety net.
- **Runaway fuse (maintainer, 2026-06-24):** a very-high total-step fuse counted *across
  rollovers within a cycle* — `CYCLE_RUNAWAY_FUSE` ≈ **50,000 steps** (tens of thousands). It
  never bites real work (even a multi-thousand-step task stays far under) but stops a literal
  infinite loop from a never-idling model. On hit → force-idle the cycle + **log loudly** (a
  "this mandate never converges → decompose" signal). It is a fuse, not a reasoning limit;
  superseded by the $/token budget (#12) when that lands. The outer `step_cap`/`INTERIM_STEP_CAP`
  still counts *cycles* and is unaffected.

## ⚠️ Test-migration re-estimate (the dominant risk / split trigger)

Redefining `tick` from "one decision" to "one cycle" cascades into the cap/MockDecide machinery:
- Today: MockDecide popped once per *decision*; cap counts decisions. `[CallTools, EmitOutput,
  Idle]` + cap=3 terminates cleanly.
- After: popped once per inner *step*; cap counts *cycles*. That same 3-entry script is now ONE
  cycle; cap=3 demands 3 cycles ≈ 9 pops → script dry after cycle 1 → MockDecide errors →
  Unhealthy.
- **Every** script's length AND cap must be recomputed against "pops = steps, cap = cycles":
  **31 `MockDecide::new` sites** (21 in loop_smoke.rs) + **~20 step_cap settings** + ~30
  `Idle{next_after}` sites. Plus the known in-proc-errors-on-exhaust vs Temporal-returns-Idle
  divergence interacts with the recompute.
- This is the most error-prone part and the most likely to force a mid-implementation split
  (cf. #141). **If the PR gets unwieldy, land as a stacked sequence** — flagged to the maintainer.

## To verify during implementation

- Multiple `WriteOutput` per cycle ⇒ multiple `ChildOutput` parent signals mid-cycle. Confirm the
  parent's reconcile loop tolerates >1 signal per child per cycle (it should — folds newer output).

## Test plan

- In-process (`loop_smoke.rs`): a multi-step cycle (read → call_tool → write_output → idle) runs
  as ONE cycle; a tool failure adapted within the cycle; own-FS read returns a note; own-FS read
  is read-only (no mutation). **No subtree assertions here** — in-process has no children
  (`SpawnChild`/`RetireChild` are `unimplemented!()` on this path, agent_core.rs:154-165), so there
  is no descendant subtree to authorize.
- Temporal (`workflow_loop.rs` + live smokes): same cycle shape across activity boundaries +
  CAN at cycle boundary; **subtree read returns a child's note** (auth via `child_handles`);
  out-of-subtree read refused.
- Prompt snapshots regenerate (seed + observations replace the windows).
- schemars golden regenerates (ContextPolicy drop from Mandate/AgentDefaults).

## Risks

- Biggest blast radius: the `Decide` trait signature + ~21 MockDecide sites + Temporal mocks +
  CapturingDecide/DeferredSinkDecide wrappers + the workflow orchestration rewrite.
- Replay determinism: the inner-loop conversation must live in workflow state as activity
  *results* (journaled), never reconstructed non-deterministically.
- decide_llm is llm-feature-gated — verify with `--features coral_node/llm-anthropic`.
- Live `TEMPORAL_LIVE_TEST` suites reshape; reasoned-only locally (live runs owed already).

## PR2 — A+C COMPLETE (mid-cycle CAN session-carry), 2026-06-27

Stacked on PR1 (`v5-filesystem-harness-pull-nav-cycle-loop`). **Workflow.rs-only** (+345/−51,
single file): `DecideStepInput` already carries a full `Session`, so A+C needed no activity
changes — exactly "Temporal-only, built last." Whole workspace green: `cargo build/clippy
--all-targets -D warnings` (default + llm-anthropic/cohere), `cargo test --workspace` (Postgres up)
= **568 passed, 0 failed across 37 binaries**, `cargo fmt --check` clean. coral_temporal lib: 88
(incl. 7 new A+C tests).

- **Mechanism:** `Carryover` gains `in_flight: Option<Session>` (the mid-cycle marker; `#[serde(default)]`
  → backward-compatible wire). `encode_carryover` always emits `None` (session is a run-loop LOCAL,
  never workflow state → replay-safe; carried session comes from the immutable workflow input).
  `run()`: extract `resume` from `carryover.in_flight`; outer loop branches resume (skip
  wait/drain/build_seed, honor retire-only via `take()`) vs fresh; `step = session.len()` derives the
  decision-log index (no extra carried counter — `step == session.len()` holds at every inner-loop
  top). Inner loop: after `push`+fuse, `continue_as_new_suggested()` → size guard → Carry (CAN with
  `in_flight: Some(session)`, resume same cycle) or ForceIdle.
- **Size guard (advisor-corrected — was the one blocking bug):** measure the **whole candidate
  `AgentInput`** (`serde_json::to_vec(&candidate).len()`, deterministic CPU), NOT the session alone,
  against `CAN_PAYLOAD_HARD_BYTES = 1.5MiB` (below Temporal's 2MiB `blobSize.error` with margin, so the
  CAN command can't fail payload-too-large and wedge the workflow). Over hard → force-idle
  (`next_wake = INITIAL` so even a `never` agent wakes promptly), boundary-CAN with `in_flight: None`,
  rebuild from FS. `CAN_PAYLOAD_WARN_BYTES = 256KiB` logs but carries. `carry_decision()` +
  `mid_cycle_input()` are pure → unit-tested at boundary values (real CAN can't be forced hermetically).
- **Cleanup (A+C-motivated):** removed dead `mid_tick_evidence` field (no writer, structurally empty;
  its "reserved for mid-tick checkpointing" doc directly contradicted A+C — `in_flight` is that feature).
  Wire-safe (no `deny_unknown_fields`).
- **Invariant locked with a comment:** the mid-cycle CAN check MUST stay strictly after `session.push`
  (that ordering is what makes `step == session.len()` + no-double-execution hold on resume).
- **⚠️ Live-run debt (now load-bearing, not optional):** unlike PR1 (worked without CAN), A+C's entire
  value is the CAN path, which can't be forced hermetically. The owed live run must verify
  `continue_as_new_suggested()` actually fires **before** Temporal's ~50K-event history hard limit
  (the 50k-*step* fuse ≈ 200K events, far over — only CAN protects history). If it fires too late,
  raise with maintainer whether to add a deterministic step-count CAN backstop (they chose
  suggestion-driven → their call, not a unilateral add).
