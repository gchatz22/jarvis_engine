# Realignment — execution plan (GitHub issue breakdown)

*Status: **FILED 2026-06-23** on Project board #1 (https://github.com/users/gchatz22/projects/1).
Tracker #129; verticals V1 #130 / V2 #136 / V3 #131 / V4 #132 / V5 #133 / V6 #134; C1 #142; D1 #151;
budget reused at #121; #114 + #107 closed. The GitHub issues are now the source of truth; this doc is the
reasoning trail. **Nothing implemented yet** — each issue gets a fresh design_realignment.md check +
maintainer go-ahead before coding. Original proposal note follows.*

*Source of truth for the design is
`scratch/design_realignment.md` (its "Target shape" section is authoritative) and
`scratch/architecture_walkthrough_target.md` (the narrative). This doc turns those into a fileable issue
set.*

*"Vertical" here = a **workstream** of the realignment (one of the six synthesizing moves), **not** a
product vertical (finance/OSINT/etc.) — the kernel is vertical-agnostic by VISION.*

---

## How this maps to the six synthesizing moves

`design_realignment.md` reduces the whole set to six moves. Five are active; one (sandbox) is deferred.
The moves are **not independent** — there's a foundation layer two moves share, and the harness + all
provenance mechanics sit on top of it:

```
            ┌─────────────────────────────────────────────────┐
 FOUNDATION │  V1 markdown+git FS   ⇄   V4 DB metadata/ref-graph │   (moves 1 & 4 — intertwined:
            │       (content)            (everything about it)   │    git blob sha = the index value)
            └───────────────┬─────────────────────┬─────────────┘
                            │                     │
        ┌───────────────────┴───┐     ┌───────────┴───────────────┐
   V3 lifecycle (persistence,   │     │  V5 filesystem harness     │  (pull-nav + cycle loop +
   cadence, no max_ticks)       │     │  — the biggest build       │   write_output citations)
        independent-ish ────────┘     └───────────┬───────────────┘
                                                  │
   V2 provider registry  (fully standalone)       │
                                      V6 propagation + consistency (#14b/#14c) — needs V4 ref-graph
```

Dependency-ordered, not six parallel tracks. Many issues ship as **stacked PRs (Graphite)**; bodies must
encode the edges.

---

## Grouping primitive (DEVELOPMENT.md §6)

~17 issues across 6 workstreams → **a GitHub Project (v2) board** is the prescribed primitive (the >10
threshold). Recommended shape: **one board** for the realignment + **one tracking/parent issue per
vertical** (V1…V6) holding that move's slice of `design_realignment.md`, with the concrete issues as its
children. Projects + parent/sub-issues compose.

**Blocker:** creating/managing a v2 board needs a `gh` scope we don't have. The token currently lacks
`project` (the list call failed on `read:project`). You'll need to run:

```
gh auth refresh -s project,read:project
```

Until then I can't create the board or move cards. Fallback if you'd rather not grant it: skip the board
and use **6 parent issues (one per vertical) with native sub-issues** — slightly weaker (no Status
columns) but fully `gh`-creatable today.

---

## Existing open issues — disposition (check-before-filing, CLAUDE.md)

| # | Title | Disposition |
|---|---|---|
| **#114** | Persistent agents — refresh instead of retire | **Superseded** by V3.1 — the realignment *removes* the `persistent` flag (persistence becomes universal), inverting #114's opt-in framing. Close as superseded, or repurpose the thread as V3.1. *Maintainer call.* |
| **#121** | CM-7 (optional): per-graph cost/cycle budget | **Reuse as the budget (#12) issue.** Don't file a new budget issue; expand #121's scope to the realignment's budget role (cycle-termination backstop + sole runaway guard + context bound). Stays deferred. |
| **#107** | MCP-5: enforce agent_tools scoping + surface tool catalog | **Fold into V4.3** (tools reshape) — V4.3 moves tool defs/assignments and is the natural home for #107's enforcement. Keep #107 as the V4.3 issue, or close + re-file. |
| **#102** | MCP tools in graph.yaml end-to-end | Mostly landed (MCP-1..4). Leave/close per your read; C1 (apply rework) touches the same YAML but doesn't re-do this. |
| **#128** | reconcile_children unconstructable from the prompt (bug) | **Keep standalone** — it's a live bug independent of the realignment. Note that V5.2 will revisit reconcile wholesale; if you'd rather, fold it into V5.2. *Lean: fix standalone now, it's small and real.* |

---

## The issue set

Effort: **S** ≈ one reviewable diff; **M** ≈ a session; **L** ≈ multi-session / itself a parent.
Each issue body, when filed, carries: goal · acceptance criteria · in-scope · out-of-scope · dependencies
· effort · **the design_realignment.md slice it implements** (DEVELOPMENT.md §5).

### V1 — Pure-content markdown FS, git-versioned  (#1/#4/#5/#9, A1) — FOUNDATION

**V1.1 — git-per-agent-path versioning behind `AgentStorage`** · **L** · deps: none
- Goal: each agent root (`graphs/<gid>/agents/<aid>/`) is a git repo; versioning = git; content hash =
  git **blob sha** (stop computing our own sha256). Add a builtin **read-at-sha** capability
  (`git show <sha>:<path>` / `cat-file`).
- Scope: `AgentStorage` trait grows a versioning surface scoped to exactly **{commit-per-tick,
  read-blob-at-sha}** — no branch/checkout/merge/rebase; working tree always = HEAD; historical reads via
  `git show`. Blob-sha references are retry-idempotent; commit-per-tick is clean-tree-no-op safe.
- Out: object-store impl (git is the local/dev impl behind the trait — future stage); fork/snapshot.
- Doc: A1 "Versioning mechanism" block; guardrail "content fingerprint as derived index".
- Note: the **trait shape** is the real long-term commitment (must later satisfy an object-store impl);
  get it right here.

**V1.2 — Markdown + slug filenames; pure-content files** · **M** · deps: V1.1, V4.2
- Goal: `evidence/`, `outputs/`, `notes/`, `conflicts/` become slug-named `*.md`; `mandate.json` →
  `mandate.md` (pure prose). Kill `<sha256>.json`. **No metadata in any file** (no created_at/sha/version
  /frontmatter) — interpretable slugs only (`tsmc-cowos-capacity.md`).
- Scope: rewrite `fs.rs` evidence/output/mandate read+write paths; remove `_tail.json` indices (recent-N
  becomes a DB query, V4.2); slug derivation from the model's `ClaimSeed`/label.
- Out: citations-in-file (they go to the DB — A1, V5.3); config-in-file (→ DB, V4.1).
- Doc: Concerns 1/4/5/9; Synthesizing move 1.

**V1.3 — Authorship boundary enforcement** · **S** · deps: V1.2
- Goal: `evidence/` is **runtime-authored only**; the model can read but never write it. notes/outputs
  stay agent-authored. This is what keeps provenance non-fakeable.
- Scope: enforce write-path separation in `fs.rs`/the harness; model FS-write actions reject `evidence/`.
- Doc: A1 "authorship boundary"; invariant "the authorship boundary".

### V4 — DB = metadata + provenance/version graph + topology + config + index  (#6/#7, A1) — FOUNDATION

**V4.1 — Collapse `AgentRecord` to topology** · **M** · deps: V4.2 (schema)
- Goal: drop the vestigial `mandate_ref`, `persistent`, `model` columns from `agents`; the row collapses to
  topology (`{ id, graph_id, name, created_at }`). **No runtime behavior change** — those columns were
  never read back by the runtime; authored config already rides `Mandate → AgentInput →` the Temporal
  durable input (config-home decision, 2026-06-24 — config stays on the durable input, **not** Postgres).
- Scope: migration (DROP 3 columns); `AgentRecord` struct (`coral_graph/src/types.rs`); the apply +
  spawn-child INSERTs and the get/list/children SELECTs; regen the sqlx offline cache; convert the
  column-round-trip tests to assert config still reaches the workflow-input `Mandate`.
- Out: any DB config home / runtime-reads-config-from-DB path (dropped — config lives on the durable input).
- Doc: Concerns 6/5; A1 "where does config live" + the 2026-06-24 config-home revision; Synthesizing move 4.
- Note: V4.1 no longer depends on C1 — the "cycle" was a stale edge here. The real Wave-1 DAG is
  V4.1 → {V4.3, C1}, **C1 strictly last** (it also depends on V4.3's tool-def shape).

**V4.2 — Reference graph + `filepath ↔ blob sha` index** · **L** · deps: V1.1
- Goal: the DB owns the **provenance/version graph**. New schema: a reference-graph table (each citation =
  `(citing file, cited path, pinned blob sha)`), a bidirectional `filepath ↔ blob sha` index (integrity +
  dedup), enumeration, exactly-once allocation, version lineage, timestamps.
- Scope: migrations; query layer; the index is the home of recent-N (replaces `_tail.json`), dedup
  (`hash → filepath`), and integrity (`filepath → hash`).
- Out: the propagation *reactor* that consumes staleness (V6) — this issue builds the graph it reads.
- Doc: A1; Concern 9 decision; Concern 14a/14b data model; Synthesizing move 4.

**V4.3 — Tools reshape (defs graph-scoped, assignments per-agent) + #107 enforcement** · **M** · deps: V4.1
- Goal: split tool **definition** (graph-scoped, shared) from **assignment** (per-agent config). Retire or
  derive `tools`/`agent_tools` as authored today; enforce per-agent scoping at dispatch (folds **#107**).
- Scope: tool-provider read path (`coral_worker/tool_provider`); `execute_tool` per-agent scoping; YAML
  tool-def + per-agent `tools: [...]` assignment.
- Doc: Concern 7; reuses/closes **#107**.

### V2 — Provider/model registry  (#2) — STANDALONE

**V2.1 — Registry keyed by `provider/model`** · **M** · deps: none
- Goal: kill the process-global single-vendor selection. A registry maps `provider/model` →
  client; populated at boot from whatever API keys exist. YAML: `model: anthropic/claude-opus-4-8` |
  `cohere/command-a` | `local/llama-…`. Any agent picks any available provider per its config.
- Scope: `model_client/` registry; prefix→client map; thread `provider/model` through the existing
  per-request `model` plumbing; feature flags become "which providers *can* be available."
- Out: routing *policy* (fallbacks, cost-based auto-routing) — explicitly resisted.
- Doc: Concern 2; Synthesizing move 2.

### V3 — Universal persistence + cadence; remove max_ticks  (#3/#10/#11) — INDEPENDENT-ISH

**V3.1 — Universal persistence; remove the `persistent` flag + model `Retire`** · **M** · deps: V4.1
- Goal: every node is persistent; no flag. Remove `Decision::Retire` from the model's vocabulary;
  termination = kernel/human op (`retire` signal / `RetireChild` / teardown). One lifecycle tail in the
  prompt ("produce/refresh, then idle"). Make `demote_retire_if_persistent` unconditional → then delete
  it. Relax the CM-4 degenerate-config guard (hard error → warning).
- Scope: drop `persistent` (DB col 0003, YAML field, `Mandate` field); `decision.rs` enum;
  `workflow.rs` demotion; `prompt.rs` lifecycle tail; CM-4 guard.
- Doc: Concern 10; Synthesizing move 3; **supersedes #114**.

**V3.2 — Cadence + remove `max_ticks`; interim step-cap; migrate live tests** · **M** · deps: V3.1
- Goal: remove `max_ticks` entirely. Authoring bound = **per-node cadence** (governs only the self
  `ScheduledWake`) with a **"never"** sentinel; all other triggers always live. Add an **interim
  step-cap** as the cycle-termination backstop until budget (#121/#12) lands.
- **In scope (load-bearing): replace the test-termination path.** `persistent_monitor_live` & friends stop
  via `max_ticks` today — removing it breaks them. Migrate to a tiny tripping budget / `retire`-after-N /
  a harness-only cycle cap (never a production authoring field). *Tests must stay green (rule 3).*
- Scope: remove `max_ticks` (YAML, `Mandate`, agent loop); cadence field + "never"; step-cap; test migration.
- Doc: Concerns 3/11; Synthesizing move 3; guardrail "test/debug termination".

### V5 — Filesystem harness: pull-navigation + cycle loop  (#8/#13) — ON TOP OF FOUNDATION

**V5.1 — Pull-navigation: drop `ContextBundle`/`ContextPolicy`; thin seed + FS-nav tools** · **L** · deps: V1.2, V4.2
- Goal: replace push-assembly with pull. A waking agent gets a **thin orienting seed** (mandate + a
  notes/outputs *index* + the waking triggers), then reads what it needs via `read`/`list`/`search` over
  **own FS + descendant subtree (read-only, A6)**.
- Scope: delete `ContextPolicy` windows + `assemble_context` fat path; add FS-nav builtin tools scoped to
  own root ∪ descendant subtree; keep a thin seed builder.
- Doc: Concern 8; Synthesizing move 5; guardrail "warm start + bound".

**V5.2 — The cycle control loop (decide→act→observe→terminal); tick = one unit of work** · **L** · deps: V5.1
- Goal: a tick becomes **one unit of mandate work** — a multi-step inner loop. Collapse the 9-variant
  `Decision` enum into a **research repertoire** (read/list/search · call MCP tools directly, results
  in-context · write_note/write_output · reconcile) + a thin **terminal/topology** surface
  (set_cadence/idle · spawn_child · retire_child). Failures become inner-loop observations
  (`staged_correction` dissolves). CAN fires at cycle boundaries; step-cap (V3.2) bounds a cycle.
- Scope: `agent.rs`/`agent_core.rs` loop; `decision.rs` enum; `workflow.rs` per-cycle orchestration;
  in-cycle ephemeral conversation, cross-cycle continuity via FS.
- Out: `run_code` action (deferred, D1); reconcile-as-structured-primitive's full shape (deferred — near
  -term reconcile is repertoire work). Revisits **#128**.
- Doc: Concern 8 decision + #13 Issue 1; gap-4 cycle loop; Synthesizing move 5.

**V5.3 — `write_output` provenance: `{ body → FS, citations → DB }`** · **M** · deps: V4.2, V5.2
- Goal: restore mechanical, enforced provenance. `write_output` takes `{ body: prose → FS, citations:
  [evidence_ref] → DB }`; runtime **rejects** unless every citation resolves to a real, runtime-authored
  evidence file. Citations live in the DB (never the file); version-pinned to the cited blob sha;
  auto-follow-to-latest rejected.
- Scope: the write_output action + the rejection check; the DB reference-edge write (uses V4.2).
- Doc: Concern 14a; invariant "provenance by construction".

### V6 — Propagation + consistency  (#14b/#14c) — NEEDS V4 REF-GRAPH

**V6.1 — (SPIKE) Staleness→wake propagation design** · **S/M** · deps: V4.2
- Goal: decide the shape of the reactive notification system over the reference graph (its shape is
  explicitly TBD in the design). **Output is an ADR/design note in `scratch/`, not code.** Resolve:
  write-path enqueue vs periodic sweep vs `LISTEN/NOTIFY` (lean: enqueue + sweep backstop); coalescing/
  debounce (don't thrash a hot parent); fan-out bound.
- Doc: Concern 14b; architecture_walkthrough_target §9.

**V6.2 — Staleness→wake propagation implementation** · **L** · deps: V6.1, V4.2, V5.2
- Goal: a child's new blob sha → references that pinned the old sha go stale → runtime wakes the
  dependents (Temporal signal) to re-reconcile + re-pin. Reference graph = dependency graph =
  wake-propagation edges. An all-"never" graph assembles its root result this way.
- Scope: write-path enqueue + coalescing + bounded fan-out per the V6.1 decision; the staleness→signal
  bridge; retention/GC of versions still pinned.
- Doc: Concern 14b; A1↔A4; Synthesizing move (propagation).

**V6.3 — FS↔DB dual-write consistency** · **M** · deps: V1.1, V4.2
- Goal: a write is **one Temporal activity** — git commit first, DB upsert second, both idempotent — so
  retry converges (worst case a recoverable orphan, never a dangling pointer). A reconciliation sweep
  repairs only `filepath ↔ blob sha` (not the reference graph — that's DB-primary, A1, protected by
  Postgres durability).
- Doc: Concern 14c; invariant "determinism/idempotency".

### Cross-cutting bootstrap

**C1 — `graph.yaml` + `coral apply` rework (bootstrap-once, then inert)** · **M** · deps: V4.1, V4.2, V4.3
- Goal: `coral apply` consumes `graph.yaml` **once** — writes DB topology + tool-defs and
  **materializes the FS** (each agent dir + `git init` + initial `mandate.md`) and builds the workflow
  inputs (carrying config to the durable input) — then is inert (no runtime role; the graph evolves via the
  agents). Config is **not** written to Postgres (config-home decision, 2026-06-24). YAML carries qualified
  `provider/model`, cadence (+ "never"), tool defs + per-agent assignments; drops `persistent`/`max_ticks`.
- Dep note: C1 is strictly last in Wave 1 — it materializes tool-defs in V4.3's shape, so V4.3 lands first.
- Scope: `coral_graph/src/yaml.rs` schema; `create_from_yaml` to also materialize FS+git; remove dropped
  fields.
- Doc: Target shape "Authoring"; ties V1/V2/V3/V4 together.

### Deferred — tracking only

**D1 — Code-execution sandbox + MCP-code bridge (#13, future)** · tracking · deps: V5.2
- One tracking issue (not a build). Future opt-in power tool: ephemeral on-demand sandbox, FS mount (own
  repo rw except `evidence/` ro; descendant subtree ro), MCP-code bridge owning tool→evidence,
  budget-at-boundary, pluggable local/Daytona/E2B. The heaviest single build; deferred until a real
  fan-out/quant need. Doc: #13 Issues 2/4/5/7; Synthesizing move 6.

**Budget (#12)** → **reuse #121** (CM-7). Expand its scope to the realignment role (cycle-termination
backstop + sole runaway guard + context bound; scope/denomination/enforcement/exhaustion). Stays deferred;
V3.2's step-cap is the interim scaffolding until it lands.

---

## Suggested execution order (waves)

The doc's own lean: moves 1 + 9 are cheapest **and** most directly serve NVIDIA-run *evaluation* (legible
files to read), so they go first. Foundation before harness.

- **Wave 0 (foundation / legibility-first):** V1.1 → V4.2 → V1.2 → V1.3. *(git + index + markdown + the
  authorship boundary — makes the FS readable & versioned for run-eval.)*
- **Wave 1 (DB + bootstrap):** V4.1 → V4.3 → C1. *(also enables V2.1 to land cleanly.)*
- **Wave 2 (lifecycle + models):** V3.1 → V3.2; V2.1 (standalone, can slot anywhere).
- **Wave 3 (the harness — biggest):** V5.1 → V5.2 → V5.3.
- **Wave 4 (continuous mechanics):** V6.1 → V6.2; V6.3.
- **Deferred:** D1, #121 (budget).

Stacks (Graphite): the Wave-0 chain is one stack; C1 stacks on V4.1; the V5 chain is one stack on the
foundation.

---

## Decisions needed before I file anything

1. **"Vertical" = workstream** (the 6 moves), confirm.
2. **Primitive:** Project board (needs `gh auth refresh -s project,read:project`) vs 6 parent issues with
   sub-issues (creatable today, no Status columns).
3. **Existing issues:** approve the dispositions above (#114 supersede, #121 reuse-as-budget, #107 fold
   into V4.3, #128 keep standalone, #102 leave).
4. **Scope of first filing:** file the whole set, or just Wave 0 (+ tracking issues) and file later waves
   as they near?
5. **Issue-body design slices:** confirm I paste the relevant `design_realignment.md` section into each
   issue (DEVELOPMENT.md §5) vs link the scratch file.
