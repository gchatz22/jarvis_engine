# Design re-alignment — "the implementation got heavy"

*Status: ideation / live design conversation. Started 2026-06-10. Captures a first-principles
review of design weight in the engine as built (post-Stage-5, post-CM-6, MCP-in-graph landed).
Maintainer is surfacing concerns "in no particular order"; more may come. **Many decisions are now
locked** — see **Target shape** below, which is *authoritative*. The per-concern log beneath it is the
reasoning trail and may contain superseded intermediate proposals (e.g. an earlier `mandate.md`-with-
frontmatter sketch); where the log and the Target shape disagree, the Target shape wins. Nothing here is
filed as issues or implemented yet.*

*Read alongside: `VISION.md` §4–5 (state-as-files, model-agnostic, provenance-by-construction),
`scratch/architecture_walkthrough.md` (the as-built three-plane snapshot these concerns react to),
`scratch/temporal_staged_plan.md` (what shipped), `scratch/continuous_monitor_plan.md` (persistence),
`scratch/nvidia_supply_chain.md` (the run that motivates legibility).*

---

## The throughline

The axis being optimized is **legibility + agency** — make the agent's state human-readable, and let the
agent exercise judgment over its own context — *not* "make it lighter." Weight is only a *sometimes*-
consequence: down for the format moves (#1/#4/#5), but #8 *adds* machinery and #9/A1 *relocate* it into
the DB. Naming the real axis matters — it's why #8 is worth its added complexity (agency, not weight),
and it predicts which future concerns are in scope: anything serving legibility or agency.

The original framing still holds as the *symptom*: the implementation optimized for **machine-shaped
correctness** (content-addressing, structured JSON, single-provider determinism, iteration-counting),
and the pull is back toward VISION's *"state as files… the way a human works in a filesystem."*
Legibility + agency is the *axis* under that symptom.

Two load-bearing observations frame the whole review:

1. **Much of the heaviness was justified by scale/durability concerns that aren't load-bearing yet**
   (dedup at millions-of-agents scale, MCP traffic collapse). At demonstration scale they buy ~nothing
   and actively hurt legibility.
2. **Under each heavy *form* there is usually a load-bearing *function*.** The discipline is to
   separate them: drop the form where it's pure weight; relocate the function where dropping it would
   break something real (idempotent retries, provenance citability).

**Connection to the NVIDIA run:** the next milestone is a real multi-cycle run evaluated by *reading
the agent's filesystem*. If that FS is 64-char-hash JSON blobs, evaluation is miserable. If it's
readable markdown, evaluation is natural. Legibility is a **prerequisite** for judging the run, not a
side-quest.

---

## Organizing principle (raised after the architecture walkthrough): *agents are filesystem paths*

Concerns 6–7 are deeper than 1–5: they're not about a file's *format*, they're about the **role of the
structural DB**. The maintainer's frame: **an agent IS its filesystem path.** The only thing the DB
needs to know about an agent is *where it lives*; the mandate, the tool list, the outputs, the evidence
— everything else — are files under that path.

This reframes the three-plane architecture (Postgres topology / FS working-memory / Temporal
execution). The defect it targets: **the DB holds primary copies of things that also live in the FS**
(`mandate_ref` half-pointer; `persistent`/`model` columns; `agent_tools` rows) — forcing dual-write and
the exact `mandate_ref` ugliness flagged. The clean split to align on:

- **Filesystem = the *content* store.** Agent identity = its path; mandate (prose), notes, outputs,
  evidence all live under it as pure-content files (git-versioned). This is "state as files."
- **The DB = the metadata + provenance/version graph + topology + index.** *(`config` was listed here
  until **2026-06-24**, when it moved to the Temporal durable input — see the config-home revision note
  below.)* *(Initially framed
  as a thin derived index over an authoritative FS; **A1 (2026-06-21) revised this** — timestamps,
  version lineage, and the reference graph are DB-*primary*, not reconstructable from pure-content files.
  The DB-survival fork is therefore **settled at A**: the DB is essential.)* Slogan: **"FS is the content
  store; the DB is the provenance/version graph."**

See **Target shape** below for the composed end-state (authoritative).

---

## Target shape (the spec — nouns, authoritative)

*The end-state the decided concerns compose into. Where a concern section below shows an earlier
intermediate proposal (e.g. `mandate.md` with frontmatter), **this section supersedes it.***

**An agent IS a filesystem path.** `graphs/<gid>/agents/<aid>/` is a git repo and the agent's whole
durable self. Identity = path; everything the agent *is* lives under it.

**Per-agent FS — pure content, git-versioned:**
```
graphs/<gid>/agents/<aid>/      ← a git repo (.git/ used for versioning only)
  mandate.md         pure prose — the standing instruction. No frontmatter, no metadata.
  notes/*.md         agent-authored working memory (the primary continuity mechanism)
  outputs/*.md       agent-authored deliverables; pure prose (citations live in the DB, not the file; #14a)
  evidence/*.md      runtime-authored tool observations (model has READ-ONLY access; only the runtime writes)
  conflicts/*.md     reconciled disagreements
```
- Every file is **pure content** — no `created_at` / sha / version / frontmatter *in the file*.
- Filenames are **interpretable slugs** (`tsmc-cowos-capacity.md`), never hashes.
- **Versioning = git:** one commit per tick ("commit = cycle"); content hash = git **blob sha**;
  read-any-version via `git show`/`cat-file`; surface scoped to **{commit, read-blob-at-sha}** (no
  branch/checkout/merge/rebase). Git is the *local* impl behind the storage trait; object-store
  equivalent is a future stage.
- **Authorship boundary:** evidence is runtime-authored (tamper-evident); notes/outputs are
  agent-authored prose. In the sandbox, model code has rw on `notes/`+`outputs/`, **read-only** on
  `evidence/` (#13).

**The DB (Postgres) — metadata + provenance/version graph + topology + config + index** (fork settled at
A; primary for what pure-content files can't carry — content stays pure in the FS, all
metadata/provenance lives here; protect via standard Postgres durability, #14c):
- **Topology** — which agents exist; parent→child edges; path-keyed.
- **Config** — cadence, model (`provider/model`), tool-assignment; authored via `graph.yaml`, written
  here, **not** in the FS.
- **Reference graph** — every citation as a `(citing file, cited path, pinned blob sha)` binding: the
  provenance edges, version-pinned.
- **Index** — `filepath ↔ blob sha` (bidirectional: integrity + dedup); enumeration; exactly-once
  allocation.

**Authoring — `graph.yaml` is a one-time bootstrap.** It defines the *initial* graph: topology + tool
definitions + initial mandates. `coral apply` consumes it **once** — writes the DB rows
(topology/config/tool-defs) and materializes the FS (each agent's dir + git repo + initial `mandate.md`)
— and is then **inert** (no runtime role; the graph evolves via the agents themselves). The **root
output** = the top-level output of the root node (`…/agents/<root>/outputs/…`), the current view the user
reads.

**Temporal — orchestration/scheduling (role thinned by #13).** One workflow per agent, now a *thin* loop:
durable timers (sleep/wake), signal inbox (child outputs / human / staleness), child-workflow topology,
continue-as-new — and **near-zero idle cost** (a parked workflow is DB rows, not a running sandbox). Per
cycle it orchestrates the inner-loop activities (each direct tool call keeps its own per-tool boundary; a
code run — deferred — would be one activity) and records the kernel decision. *(Real alternative axis if
ops weight bites: a lighter durable-execution engine — DBOS/Restate/Inngest — not a sandbox. Lean keep
Temporal.)*

**Code-execution sandbox — DEFERRED (future direction; not in the near-term shape; #13).** *When it
lands:* an on-demand, pluggable, ephemeral power tool — spun up only when the agent invokes a code action,
mounts the agent's FS (own repo rw except `evidence/` ro; descendant subtree ro), exposes the MCP-code
bridge (owns the tool→evidence write path), enforces budget; local dev / Daytona/E2B prod. **Durability
would have three homes:** Temporal (scheduling/lifecycle), FS+DB (work/provenance), and the sandbox holds
**nothing** durable (re-runnable; the content-addressed tool cache skips already-fetched calls).
*Near-term: no sandbox — the Act is direct tool calls in-context.*

**Lifecycle — two knobs, nothing else:**
- **Persistence is universal** (no flag). An agent never self-terminates; `Decision::Retire` is *not* in
  the model's vocabulary. Termination = a `retire` signal / `RetireChild` / teardown (kernel/human).
- **Cadence** (per node) governs *only* the self-scheduled `ScheduledWake`; **"never"** = no self-wake.
  All other triggers (child/upstream update, human, external) are always live — the *dependency graph*.
- **Budget** (#12, deferred) is the intended *sole* runaway guard / cycle-termination backstop; until it
  lands, an **interim step-cap** does the job (temporary scaffolding, not the end state).
- **Tick = one unit of mandate work** = one inner-loop cycle (direct tool calls, results in-context) that
  may produce/refresh an output, then idles.

**The harness — Filesystem harness; the cycle control loop (gap 4, resolved 2026-06-22; #8/#13).** On wake
(cadence timer or trigger): build a **thin orienting seed** (mandate + a notes/outputs *index* + the
waking triggers), then run a **multi-step inner loop** — `decide → act → observe → … → terminal`:
- *decide* = one LLM call (a Temporal activity). The model acts **only by calling tools** — one uniform
  action space.
- *research actions (near-term):* `read`/`list`/`search` over own FS + descendant subtree (read-only),
  **call MCP tools directly** (results land **in-context**), write `notes/`+`outputs/` (prose), reconcile
  children. Each is one activity; the runtime captures evidence on tool calls.
- *terminal / topology actions:* `set_cadence`/`idle` ends the cycle (→ sleep); `spawn_child` /
  `retire_child` are executed by the workflow.
- **(A) In-cycle context is an ephemeral conversation; cross-cycle continuity is the FS** — the
  conversation is discarded at cycle end; `notes/`+`outputs/` carry forward. (This is *why* notes are the
  primary continuity mechanism.)
- **(C) Failures are inner-loop observations**, not cross-tick corrections — the model adapts on the next
  step; `staged_correction` dissolves.
- **(D) Termination** = the model calls a terminal action, or an **interim step-cap** trips — *temporary
  scaffolding until budget (#12); not the end state.*
- **(E) Continue-as-new fires at cycle boundaries**; a single cycle's history is bounded by the step-cap.
- No `ContextBundle`/`ContextPolicy` (pull, not push).

**(B) Code-execution is deferred** (future direction — see #13's second amendment): later, fan-out/quant
work becomes a `run_code` action whose raw results stay out of context (→ FS). Near-term, everything lands
in context via direct tool calls.

**Models** — a registry keyed by `provider/model`; any agent picks any available provider per its config.

**Propagation (topology-driven; child→parent, one hop up — REVISED 2026-07-01).** A node's new output
wakes its **single direct parent** (the existing best-effort `ChildOutput` Temporal signal); the parent
re-reconciles and, if its own output changes, wakes *its* parent — so a change ripples to the root up the
parent→child tree, hop by hop. An all-"never" graph assembles its top-level result this way. **The
reference graph (citations) is provenance ONLY — it is NOT consulted to decide who to wake.** This is the
"child→parent output propagation" mechanism the Invariants below already commit to, not a separate reactive
subsystem; the earlier reference-graph-reactor framing is descoped (see the revised Concern 14b).

**Provenance (preserved by construction, #14a)** — `write_output` carries `{ body: pure prose → FS,
citations: [evidence_ref] → DB }`; the runtime rejects the output unless every citation resolves to a
real, runtime-authored evidence file (output-level — the old `EmitOutput` guarantee). Citations live in
the DB, never in the file; tooling overlays them. Each resolves to a version-pinned evidence file; old
outputs stay pinned to the versions they cited (time-scrubbable); auto-follow-to-latest is rejected.

**FS↔DB consistency (#14c)** — a write is one Temporal activity: **git first, DB upsert second, both
idempotent**, so Temporal's retry converges (at worst a recoverable orphan, never a dangling pointer). The
DB is **not** fully FS-rebuildable (provenance is DB-primary, A1) — the sweep repairs only
`filepath↔blobsha`; the DB's provenance state is protected by standard Postgres durability.

---

## Invariants that do NOT change (this is evolution, not a rewrite)

The redesign reshapes *form*; these principles/mechanisms are explicitly preserved:

- **Temporal as the orchestration/scheduling layer** — workflow-per-agent, durable timers,
  signals-as-inbox, child-workflow topology, continue-as-new, near-zero idle cost. *Role thinned by #13*
  (per-tool activities for direct calls; a code run — deferred — would be one coarse activity), but the
  layer stays.
  (Watch-item: the real alternative is a lighter durable-execution engine like DBOS, **not** a sandbox;
  revisit only if ops weight bites.)
- **Parent→child graph topology + child→parent output propagation.** The *mechanism* (cross-agent reads
  + reconciliation) stays; it generalizes to read-only subtree reads (A6).
- **MCP-native tools** — data fetchers are MCP servers; per-graph registries. Unchanged.
- **Provenance by construction** — "no claim without a trail to evidence" is non-negotiable. What changes
  is the *form* (footnote → DB → blob sha), not the *guarantee*.
- **Content-addressing as the *mechanism*** — still content-addressed underneath (git blob shas) for
  idempotency + integrity + dedup; just never the human surface.
- **The authorship boundary** — runtime-authored evidence vs agent-authored prose: what keeps provenance
  non-fakeable. Under code-execution (#13) the runtime owns the tool→evidence write path and the model
  has **read-only** access to `evidence/`.
- **Determinism / idempotency on the Temporal path** — retries stay byte-safe (blob shas content-derived;
  git clean-tree no-op; `scheduled_time` for any timestamps).
- **DEVELOPMENT.md discipline** — smallest correct diff, tests-with-change, plan-before-code. These are
  design decisions, not yet a work queue.

---

## Concerns

| # | Concern (maintainer) | My read | Verdict | Status |
|---|---|---|---|---|
| 1 | Content-hash per output/evidence is heavy — too many long hashes | Valid for the *filename*; the *function* (idempotent retries) must be relocated, not deleted | Agree w/ caveat | open |
| 2 | Picking one provider is heavy; YAML should use models from any provider per-agent | Real smell, contradicts VISION's model-agnosticism outright | Agree | open |
| 3 | Ticks / max_ticks feel weird; want to bound timesteps + max but not like this | Concept fine *internally*; wrong as the authoring bound — **superseded by #10/#11** | Agree | resolved |
| 4 | JSON for evidence feels off; agents should write text/md; metadata (searchTime) shouldn't be in the body | Strongest, most vision-aligned; folds into #5 | **Decided** (+ A1: authorship boundary, metadata→DB) | decided |
| 5 | Mandate as JSON feels off; text/md makes more sense | Trivially yes | **Decided: mandate.md pure prose; config (cadence/model/tools) → DB** | decided |
| 6 | `AgentRecord.mandate_ref` — DB↔FS link should literally just be the filesystem path | Strong agree; reinforces #5; collapses the agent row to topology | Agree | open |
| 7 | `agent_tools` in DB — which tools an agent has should be a file in the FS | Agree; split tool *definition* (graph-scoped) from *assignment* (per-agent), both → FS | Agree w/ distinction | open |
| 8 | ContextBundle/ContextPolicy is wrong; agent should explore dynamically, fetch its own sources, reuse its own notes by intelligence | Strong agree; the natural end of the markdown-FS move — push-assembly → pull-navigation | **Decided: ReAct inner loop, tick = one unit of mandate work** | decided |
| 9 | sha evidence id isn't interpretable; want content-addressed *and* readable ids (collaborative) | Agree; refines #1 | **Decided: interpretable names + DB `filepath ↔ hash` binding** | decided |
| 10 | No non-persistent graph; persistence is universal; one-off = cadence "never" | Strong agree; makes VISION's default the only mode; one-shot becomes a degenerate cadence | **Decided: remove the `persistent` flag** | decided |
| 11 | Remove max_ticks; one tick = one output cycle; per-node re-wake config (or never) | Agree; budget becomes sole runaway guard; resolves #3 | **Decided: no max_ticks; per-node cadence + budget** | decided |
| 12 | *(raised by critique A2)* budget — load-bearing for #8/#11 but unspecified | Agree it's load-bearing; needs its own pass | **Deferred — define later** | deferred |
| 13 | Make the Filesystem harness (FS-context, progressive disclosure, code-execution) the standard | Agree; FS harness *subsumes* ReAct; ratifies #8. **Amended:** code-execution = opt-in *power tool* (fan-out/quant), not the mandatory Act | **Decided: FS harness = standard; code-exec = opt-in power tool; reconcile stays structured** | decided |

---

### Concern 1 — content-hashing for provenance

**Today.** `EvidenceId` / `OutputId` = sha256 of canonical content; that 64-char hash is *also the
filename* (`<sha256>.json`).

**Two complaints, two answers:**
- *Hash as filename* → pure weight. Kill it. Browsing `evidence/` should show
  `tsmc-cowos-capacity.md`, not a hash.
- *Content-addressing as identity primitive* → earns its keep for exactly two functions:
  (a) **idempotent retries** on the Temporal path (retried activity writes byte-identical content to
  the same key → a worker crash mid-tick doesn't duplicate); (b) **dedup** of identical tool calls.

**Pushback / what to preserve.** (b) is a *scale* optimization — drop it as a driver at this stage.
(a) is **not optional** on a durable runtime, but idempotency wants a *deterministic* key, not a
*hash* key — `<tick>-<seq>-<slug>` is equally deterministic and readable. Keep the content fingerprint
as an *internal field* (git's trick: content-addressed underneath, human refs on top); stop making it
the surface.

**Proposed lighter shape.** Human-named files; deterministic readable keys for retry-idempotency;
content fingerprint demoted to metadata.

---

### Concern 2 — pick-a-provider is heavy

**Today.** Vendor chosen by feature flag + worker-boot env (`CORAL_MODEL_VENDOR`) → one process-global
`Arc<dyn Decide>`. Per-agent `model: claude-opus-4-8` exists; per-agent **vendor** does not. So an
Opus (Anthropic) parent reconciling Cohere/local children is impossible — directly contradicts VISION
§7 ("route each mandate to the cheapest model that meets the bar") and the model-agnostic principle.

**Proposed lighter shape.** A **model registry keyed by qualified name**. YAML: `model:
anthropic/claude-opus-4-8` | `cohere/command-a` | `local/llama-…`. Runtime resolves `provider/model` →
client via a prefix→client map populated at boot from whatever keys exist. Feature flags become "which
providers *can* be available," not "which one *is* THE one." The per-request `model` plumbing already
threads through — extend it to carry provider.

**What to resist.** A routing *policy* engine (fallbacks, cost-based auto-routing). Just qualified
names + a registry. Smallest version that makes the vision true.

---

### Concern 3 — ticks / max_ticks

**Today.** A "tick" = one wait→drain→decide→dispatch iteration. `max_ticks` does triple duty:
runaway-protection, cost-cap, lifetime-cap. It's a bad proxy for all three and **self-contradictory
with persistence** — a monitor's value is longevity, CM demotes `Retire`→`Idle` to keep it alive…
then `max_ticks` kills it anyway. `max_ticks: 8` in the persistent example is a *test guardrail*
cosplaying as a monitor bound.

**Decompose the three axes it conflates:**
- **Constrain model output** (per-decision): max output tokens, maybe max parallel tool calls.
- **Measure/enforce timesteps** (cadence): `idle_period` already does this. A monitor's timestep is
  **wall-clock cadence**, not iteration count. *Subtlety:* agent A's tick #5 ≠ agent B's tick #5 in
  wall-clock — ticks measure an agent's own progress; wall-clock measures the graph's time (what
  snapshot/time-scrub needs). Don't let ticks pretend to be graph-time.
- **Runaway/cost protection** (budget): denominated in $/tokens (the deferred CM-7). *This* is what
  should stop a misbehaving monitor.

**Pushback / what to preserve.** Keep `tick` as an *internal* counter — load-bearing for
continue-as-new history thresholds and decision-log numbering. Demote `max_ticks` to a *debug/test
fuse*; stop exposing it as the operator's monitor bound. Authoring surface = **cadence + budget**;
ticks retreat to plumbing.

**Superseded (2026-06-13) by #10/#11.** Stronger than "demote": `max_ticks` is *removed*, and
persistence becomes universal (no flag). Authoring surface = per-node cadence (+ a "never" sentinel) +
budget; budget is the *sole* runaway guard. `tick` survives only as an internal counter — and per #8
now *means* "output cycle." See #10/#11 for the decisions.

---

### Concerns 4 & 5 — JSON evidence + JSON mandate → one move

> **Superseded in part by A1 (see Target shape).** The "markdown-*with-frontmatter*" framing and the
> `mandate.md` frontmatter example below are an *earlier* proposal — A1 moved config to the DB and made
> `mandate.md` pure prose. The markdown + interpretable-names + authorship-boundary core stands; the
> frontmatter part does not. Kept for the reasoning trail.

The most vision-aligned thread. #4, #5, and #1's filename are **the same fix: markdown-with-frontmatter
as the on-disk format for the per-agent FS.**

**Nuance on #4 — two things are conflated in "evidence":**
- *The agent's own writing* (distilled understanding, draft, memo, notes, output) → prose. Markdown,
  full stop.
- *Raw evidence* (immutable "called tool X, args Y, got Z at T") → inherently structured. Make it all
  free markdown and you lose the structure provenance needs. But the *result* is a natural artifact
  (body), and the metadata (`searchTime`, tool, args) is **sidecar/frontmatter, not body** — exactly
  the maintainer's instinct.

**Frontmatter resolves #4, #5, and #1's filename together:**

```markdown
---
idle_period: 1h
persistent: true
model: anthropic/claude-opus-4-8
---
You coordinate a supply-chain risk assessment of NVIDIA's
data-center accelerator stack…
```

That's `mandate.md` — human-writable, human-readable, and *parent-agent-writable* (an agent spawning a
child writes a markdown file with a few frontmatter lines — far more natural than a JSON blob). Kernel
parses frontmatter for knobs; humans/agents read the body as prose. Same pattern for evidence
(metadata in frontmatter, fetched content as body), outputs (memo body + cited-evidence handles in
frontmatter), notes, conflicts. **One format unifies the whole FS** — literally "state as files."

**What to preserve.** Outputs must still **cite resolvable evidence handles** (provenance-by-
construction, non-negotiable). The handle becomes a readable slug; the file becomes `slug.md`; the
citation lives in frontmatter. Provenance survives; hash-JSON form dies.

---

### Concern 6 — `AgentRecord.mandate_ref`: the DB↔FS link should *be* the path

**Today.** `AgentRecord = { id, graph_id, name, mandate_ref, persistent, model, created_at }`. The
mandate *text* isn't in the DB — `mandate_ref` is an opaque handle with no FK ("mandates live outside
the DB"). Meanwhile `persistent` and `model` are structured state duplicated as columns that *also*
belong in the mandate.

**Take — strong agree, and it reinforces Concern 5.** If the agent is its path, the mandate is just
`<agent_path>/mandate.md` by convention — `mandate_ref` points at a thing the path already locates, so
it disappears. And `persistent`/`model`/`idle_period` move into `mandate.md` frontmatter (exactly
Concern 5's move), so they leave the DB too. The agent's row collapses to pure topology: roughly
`{ path, parent_path }`. The DB→FS link becomes the path, as asked.

**Load-bearing function to preserve.** *Enumeration + exactly-once allocation.* Something still has to
answer "what agents exist in this graph" and "mint this child exactly once under concurrency." Today
that's the DB row + UUID + unique constraints. In an FS-first world it's a path + `put_if_absent`
(see the fork).

**Proposed shape.** Drop `mandate_ref`, `persistent`, `model` from the agent row. Agent identity =
path; all per-agent definition lives in `<path>/mandate.md`. The row (if a DB remains) is
`{ path, parent }`.

---

### Concern 7 — `agent_tools`: which tools an agent has belongs in the FS

**Today.** Tool *definitions* (`tools` table: kind/command/args/env) + per-agent *assignment*
(`agent_tools` M:N) both live in Postgres; the tool-provider reads `list_tools_for_graph` to build the
per-graph MCP registry. Note: per-agent scoping is **authored but unenforced** — runtime dispatch is
global-by-name within a graph (`execute_tool` → per-*graph* registry, not per-agent).

**Take — agree, with one distinction.** Two different things are bundled in "tools":
- **Definition** (what `web-search` *is*: `npx exa-mcp-server`, `EXA_API_KEY`) — *graph-scoped, shared*.
  Natural home: a graph-level FS file, e.g. `graphs/<gid>/tools.yaml` (or `.md` frontmatter).
- **Assignment** (which tools agent `fabrication` may call: `[web-search, x-search]`) — *per-agent*,
  part of the agent's definition. Natural home: `mandate.md` frontmatter `tools: [...]` under the
  agent's path.

The tool-provider reads these FS files instead of the DB. Since runtime scoping is *currently
unenforced*, the FS file is also the obvious place #107's enforcement reads from when it lands.

**Load-bearing function to preserve.** The per-graph MCP registry still needs *one* place to read "what
servers does this graph run, deduped." That read just moves DB → FS (graph-level `tools.yaml`).

**Proposed shape.** Tool definitions → graph-level FS file; tool assignments → per-agent `mandate.md`
frontmatter; `tools` + `agent_tools` tables retire (or become a derived index).

---

### Concern 8 — ContextBundle / ContextPolicy: push-assembly → pull-navigation

**Today.** Each tick the runtime *assembles* a `ContextBundle` (mandate + recent-outputs window +
recent-evidence window + open claims + triggers + correction), window sizes fixed by `ContextPolicy`.
The model is *handed* a pre-selected slice; it never chooses what to load.

**Take — strong agree; the natural end of the markdown-FS move.** `ContextPolicy` is a retrieval
heuristic that exists *because* today's FS is hash-named JSON the model can't browse. Make the FS
legible markdown (move 1) with interpretable names (#9), give the agent **FS-navigation tools**
(`list`, `read`, `search`/grep), and the windowing policy's job evaporates: the agent pulls what its
intelligence says it needs — its own past notes, prior outputs, specific evidence — instead of being
fed a fixed window. Exactly how a coding agent works (Read/Grep/Glob, not a "context bundle"), and what
VISION means by "distilled memories, prior reasoning… reuse." **Push → pull.** Side effect: `notes/`
graduates from vestigial scratch to the **primary continuity mechanism** — good notes make future
pulls cheap.

**Load-bearing functions to preserve (don't let these evaporate with the policy):**
- **Warm start.** A freshly-woken agent (post-CAN, or after an hour idle) has *zero* in-context memory;
  pure-pull wakes it blind. Keep a **thin orienting seed** — mandate + a notes/outputs *index*
  (pointers, not contents) + the triggers that woke it — then let it pull the rest. Not "bundle vs no
  bundle"; "fat policy-window → thin seed + navigation tools."
- **Bound / cost.** `ContextPolicy`'s real hidden job was *bounding* prompt size. That moves from a
  static window to an **economic constraint** (#3's budget) — a dumb agent re-reading its whole history
  every tick must pay for it. Discipline shifts from policy to budget + judgment.
- **Provenance** gets *cleaner*: the agent cites the specific notes/evidence it actually read, not a
  runtime-guessed window.
- **Temporal determinism** *improves*: each dynamic read is its own journaled activity; the workflow
  body stays pure orchestration.

**The one real structural implication — sub-fork on loop shape:**
- *(i) Reads-as-tools, reuse today's loop.* Add `read`/`list`/`search` as builtin tools; the model
  calls them via `CallTools`, sees results next tick, continues. **Lowest effort** (no new machinery),
  but stutters (one wake per read), and "reading your own note → evidence record" is semantically odd.
- *(ii) Agentic inner loop within a tick.* The model does a ReAct loop (read ↔ reason ↔ act) inside one
  wake, each step a journaled activity. **More "on the spot"** (what the maintainer wants), more
  machinery, must stay replay-deterministic. *Lean: (ii) for the UX, but it's the bigger build — name
  it as a deliberate decision.*

**Proposed shape.** Drop `ContextPolicy` (static windows). Replace push-assembly with: thin orienting
seed + FS-navigation tools + budget. Decide the loop-shape sub-fork (i vs ii).

**Decision (2026-06-13).** Sub-fork **(ii): an agentic ReAct inner loop**, where **a tick = one full
execution of the unit of work described by the mandate.** The agent wakes, runs a multi-step ReAct
session (pull context dynamically ↔ reason ↔ call tools / read notes ↔ …) until it has produced this
cycle's deliverable, then idles until the next wake. A tick is no longer "one decision" — it's "one
refresh/synthesis cycle."

Structure (two nested loops):
- **Outer (wake boundary):** `wait_for_tick` (trigger or timer) → run one inner session → the agent
  ends the session by choosing to idle/sleep → back to wait.
- **Inner (the unit of work):** thin orienting seed (once) → loop { decide → if read/tool: execute,
  append observation, continue; if emit: persist; if done (idle/retire): break } — every inner step a
  journaled activity; the pure workflow body orchestrates the loop deterministically.

Consequences:
- **Termination + budget become load-bearing, not optional.** The session ends when the model signals
  done *or* a per-tick step/token/$ budget trips. Without the budget a ReAct loop can read/search
  forever and never emit — so #3's budget is now a **prerequisite for #8**.
- **Partially rehabilitates the tick (link to #3).** "12 refresh cycles" is a *meaningful* count,
  unlike "12 decide-iterations" — `tick`-as-a-unit regains interpretability. Budget is still the better
  runaway guard, but a max-cycles fuse now means something sensible.
- **Simplifies the correction loop.** Tool failures become inner-loop observations the agent adapts to
  *within* the same session — the cross-tick `staged_correction` machinery largely dissolves.
  Health/retry budgets become per-session budgets.
- **Mandate-authoring implication.** The mandate must describe a *bounded per-cycle deliverable* clearly
  enough that the model knows when this cycle is "done." Prompt work shifts from "do X then retire" to
  "each cycle, produce/refresh Y."
- **Read scope = own FS ∪ entire descendant subtree (read-only) (A6).** The pull tools
  (`list`/`read`/`search`) are scoped across agent roots: a node may read the filesystems of its whole
  children *subtree*, not just direct children (generalizes today's direct-child reconcile read).
  Cross-node citations pin a version (A1) and propagate staleness upward.

**Reshaped by #13, then simplified (2026-06-22).** Pull-navigation stands and is the resolved cycle loop
(see Target shape): a multi-step inner loop of direct tool calls / reads / writes, results **in-context**.
Code-execution is **deferred** (future power-tool). The "reads-as-tools vs inner-loop" sub-fork is mooted
— it's an inner loop. See #13 + Target shape.

**Refinement (2026-06-27) — the standing status note: warm-start made robust, summary-discipline as
telemetry (not a gate).** The "warm start" function above keeps a *pointer* index, but a freshly-woken
agent still pays a cold re-read to reconstruct where it left off. Close that with a single agent-authored
**standing status note** under `notes/` — its running progress and current outlook on the mandate
(conclusions so far, what it's investigating, what's open). Three deliberately minimal moves:

- **Pin it in the seed.** The note is the agent's durable cross-cycle memory, so the seed *always*
  surfaces its filename — even once the agent has more notes than the index cap, where the plain
  recency/lexicographic tail would otherwise drop it (precisely when a long-running monitor most needs
  it). The agent still *pulls* the body on demand: a pin guarantees discoverability, not in-seed content.
  Bodies never ride the seed — that's what keeps the push→pull shape intact *and* the continue-as-new
  carryover bounded.
- **Urge it in the prompt as self-interest, not a rule.** A standing invariant tells the agent to keep
  the note current *because* a current note lets its next wake start from its own synthesis instead of a
  cold re-read. The incentive is intrinsic — an agent that lets the note rot pays the re-derivation cost
  itself next cycle. No new repertoire step: it writes the note with the ordinary FS-write it already has.
- **Measure compliance; do not gate on it.** At the cycle's `idle` terminal the host emits telemetry
  recording whether the agent refreshed the note this cycle — pure inspection of the cycle's own steps
  (no FS read, so it's replay-deterministic, and the carried session sees writes across any mid-cycle
  rollover). It is **not** a gate: we do not reject `idle`, force a write, retry, or auto-write on the
  agent's behalf. Why measure-not-gate: (1) "a write happened" is checkable but "the summary is true" is
  not — gating the cheap proxy invites perfunctory writes (Goodhart); (2) a hard gate would contaminate
  the very question the first real-model run exists to answer — *do agents keep a status note current on
  their own?*; (3) skipping degrades gracefully — the pin still surfaces the last good note, never a
  void; (4) the path where a final summary matters most, retirement, must stay unblockable, so it can't
  be gated regardless. If real-model data later shows agents skip it *and* skipping demonstrably hurts the
  next cycle, revisit a *soft* nudge (one correction, then idle regardless, always logged) — never a hard
  gate.

This refines the "warm start" load-bearing function; it changes neither the seed's push→pull shape (the
note is pulled like any file) nor the FS-as-continuity model (the note lives in `notes/`, the primary
continuity mechanism).

**Refinement (2026-06-28) — seed-index effectiveness: recency-ordered notes, truncation signpost,
smaller window.** The pointer index above was effective only by luck for `notes/`: outputs were surfaced
most-recent-first (a recency sidecar), but notes were a *lexicographic* tail — for topic-named notes the
surfaced subset was arbitrary, neither recent nor relevant. And both buckets truncated *silently* — the
agent saw N filenames with no signal the view was partial, so it had no reason to explore for the rest.
The bound itself is correct (the index is paid in prompt tokens + attention every cycle); the defects
were the *ordering* that filled it and the *silence* about what it omitted. Three moves, all behavioral:

- **Recency-order notes too.** `notes/` gets its own recency index (maintained on every write/delete,
  rebuilt from disk after a crash), so the surfaced subset is the most-recently-touched — parity with
  outputs, and the most defensible default for "where did I leave off." Bodies still never ride the seed.
- **Signpost the truncation.** When a bucket holds more files than are surfaced, the index says how many
  (`+N more` exact, or `N+ more` as a lower bound) and points at `list`. The count is *free*: the recency
  index already holds it — while sub-capacity it is the complete set, so the overflow is exact and in
  hand; only at capacity is it a lower bound. No full listing is paid to count. The standing invariant is
  also widened: the index is *most-recent, not all* — explore for anything not shown.
- **Shrink the window.** The per-bucket cap drops to a small constant (mirroring the FS layer's existing
  "recent" window). With recency + signpost + explore-nudge in place, the safety net is explicit, so the
  seed can return to "orient, don't carry" rather than a fat tuned window; the rare need for a
  non-surfaced file costs one `list` round-trip, paid only when it actually arises.

Rejected: a sortable filename convention (date/sequence prefixes) — complicates filenames and hurts
readability. Deferred: relevance/semantic selection of the window (now *viable* — index assembly runs in
a journaled activity, so the selection need not be deterministic — but it adds per-cycle inference and
grows the kernel; revisit only if recency proves insufficient in a real run). The pinned status note
above is the agent-curated complement to this machine-curated recency window.

---

### Concern 9 — evidence id: content-addressed *and* interpretable (refines #1)

**Today.** `EvidenceId = sha256(canonical_json(tool, args, result))` — distinct, deterministic, stable,
but maximally un-interpretable (64 hex). Maintainer likes content-addressing (distinct + deterministic)
but wants a readable id. Explicitly a *figure-it-out-together*.

**What an id must be** — five properties; tension only between the last two: distinct ·
deterministic/content-addressed · stable · **citable (short)** · **interpretable**.

**Options on the table:**
- **A — slug + short content-hash suffix.** `tsmc-cowos-capacity-a3f8c1`. Slug = meaning; 6–8 hex =
  uniqueness + determinism. (git short-hash / "human title + opaque id" pattern.)
- **B — structured readable path.** `evidence/web-search/2026-06-12/tsmc-cowos.md` — tool/date/topic as
  the path, browseable as a tree; date from Temporal `scheduled_time` keeps retry-idempotency. Not
  strictly content-addressed (slug collisions → version suffix).
- **C — monotonic counter + slug.** `e0042-tsmc-cowos`. Short, ordered, readable — but **drops
  content-addressing** (allocation-order-dependent → fights replay determinism + loses dedup). The one
  that sacrifices the property you said you like; avoid.
- **D — readable surface, full sha as metadata.** Filename/cite-handle is the slug; the full sha lives
  as a `fingerprint:` frontmatter field used only for dedup/idempotency. Keeps *all* content-addressing
  function; changes only the surface.

**Recommendation: A + D.** Id = `<slug>-<shorthash>`, full sha retained as `fingerprint` frontmatter.
The hook: the model **already emits `ClaimSeed`** on each tool call ("opaque hint for evidence
linkage") — surface that (or a dedicated `label`) as the slug. The agent knows best what the evidence
is *for* ("intelligence on the spot"); the label is part of the journaled `Decision`, so replay
reproduces it; the shorthash is content-derived. Result: interpretable · distinct · content-addressed ·
citable · retry-idempotent — losing only the unreadable surface. Outputs follow the same pattern
(`<title-slug>-<shorthash>`); covered under #1.

**Linkage:** #9 is a *prerequisite* for #8 — dynamic navigation is only pleasant if files have
interpretable names to read/grep by. Hash-named files are un-navigable; slug-named ones are.

**Decision (2026-06-13).** Interpretable filenames on the FS + a **DB binding `filepath ↔ content
hash`**. The filename carries meaning (`tsmc-cowos-capacity.md`); the content hash (uniqueness, dedup,
idempotency) lives in the **DB index** — *not* in the filename and *not* in frontmatter. Net vs the A+D
proposal above: same split (readable surface, hash as the durable fingerprint), but the hash lives in
the DB so dedup is an indexed `hash → filepath` lookup instead of a frontmatter scan — the right call
for dedup-at-scale (aligns with future cross-sibling MCP-fetch dedup).

Details to nail down:
- **Bidirectional & indexed:** `filepath → hash` (integrity) and `hash → filepath` (dedup: "have I
  already fetched this exact content?"). Both indexed.
- **Collision rule:** same content + same name → reuse the existing file; *different* content the model
  labels the same slug → disambiguate the *path* (the DB enforces filepath uniqueness). Open detail:
  disambiguator = counter (`-2`) or short hash. The hash distinguishes content; only a genuine
  name-clash needs a path discriminator.
- **Rebuildable from FS** (re-hash every file) so the binding stays a *derived index*, not a primary
  store — composes with #6: the index is **path-keyed** ("the connection is the path"), the hash is a
  column on that row.
- **Nudges the DB fork toward A.** The DB now indexes evidence (filepath↔hash) — a second index job
  beyond topology — so a thinned DB-as-index (A) is the implied direction over drop-the-DB (B). Confirm
  when the fork is settled.

---

### Concern 4/9 refinement (2026-06-21) — the metadata model: content in FS, metadata in DB, versioned references

Resolves the authorship-boundary critique (A1) and generalizes #9's `filepath ↔ hash` binding to the
whole FS.

**Authorship boundary (agreed).** Two write-paths, never blurred:
- **Evidence = runtime-authored.** Machine-generated from an actual tool call, tamper-evident; the model
  never hand-writes an evidence file. This is what keeps provenance *non-fakeable* — **citability ≠
  non-fakeability**: a resolving handle must also be a *real observation*, not prose the model wrote.
- **Notes / outputs = agent-authored.** Prose the agent writes.

**Content/metadata split (agreed — with one consequence to accept).** The FS holds **pure content** — no
`created_at`, no sha, no version numbers in the file. **All metadata lives in the DB**: timestamps,
content hash, version, and the *reference bindings* between files. (It's git's shape: FS = object store
of immutable content, DB = refs + history.)

*Consequence:* timestamps, version lineage, and the reference graph are **not reconstructable from
pure-content files** (you can re-hash content, but not recover *when* it was written, its version order,
or *which version* A cited). So the DB stops being a *derived* index and becomes **primary state for
provenance/versioning** — which **reverses the "FS is the single source of truth" lean** from #6/#7 and
**settles the DB fork at A**. New slogan: *"FS is the content store; the DB is the provenance/version
graph."* Coherent — just a conscious trade.

**Forces: the FS must be immutable + versioned.** If references pin to a version and the version lives
DB-side, "updating fileB" must mint a *new retained version*, never overwrite — else old references
dangle. Under the hood = content-addressed storage with the readable path as a DB-managed alias;
`put_if_absent` + content-addressing already give it. Content-addressing survives as the *mechanism*,
not the surface (consistent with #1/#9).

**Provenance lives in the DB, not the content (re-confirmed 2026-06-22).** The file is pure prose; the
citation bindings (which evidence an output rests on) live in the DB (`write_output`'s `citations`, #14a).
Auditing provenance is a DB/tooling overlay, **not** read from the raw file — the maintainer's accepted
trade for keeping content pure. *(An earlier draft proposed inline `[n]` markers / a references section in
the prose; superseded — no citations in files.)*

**One conflict to reconcile — where does authored *config* (cadence, model, tools) live?** #5 put it in
`mandate.md` frontmatter; "no metadata in FS" says DB. *Recommendation:* config is *input* (authored in
`graph.yaml`; a parent writes it on spawn), not derived metadata → it lives in the **DB**, and
`mandate.md` becomes **pure prose**. Frontmatter mostly dissolves; one structured home (DB), one prose
home (FS). **Decided (2026-06-21): config → DB; `mandate.md` is pure prose.** (Refines #5.)

> **Revised (2026-06-24) — config rides the Temporal durable input, not Postgres.** Implementing V4.1
> exposed that authored config already reaches the runtime via `Mandate → AgentInput →` Temporal's durable
> workflow history; the `agents.persistent`/`model`/`mandate_ref` columns were never read back by the
> runtime (vestigial dual-write). Config is bootstrap *input*, which is closer to the scheduling plane
> than to metadata/provenance, so it stays on the durable workflow input; **Postgres keeps topology +
> provenance/version graph + index only.** The "config → DB" wording above (and in the Target-shape DB-role
> bullet) is superseded by this: `mandate.md` stays pure prose, but config's structured home is the
> durable input, not a Postgres column/table. V4.1 therefore collapses the agent row to topology and drops
> the vestigial columns; C1 (`coral apply`) writes topology to Postgres + materializes the FS and builds
> the workflow inputs (carrying config) — it does **not** write config to Postgres.

**Stale shas — not a bug; the propagation mechanism (ties A1 ↔ A4).** A cites B@sha1; B → sha2:
- With version-pinned references over immutable content, **staleness is *correct*:** A's version that
  cited sha1 stays pinned to sha1 (a true record of what A concluded then); A's *current* version cites
  B's current version. Old→old, current→current — exactly what makes provenance time-scrubbable. The
  alternative (references to a mutable path that auto-follows to latest) silently changes the evidence
  under a claim → breaks "no claim without a trail." **Reject auto-follow** for evidence and cross-node
  citations.
- **The key move:** stale-reference *detection* IS the upstream-update wake. When B → sha2 the DB knows
  who pinned sha1 and fires "your dependency changed, re-evaluate" at them — exactly A4's child-update
  propagation. On an all-"never" graph the final result assembles precisely because each new child
  version makes its parents' references stale, waking them to re-reconcile and re-pin. **The reference
  graph = the dependency graph = the wake-propagation edges. A1 and A4 are the same mechanism.**
- We *use* staleness, not fix it. Real questions left: **retention/GC** (keep an old version until
  nothing live pins it) and **coalescing** (don't thrash a parent on every micro-update — see A4).

> **REVISED 2026-07-01 (maintainer-directed) — A1 and A4 are DECOUPLED for waking.** The version-pinning
> half above (staleness is *correct*; old→old / current→current; reject auto-follow) STANDS — it is the
> provenance contract. What is revised is the "**staleness *detection* IS the wake / reference graph =
> the wake-propagation edges**" half: **propagation is topology-driven, not reference-graph-driven.** A
> node wakes only its single direct parent (via the `ChildOutput` signal), which re-reconciles and re-pins;
> changes reach the root hop-by-hop up the parent→child tree. The reference graph is **provenance only**,
> never read to decide who to wake. Termination is then structural (single-parent tree ⇒ acyclic + bounded
> depth). "Coalescing" is free (a parent's per-tick trigger drain collapses bursts); "fan-out" does not
> arise (single `parent_handle` — a narrowing assumption). See the revised Concern 14b for the full
> rationale + the rejected reference-graph-reactor road-not-taken.

**Versioning mechanism (2026-06-21) — local `git` per agent FS path. Decided; all three qualms resolved; git surface scoped minimal.**
Idea: each agent root (`graphs/<gid>/agents/<aid>/`) is a git repo; git handles versioning; a builtin
tool retrieves a file at a given sha (`git show <sha>:<path>`). Strong fit:
- Reuses a battle-tested content-addressed versioned store (DEVELOPMENT.md: prefer established tools).
  Git's **blob sha *is* our content hash** (#1/#9) — we stop computing our own.
- **Reinforces A1, doesn't conflict it.** Content files stay pure prose; version metadata lives in
  `.git/` (clean separate place), not in the file. It *narrows* the DB: no version lineage in the DB
  (git has it) — the DB holds the **reference graph as git shas** + topology + a query index.
- Inspectability/forkability for ~free: `git log`/`show`/`diff` on an agent dir = VISION's "inspectable,
  forkable," and serves NVIDIA-run eval ("what changed cycle N→N+1" = `git diff`). **Stage 8
  (snapshot/fork) becomes near-trivial** — snapshot = a commit sha; fork = copy content into a new
  agent path + fresh `git init` (not a branch — see qualm 3).
- The **read-at-sha tool** is clean, well-scoped; serves pull-navigation (#8) + stale-sha resolution.

Three qualms (phase-appropriate, none fatal):
1. **Temporal determinism — resolved.** `git commit` isn't naturally idempotent (the commit sha embeds
   timestamp + author). **Decided: pin references to the *blob* sha (pure content), not the *commit*
   sha** — blob shas are content-deterministic, so references are retry-stable regardless of commit
   metadata. Commit-per-tick is itself retry-safe via git's clean-tree no-op (a retried commit on an
   unchanged tree is a no-op), and a crashed-then-retried write recovers the same blob sha from content.
   Commit metadata stays human-friendly; no need to pin commit dates.
2. **Object-storage collision (strategic).** Git needs a POSIX `.git/`; it does **not** run on
   S3/object storage — the production/scale target behind the `AgentStorage` trait (stage 2.5/9) — and
   millions of tiny `.git/` repos is an operational burden. So git is a **versioning *implementation*
   behind the `AgentStorage` trait (local/dev impl), not a kernel assumption.** The prod/object-store
   path later needs a content-addressed-object-store equivalent ("git's object model on S3").
3. **Concurrency — resolved by scoping.** Git's index/ref locking assumes ~single-writer. Per-agent
   that mostly holds (one workflow, sequential ticks), but cross-agent subtree reads (A6) while a child
   commits can see mid-commit state. **Decided — hard scoping rule: git's surface is exactly
   {commit-per-tick, read-blob-at-sha}** — no branch, checkout, merge, rebase, or history rewrite. The
   working tree always reflects HEAD (latest); historical reads go through `git show`/`cat-file
   <sha>:<path>`, never `git checkout`. Since only HEAD ever advances, most ref-contention is sidestepped.

Design points (set): commit granularity = **one commit per tick** (clean "commit = cycle" history,
atomic per unit-of-work); **fork = copy content into a new agent path + fresh `git init`** (one-way
divergence; *not* a git branch; provenance link to source recorded in the DB); working tree = HEAD,
historical reads via `git show`/`cat-file` (no checkout).

---

### Concern 10 — there is no non-persistent graph; persistence is universal

**Today.** `persistent` is a per-agent flag (default `false`). Non-persistent = one-shot ("emit Output →
retire"); persistent = refresh-forever (Retire demoted to Idle). The whole CM-* effort built persistence
as an *opt-in*.

**Decision (2026-06-13) — remove the flag; every node is persistent.** This makes the engine's default
the thing VISION already calls the default ("agents don't end… they idle, wake, run, idle"). One-shot
stops being a *mode* and becomes a *degenerate cadence*: a node whose re-wake is set to **"never"** runs
its one cycle on seed, idles, and re-wakes only if a trigger arrives (child upstream update, human /
external signal). No flag, no second lifecycle.

Cascade (what removing the flag takes with it — all good riddance):
- **`persistent` column / YAML field / frontmatter knob → gone.**
- **Prompt collapses to one lifecycle tail.** The one-shot "after Output, retire" invariants
  (`prompt.rs`) are deleted; every node gets "produce/refresh, then idle, you don't terminate yourself."
- **`demote_retire_if_persistent` becomes unconditional → remove `Decision::Retire` from the model's
  vocabulary entirely.** Termination is a *kernel/human* op only — `RetireChild` / a `retire` signal /
  graph teardown. (VISION: lifecycle belongs to the kernel, not a model decision.)
- **The CM-4 degenerate-config guard relaxes.** "persistent parent must have ≥1 persistent child" is
  obsolete: a parent with cadence "never" simply waits for child updates; it can't spin. The only
  residual wasteful case — a parent with a *self-cadence* whose children never update (re-reconciles
  stale inputs) — is wasteful, not invalid → relax hard error to a warning.

**Scale note / mild pushback to keep in view.** Universal persistence trades *automatic reclamation on
completion* for *uniform, always-resumable lifecycle*. A "one-off" node is now an **idle-forever
workflow**, not a terminated one; reclamation becomes an **explicit** op (retire-signal / teardown). At
millions-scale that's a visibility-store cost, not compute (a workflow parked on a signal with no timer
is cheap on CPU). The trade is right for "graphs as long-lived memory you *want* resumable" — just
naming that "idle" ≠ "reclaimed," and we'll eventually want a cheap representation for never-waking
parked agents.

---

### Concern 11 — remove max_ticks; tick = one output cycle; per-node re-wake

**Today.** `max_ticks` caps loop iterations; it's the de-facto runaway/cost/lifetime bound (and the
thing tests use to terminate). Concern 3 argued "demote it to a debug fuse." This decision goes further.

**Decision (2026-06-13) — remove `max_ticks` entirely.** Sharpens #8: **one tick = one cycle in which a
node produces (refreshes) an output.** The bounds become two orthogonal knobs:
- **Per-node cadence — governs *only* the self-scheduled idle wake (`ScheduledWake`)**: `30m`, `1h`, …,
  with a **"never"** sentinel = no self-wake at all. **All other triggers are always live and are *not*
  part of cadence** — child/upstream updates, human, external are the *dependency graph*, not the clock
  (A4). So an all-"never" graph still produces a final result: child updates propagate up the dependency
  edges as they're created (A1's staleness-detection *is* that propagation). Different nodes carry
  different cadences.
- **Budget** (#3) — denominated in $/tokens; now the **sole runaway guard** (no iteration cap remains).

`tick` survives only as an internal counter (continue-as-new thresholds, decision/cycle-log numbering)
and per #8 now *means* "cycle."

**What to relocate (the function `max_ticks` quietly served):**
- **Test/hermetic termination.** Live tests stop via `max_ticks: 8/4` today. Replace with (a) a tiny
  tripping budget, (b) drive N cycles then send a `retire` signal, or (c) a *harness-only* cycle cap
  that is explicitly **not** a production authoring field. Migrate `persistent_monitor_live` & friends.
- **Runaway protection** → budget (so budget must ship; see #3/#8 coupling).

**Open detail.** Does *every* tick emit an output, or can a wake conclude "nothing material changed →
emit nothing, re-idle"? Forcing an emit per wake makes a frequent monitor noisy ("still nothing"
×24/day). Lean: a tick is a cycle in which the node *may* refresh; "no change" is a valid no-emit
outcome (and then nothing ripples upward). Confirm — it affects upward re-reconciliation frequency and
cycle counting.

---

### Concern 12 — budget (placeholder; to define extensively later)

**Status: deferred — flagged now, designed later.** The lifecycle decisions made budget **load-bearing
for safety**: it's the ReAct-loop **termination backstop** (#8), the **sole runaway guard** after
max_ticks is gone (#11), and the **context bound** replacing `ContextPolicy` (#8). Yet it's the
least-specified primitive (was deferred CM-7). Not designing it now (don't do too much at once); recording
the questions a later, dedicated pass must answer:
- **Scope:** per agent / graph / tenant / cycle — likely several, nested.
- **Denomination:** $ vs tokens vs tool-calls (or all, normalized).
- **Enforcement point:** inside the ReAct inner loop (per-cycle), the scheduler (cadence admission), or both.
- **Exhaustion behavior:** pause, degrade (cheaper model), alert, retire, or hold-for-human.
- **Coupling:** it's the termination backstop for #8 and the only runaway guard after #11 — so it must
  exist before either ships.

---

### Concern 13 — the Filesystem harness as the Coral standard; code-execution as an opt-in power tool

**Decision (2026-06-22; amended same day).** The **Filesystem harness** — context-as-FS-resource +
progressive disclosure + FS-as-memory — is the **first-class standard** (ratifies #8). **Code-execution
is an opt-in *power tool* the agent reaches for** (fan-out, quantitative, multi-source aggregation) —
**not the mandatory medium of every cycle.**

> **Amendment (why).** The first cut made code the *sole* Act (everything dissolved into code). On
> stress-test that over-reached: it imported the coding-agent paradigm — where code is the native work
> product, the operator is a programmer, it's single-agent, and a human supervises each session — into
> Coral, where *none* of those hold. Mandatory code hurt reasoning-legibility for **domain-expert**
> operators, inverted cost on the common **light** cycle (sandbox-spin just to conclude "no change"),
> worsened the failure space for **unsupervised** long-lived agents, and is the hardest thing to clear
> with the sovereign/high-trust buyers VISION names. Fix: **default Act = reason over pulled files +
> call tools directly + write prose + reconcile (structured); code is one *invokable* action for heavy
> work.** (VISION §5 frames sandboxed execution as a capability an agent *reaches for*, not the sole
> medium — this realigns to that.)

**Second amendment (2026-06-22) — simplify for now: defer code-execution entirely.** Near-term the Act is
**direct tool calls with results in-context** — no sandbox, no MCP-code bridge. The FS-harness *context*
half stays in full (pull-navigation #8, FS-as-memory, markdown, git). Code-execution + the sandbox/bridge
subsystem (Issues 2/4/5 below) move to **deferred / future direction** — the maintainer is drawn to a
fuller code-first FS-harness design later, and it slots back in cleanly as the power-tool evolution when a
real fan-out/quant need shows up. The Issue 1–7 analysis below is retained as the **future design
record**; the *decided-now* Act is direct-tool-call.

Resolutions, issue by issue (2/4/5 are now future; 1/3/6/7 + "Why Temporal stays" apply now):

**Issue 1 — structured kernel surface + a research *repertoire* (code is one option in it).** The Act is
no longer "all code." Each cycle the agent works from a repertoire:
- **Reason over pulled files** (own FS + descendant subtree, read-only) — the default.
- **Call tools directly** (common case: one/few MCP calls; the runtime captures evidence as today).
- **Write `notes/` / `outputs/`** (prose).
- **Reconcile children** — read the subtree → synthesize → optionally record a conflict. **Stays a
  structured, auditable action** (not dissolved into ad-hoc code — it's the engine's epistemic core,
  VISION-mandated reviewable + human-overridable).
- **Run code** — an *invokable* action for fan-out / quantitative / multi-source aggregation; raw data
  stays out of context (results → FS).
- **Kernel/topology decisions stay structured + explicit:** `SpawnChild` (starts a child workflow),
  set-cadence/`Idle`, `RetireChild`. (Retire-self gone, #10.)

A cycle = wake → research from the repertoire (reasoning + direct tool calls + *optionally* a code run) →
emit output(s) or conclude no-change → return a kernel control outcome. **Code is a power tool in the
box, not the box.**

**Issue 2 — provenance bridge (the correctness must-have).** Evidence stays runtime-authored:
- The MCP bridge the sandbox exposes **intercepts every tool call** `(tool, args, result)` and writes the
  evidence file itself, out-of-band from the model's code.
- The tool API returns `(result, evidence_ref)` so the model's code cites `evidence_ref` in output
  footnotes.
- **Sandbox FS permissions:** model code has **read+write** on `notes/` and `outputs/`, and **read-only**
  on `evidence/` (only the runtime writes evidence). Read access lets the agent re-read and cite what it
  gathered; the write-lock keeps provenance non-fakeable.
- **Two provenance paths, one invariant.** *Direct* tool calls keep today's clean runtime-captured-
  evidence path; *code-invoked* tool calls go through the MCP-code bridge above. Both enforce *model
  never writes `evidence/`*. The bridge only matters on code cycles; the common direct-call path is
  unchanged.

**Issue 3 — durability granularity (resolved 2026-06-22).** Durability splits across **three homes by
type**, and the sandbox holds nothing durable:
- **Scheduling / lifecycle → Temporal** (sleep/wake/signal/topology survive crashes).
- **Work + provenance → FS (git) + DB** (content, versions, reference graph).
- **The sandbox is ephemeral + re-runnable** — it can die anytime; only in-flight compute is lost.

Code-execution = **one Temporal activity**. Resumability falls out: a crashed cycle just **re-runs the
code**, and the **content-addressed tool cache** (`hash → filepath` dedup index) makes the re-run re-hit
cached tool results instead of re-calling externally (no duplicate fetches / double-spend); git holds
any partial committed state. So the *harness* provides resumability for the expensive/external part (tool
calls), and the model's pure compute re-derives cheaply from cached inputs — **the model code does not
have to be written to be resumable.**

**Per-tool durability survives for the common case.** Because code is now opt-in (Issue 1), *direct* tool
calls keep their own fine-grained Temporal activity boundaries (the staged-execution principle —
partial-batch survival). Only a **code run** is the coarse "one activity" with FS + cache resumability.
So fine-grained durability stays where it's cheap (direct calls); coarse durability applies only where
code earns it (fan-out).

**Why Temporal stays — the layer reframe (2026-06-22).** "Is Temporal still right now that *code* takes
every action?" Yes — but its role shrinks to its *original* justification. **Temporal vs a sandbox
(Daytona/E2B) is a category error:** different layers. Temporal = durable orchestration/scheduling/
messaging for long-lived, mostly-idle, signal-driven agents; a sandbox = the ephemeral environment the
Act runs in. We use **both, stacked** — Temporal hosts the thin loop and fires a code-execution activity
that runs in the sandbox.
- **What Temporal keeps (undiminished — its original reason):** durable timers (sleep an hour or a month,
  wake reliably), durable signal inbox (child outputs / human overrides / staleness wakes delivered to a
  *sleeping* agent), child-workflow topology, and — decisive — **near-zero idle cost** (a parked workflow
  is DB rows + zero compute).
- **What it sheds:** fine-grained per-action durability — a *bonus* of the old engine-takes-every-action
  model, never the reason. Shedding it makes the workflow body *thinner* (timer + signals + one activity
  + continue-as-new), less determinism surface — a feature.
- **Why the sandbox can't be the host:** idle economics (VISION §7). Millions of parked workflows ≈ free;
  millions of *persistent* sandboxes = millions of idle containers = bankrupt. Idle must live in the
  cheap durable layer; the sandbox is spun up only when a cycle fires, then torn down.
- **The real alternative axis (watch-item, not now):** if Temporal's operational weight bites, the
  comparison is *other durable-execution engines* — **DBOS** (Postgres-native, which we already run),
  Restate, Inngest — **not** a sandbox. Lean: keep Temporal (built; scale-proven for millions of
  long-running workflows); revisit only if ops weight hurts.

**Issue 4 — the sandbox subsystem (the heaviest new build).** Language: Python (the de-facto for this
pattern). Isolation: subprocess-with-limits for trusted dev / the NVIDIA run; hardened
(container/microVM) is a production axis (Issue 7). Core surfaces: the **MCP-tools-as-code-API bridge**
(Issue 2) and the **FS mount** (own repo rw except `evidence/` read-only; descendant subtree read-only,
A6). It's a **pluggable layer behind the code-execution activity** — clean contract: *run this cycle's
code with this FS mount + MCP bridge + budget* — local subprocess for dev, Daytona/E2B/Modal for prod,
the same swappable philosophy as `AgentStorage`. **Ephemeral and on-demand** (spun up only when the
agent invokes a code action — *not* every cycle, which is what removes the light-cycle cost-inversion;
runs, torn down; never persistent, for idle cost); Temporal *calls into* it and owns scheduling/idle,
never the lifecycle.
VISION §5 lists this execution layer; the staged plan deferred it — first-classing code-execution pulls
it onto the path. Single biggest build in the realignment.

**Issue 5 — budget at the sandbox boundary.** Per-cycle budget (#12) is enforced *in the bridge/sandbox*:
tool-call count/cost, wall-clock/CPU, tokens. So #12 is **co-designed with the sandbox**, not
independently deferred.

**Issue 6 — audit artifacts, not code.** Arbitrary model-code isn't bit-reproducible; we audit the
git-versioned outputs/evidence + the reference graph, not by re-running code.

**Issue 7 — security & cost-at-scale (deferred).** Trusted-dev sandbox now; hardened isolation + sandbox
pooling/reuse (startup cost matters at millions-of-agents scale) are later.

**Reshapes / adds (now mostly additive).** With code demoted to opt-in, #13 *adds* rather than replaces:
it adds code-execution as an invokable capability + the sandbox/bridge subsystem (used on heavy cycles
only). Still **removes** `ContextBundle`/`ContextPolicy` (pull-navigation, #8, stands). **Keeps** (contra
the first cut) per-tool `CallTools` dispatch + the `execute_tool` activity (the default direct-call path)
and the structured reconcile/conflict machinery. Reshapes #8 only in that *when code is invoked* it does
the reads + tool calls in one run; otherwise reads and tool calls are direct loop actions. New weight:
the sandbox + bridge.

---

### Concern 14 — provenance & consistency mechanics (post-simplification critique, 2026-06-22)

Three load-bearing mechanics that were one-liners in the decided design. They interlock.

**14a — `write_output` carries a structured citation map (restores mechanical provenance). Decided.** The
prose move weakened provenance from *enforced* (`EmitOutput { content, evidence: Vec<EvidenceId> }` —
persist rejected unresolvable evidence) to *checkable-at-best*. Fix: the `write_output` action takes
**`{ body: pure prose → FS, citations: [evidence_ref] → DB }`** — the body (content) goes to the FS; the
`citations` set goes to the **DB** (the reference graph), **never into the file** (A1: content-only in FS,
provenance in DB). The runtime **rejects** the output unless every `evidence_ref` resolves to a real
evidence file — output-level granularity, exactly the old `EmitOutput { content, evidence }` guarantee,
now with a markdown body. Provenance is overlaid from the DB by tooling, not authored into the content.
*(Span-level "which sentence cites which evidence" would need anchors stored DB-side; defer.)*

**14b — REVISED 2026-07-01 (maintainer-directed): propagation is TOPOLOGY-DRIVEN, not a reference-graph
reactor. The reactive-subsystem framing below is descoped; retained as the road-not-taken.** Propagation
follows the **parent→child spawn tree, one hop up**: a node's `write_output` wakes its single direct parent
(the existing best-effort `ChildOutput` Temporal signal — already wired end-to-end: the woken parent's seed
renders the child-output trigger with an explicit "fold it via `reconcile_children`" instruction, so the
decision layer can act on it); the parent re-reconciles, and if its own output changes it wakes *its*
parent, so a change ripples to the root hop by hop. This is exactly the "child→parent output propagation"
mechanism the Invariants section already commits to — not a new subsystem. Consequences:
- **The reference graph (citations) is provenance ONLY**, never consulted to decide who to wake (A1↔A4
  decoupled — see the "Stale shas" note above). Citations mirror topology anyway (a node cites its *direct*
  children via reconcile), so the reference-graph reactor would solve a long-range-citation problem that
  does not arise.
- **Termination is structural**: single-parent tree ⇒ acyclic; one-hop-up ⇒ bounded depth ⇒ a cascade
  always ends at the root. No convergence/staleness-re-derivation machinery needed.
- **No fan-out** — single `parent_handle` today. **NARROWING ASSUMPTION: if multi-parent (a node feeding
  several parents) is ever added, fan-out + coalescing return and this decision reopens.**
- **Coalescing is free** — a parent's `pending_triggers` drain-per-tick collapses a burst of child updates
  into one reconcile pass.
- **Reliability:** the signal is a durable Temporal command; a missed wake self-heals on the node's next
  write — EXCEPT an all-"never" node that writes once, where the single signal+reconcile wave is the only
  shot (a dropped signal or a non-reconciling wake strands that subtree until a human/trigger re-pokes).
- **Residual V6 work = VERIFY, not build:** whether a multi-level all-"never" graph assembles its root
  end-to-end (with a real model choosing to reconcile at each level) is UNVERIFIED — the residual is that
  test (fix if broken; add a safety net only if a real gap shows), not a reactor.

**ORIGINAL 14b (road-not-taken, retained):** the staleness→wake propagation is a real subsystem, not a
one-liner. A1↔A4's "the DB wakes the dependents when a cited version goes stale" is a **reactive
notification system over the reference graph**, and the continuous-monitor behavior depends on it. Shape:
on each write (a path gets a new blob sha), the runtime updates the reference index, finds dependents that
pinned the *previous* version, and fires a "dependency changed" wake (Temporal signal) at each. Open design:
- **Where it runs** — write-path enqueue (the write activity queues wakes, for immediacy) vs. a periodic
  reconciler sweep vs. Postgres `LISTEN/NOTIFY`. *Lean: write-path enqueue + sweep as backstop.*
- **Coalescing** — debounce so a hot child doesn't thrash its parents (A4).
- **Fan-out scale** — a child with many parents = a wake fan-out; bound it.
- *(Rejected because: on a single-parent tree, topology already carries propagation one-hop-up; the
  reference graph reduces to a provenance record. The reactor's precision only pays off with long-range or
  multi-parent citations, neither of which the current topology produces.)*

**14c — FS↔DB dual-write consistency. Resolved (2026-06-22).** Every write touches two stores: commit a
blob to git *and* update the DB. Crash between → *git-ok/DB-not* = orphan blob (recoverable index);
*DB-ok/git-not* = dangling DB pointer (corruption). Resolution:
- **One Temporal activity, both halves idempotent** — git commit (clean-tree no-op on retry) + DB upsert
  keyed by `(filepath, blobsha)`. A retried write converges; **Temporal's retry *is* the consistency
  mechanism** (it completes the second half), regardless of store roles.
- **FS-first, DB-second** as the secondary preference — an interrupted, un-retried write leaves a
  recoverable orphan rather than a dangling pointer.
- **Reconciliation sweep** repairs only the *FS-derivable* part — `filepath ↔ blobsha` orphans. It does
  **not** rebuild the reference graph (below).

*A1 stands — softening considered and rejected by the maintainer (2026-06-22).* The reference graph +
provenance metadata are **DB-primary**: content stays pure in the FS; citations / lineage / timestamps
live in the DB. So the DB is **not fully rebuildable from the FS** (the sweep recovers the index, not the
reference graph). **Consequence (accepted):** the DB's own durability is load-bearing — protect it with
standard Postgres durability (WAL / backups / replication). Provenance does not survive total DB loss the
way FS-resident provenance would; standard DB durability is the mitigation. *(The "put citations in
content to make the DB rebuildable" move is rejected — it would put provenance in files, violating A1.)*

---

## Synthesizing moves (the whole set so far reduces to six)

1. **Pure-content markdown FS, git-versioned** — #1/#4/#5/#9 + A1. mandate.md (pure prose), notes /
   outputs / evidence / conflicts as `*.md`. *No frontmatter / no metadata in the files* (config → DB).
   Interpretable slug filenames; content hash = git blob sha; the DB binds `filepath ↔ blob sha`.
   Authorship boundary: evidence runtime-authored, notes/outputs agent-authored.
2. **Provider/model registry** — #2. Qualified `provider/model` names, prefix→client map, all
   providers available, per-agent free choice.
3. **Universal persistence; per-node cadence + budget as the bounds; max_ticks removed** —
   #3/#10/#11. No `persistent` flag (every node persistent); one tick = one output cycle; each node
   configures its re-wake cadence (with a **"never"** sentinel = wake only on triggers); a $/token
   **budget is the sole runaway guard**; model-`Retire` leaves the vocabulary (termination =
   signal/teardown). `tick` stays an internal counter that now means "cycle."
4. **FS = content store; DB = provenance/version graph + topology + config + index (fork settled at A)**
   — #6/#7 + A1. Drop `mandate_ref`/`persistent` from the schema; the DB holds topology (path-keyed),
   config (cadence/model/tools), the reference graph (citations as version-pinned blob shas), and the
   `filepath ↔ blob sha` index. Versioning is git (local impl behind the storage trait).
5. **Filesystem harness — pull-navigation + direct-tool-call Act (the standard)** — #8/#13. Drop
   `ContextPolicy`; thin orienting seed; a **tick = one cycle** (multi-step inner loop
   decide→act→observe→terminal) in which the agent reasons over pulled files, **calls tools directly
   (results in-context)**, writes notes/outputs, reconciles children (structured) → ends with a kernel
   decision (cadence / spawn / retire-child). `notes/` is the primary continuity mechanism; an interim
   step-cap is the termination backstop until budget (#12). Dissolves `ContextBundle`/`ContextPolicy`;
   **keeps** direct `CallTools` dispatch + structured reconcile. *(Code-execution = deferred future
   direction — move 6.)*
6. **Execution sandbox + MCP-code bridge — DEFERRED (future direction)** — #13. *When it lands:* Python
   sandbox (subprocess-now, hardened-later); MCP-tools-as-code-API bridge owning the tool→evidence write
   path; FS mount = own repo rw except `evidence/` (read-only) + descendant subtree read-only; pluggable
   (local dev / Daytona/E2B prod), ephemeral + on-demand. The heaviest build — deferred until a real
   fan-out/quant need shows up. The maintainer is drawn to a fuller code-first design eventually.

## Guardrails — load-bearing functions NOT to drop while lightening

- **Idempotent retry keys** on the Temporal path (deterministic — just make them readable).
- **Content fingerprint** kept as a *derived index* (per #9's decision: a DB `filepath ↔ hash`
  binding), not as the filename or the human surface — honest provenance/dedup primitive, rebuildable
  from the FS.
- **Evidence citability** — every output traces to resolvable evidence handles.
- **The DB's three *real* residual jobs** — create-time uniqueness/race-safety, exactly-once child
  allocation, and enumeration/discovery. If the DB is thinned or dropped, these must land somewhere
  explicit, not vanish. `AgentStorage::put_if_absent` (already exists, returns `PutOutcome`) covers the
  first two for single-file writes; enumeration becomes a prefix-`list` or a rebuilt index.
- **Warm start + bound (when dropping the context bundle)** — a freshly-woken agent must not wake blind
  (keep a thin orienting seed: mandate + index + waking triggers), and must not be free to re-read its
  whole history every tick (keep a budget). The "what to load" judgment moves to the agent; the "don't
  load everything" constraint stays — now economic, not a static window.
- **Test/debug termination (when removing `max_ticks`, #11)** — tests stop via `max_ticks` today.
  Replace with a tiny tripping budget, an explicit `retire` signal after N cycles, or a *harness-only*
  cycle cap (never a production authoring field). Migrate `persistent_monitor_live` & co.
- **Reclamation is explicit (universal persistence, #10)** — a "one-off" node is an *idle-forever*
  workflow, not a terminated one; only a retire-signal/teardown reclaims it. "Idle" ≠ "reclaimed." At
  scale, want a cheap representation for never-waking parked workflows.

---

## Open / not yet raised

- **The DB-survival fork — SETTLED at A (by A1; re-confirmed 2026-06-22).** #14c briefly re-opened it
  (could citations-in-content make the DB FS-rebuildable?) — **rejected by the maintainer: content stays
  pure, provenance is DB-primary.** So the DB holds irreplaceable provenance state, protected by standard
  Postgres durability (not FS-rebuild). The A/B analysis below is the original rationale:
  - **A — DB as thin, path-keyed index.** Postgres stays but holds only topology + uniqueness
    (`{ path, parent }`), rebuildable from the FS; FS is source of truth. Smallest delta from today;
    keeps create-time race-safety and future cross-cutting query scale. Matches the maintainer's stated
    "DB keeps only the path; everything else → FS."
  - **B — no DB; the FS *is* the graph.** Topology = parent-pointer in child frontmatter (collapses the
    two-row agent+edge write into one atomic file create); discovery = prefix-`list`; exactly-once =
    `put_if_absent`. Maximally "agents are filesystem paths." Loses relational constraints + fast
    cross-cutting queries; leans on the object-store consistency model.
  - *Why B is more feasible than it first looks:* the runtime already doesn't read the DB (topology
    lives in Temporal `child_handles` carryover — walkthrough §8), and `put_if_absent` already gives
    atomic exactly-once. *Why B has real cost:* at millions-of-agents scale, cross-cutting queries
    ("all agents with property X") become FS scans; relational integrity becomes convention + storage
    primitives. **Lean (updated 2026-06-21 by A1):** A1's content/metadata split puts
    provenance/version/timestamps in the DB as *primary* state (not derivable from pure-content FS) —
    which **settles the fork at A**: the DB is essential, and "FS is the single source of truth" is
    replaced by "FS is the content store; the DB is the provenance/version graph."
- **Execution order — deferred (A7).** Not sequencing now; goal is all thoughts on the table first, and
  the NVIDIA run informs priority later. *(Prior tiering kept for reference below.)*
- **Tiering (reference only).** By depth: **format** (1, 4-fmt, 5, 9) — markdown+frontmatter FS, interpretable
  ids; **structural** (6, 7, the DB fork) — DB reshape; **behavioral** (8, 10, 11) — pull-navigation + the
  unified persistent/cadence lifecycle (no flag, no max_ticks). None *block* the NVIDIA run (today's DB +
  bundle + max_ticks serve it fine), but moves 1
  and 9 most directly serve run-*evaluation* (legible files to read) and are the cheapest, so they go
  first. #8 (ReAct inner loop, decided) is the highest-leverage behavioral change but the biggest
  build, and it's now **coupled to #3** (the per-cycle budget is its termination backstop) — so #3 gets
  pulled forward from "can wait" to "ships with #8." #2 can wait; the DB fork (4) waits until the run
  informs the scale story (though #9's decision already nudges it toward A).
- **Open micro-details from decided concerns** (small, deferred): #9 — disambiguator on a genuine
  name-clash (counter `-2` vs short hash); #11 — does every tick emit, or is "no material change →
  no-emit" valid (lean: valid); #10/#11 — which test-termination replacement (tiny budget vs
  retire-after-N vs harness-only cap); #2/#3 — qualified model-name format + budget
  denomination/granularity; **A1** — version retention/GC policy; **A4** — coalescing of upstream-update
  wakes (don't thrash a parent); **git** — production/object-store versioning impl (git is the local/dev
  impl behind the storage trait), and the `AgentStorage`/versioning *trait shape* that must satisfy both
  the git-local and the object-store impls (the real long-term commitment; git is just impl #1);
  **#13** — *(Issue 3 durability now resolved: three-home split + tool-cache resumability)*; remaining
  opens: sandbox language/isolation choice, the MCP-code bridge contract, and the watch-item
  Temporal-vs-lighter-durable-engine (DBOS) if ops weight bites; **#14b — RESOLVED 2026-07-01:
  propagation is topology-driven (child→parent, one hop up the tree, via the existing `ChildOutput`
  signal); the reference-graph reactor (enqueue/sweep/coalescing/fan-out) is descoped; the reference graph
  is provenance-only. Residual = VERIFY multi-level all-"never" end-to-end assembly (a test), not build a
  reactor.** *(#14c consistency resolved; A1 re-confirmed — DB primary, content pure, protected by
  Postgres durability.)*
- **Consciously deferred to later design rounds** (maintainer's call, 2026-06-22): **code-execution +
  sandbox + MCP-code bridge** (#13 Issues 2/4/5) — near-term Act is direct tool calls in-context; the
  maintainer is drawn to a fuller code-first FS-harness later; **human-in-the-kernel** surface — later;
  **budget** (#12, with the interim step-cap as scaffolding until then) — later; **what makes reconcile
  "structured"** — later; **forking / whole-graph snapshot** — later; **migration** — N/A (no users
  yet). *(Cycle control loop, gap 4 — now resolved; see Target shape.)*
- More concerns coming from the maintainer (this list is incomplete by design).
