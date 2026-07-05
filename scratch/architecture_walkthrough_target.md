# Coral Engine — Architecture Walkthrough (TARGET, post-realignment)

> The **target** architecture once `scratch/design_realignment.md` is applied. Same shape and reading
> order as `scratch/architecture_walkthrough.md` (the as-built snapshot) so the two can be read side by
> side.
>
> **Status: design target, NOT yet implemented.** Nothing here is shipped — it's the composed end-state
> of the decided concerns in `design_realignment.md` (its **Target shape** section is authoritative; this
> doc renders that as a narrative). References are to **concern numbers** (`#N`, `A1`, …) in
> `design_realignment.md`, not code lines, because the code will change. Where this diverges from the
> as-built engine, a *→ from today* note says so.
>
> Deferred-for-later (so this doc doesn't over-promise): **code-execution + sandbox** (#13 future),
> **budget** (#12), **human-in-the-kernel**, the **reconcile-as-structured-primitive** definition,
> **forking / whole-graph snapshot**, and the **propagation subsystem's** detailed shape (#14b). See §9.

---

## 0. The one-sentence model

> **A graph of long-lived agents, each one a git-versioned filesystem path running a
> wake → pull → reason → act → idle inner loop, whose state lives in two planes — pure content on a
> per-agent git filesystem, and all metadata/provenance/topology/config in Postgres — scheduled durably
> by Temporal, and whose outputs flow child → parent by *staleness propagation* until the root holds a
> continuously-current, fully-sourced view.**

*→ from today:* the as-built model put working memory + provenance together on the FS as content-addressed
JSON. The target **splits content from metadata**: the FS holds *pure markdown content*; the DB holds
*everything about it* (provenance, versions, references, topology, config). "State as files" becomes
"**content** as files; metadata in the DB."

---

## 1. What changes from the as-built architecture (the diff)

| Dimension | As-built (today) | Target (post-realignment) | Concern |
|---|---|---|---|
| FS format | `<sha256>.json` blobs; mandate.json | **pure-markdown, slug-named** files; `mandate.md` is prose | #1/#4/#5/#9 |
| Versioning | content-addressed files, no history | **git per agent path**; blob sha = content hash; commit/tick | A1, git |
| Metadata (created_at, sha, version, citations) | in the FS files | **all in the DB**; files carry none | A1 |
| DB role | thin topology (graphs/agents/edges/tools/agent_tools) | **metadata + provenance/version graph + topology + config + index** (primary) | #6/#7/A1 |
| Config (cadence, model, tools) | DB columns + YAML + some duplicated on FS | **DB only**, authored once via `graph.yaml`; `mandate.md` pure prose | #5/#6/A1 |
| Model selection | one process-global vendor (feature flag/env) | **registry keyed by `provider/model`**; per-agent free choice | #2 |
| Lifecycle | `persistent` flag (default one-shot → retire) | **universal persistence**; no flag; no model self-`Retire` | #10 |
| Bound | `max_ticks` (triple-duty) | **per-node cadence + budget**; `max_ticks` removed | #3/#11 |
| Re-wake | `idle_period` | **cadence** governs self-`ScheduledWake`; "**never**" sentinel; all other triggers always live | #11/A4 |
| Tick | one decide→dispatch iteration | **one unit of mandate work** = one multi-step inner-loop cycle | #8/#11 |
| Context | runtime-assembled `ContextBundle` (push) sized by `ContextPolicy` | **pull-navigation**: thin seed + the agent reads its own FS on demand | #8 |
| Act | structured `Decision` enum (9 variants) | **research repertoire** in an inner loop; thin structured kernel surface | #8/#13 |
| Provenance | `EmitOutput { content, evidence }`, sha-named cite | `write_output { body→FS, citations→DB }`; output-level; DB-resolved | #14a |
| Child→parent | `ChildOutput` signal + synthetic-evidence reconcile | **staleness propagation**: new blob sha → stale refs → wake dependents | #14b/A1↔A4 |
| Temporal | orchestrates per-tick + per-tool activities | **thinned**: timers/signals/child-wf/CAN + per-tool boundaries for direct calls | #13 |
| (deferred) code execution | — | a future opt-in power tool (sandbox + MCP-code bridge) | #13 |

---

## 2. The primitives (the nouns)

Four families now, one per plane plus the runtime.

### A. Content primitives — *pure content*, on the per-agent git filesystem (#1/#4/#5/#9, A1, git)

An agent **is** a filesystem path: `graphs/<gid>/agents/<aid>/`, a **git repo** that is the agent's whole
durable self.

```
graphs/<gid>/agents/<aid>/      ← a git repo (.git/ used for versioning only)
  mandate.md         pure prose — the standing instruction. No frontmatter, no metadata.
  notes/*.md         agent-authored working memory (the primary continuity mechanism)
  outputs/*.md       agent-authored deliverables — pure prose (citations live in the DB, not the file)
  evidence/*.md      runtime-authored tool observations (model has READ-ONLY access; only runtime writes)
  conflicts/*.md     reconciled disagreements
```

- Every file is **pure content** — no `created_at` / sha / version / frontmatter *in the file*.
- Filenames are **interpretable slugs** (`tsmc-cowos-capacity.md`), never hashes.
- **Authorship boundary** (the provenance keystone): `evidence/` is runtime-authored and tamper-evident
  — the model never writes it (read-only); `notes/`+`outputs/` are agent-authored prose.

*→ from today:* replaces `outputs/<sha>.json`, `evidence/<sha>.json`, `mandate.json`, `health.json`,
`decisions/<tick>.jsonl`. The sha moves into git (blob sha) and the DB index; the metadata moves into the
DB; health/decision logs become DB/observability concerns (deferred surface).

### B. Metadata & provenance primitives — *everything about the content*, in Postgres (#6/#7, A1, #14)

The DB is **primary** (the survival fork is settled at A): it holds what pure-content files can't carry.

- **Topology** — which agents exist; parent→child edges; path-keyed.
- **Config** — cadence, `provider/model`, tool-assignment. Authored once via `graph.yaml`.
- **Reference graph** — every citation as a `(citing file, cited path, pinned blob sha)` binding: the
  provenance edges, version-pinned. *This is the provenance graph.*
- **Index** — `filepath ↔ blob sha` (bidirectional: integrity + dedup); enumeration; exactly-once
  allocation; version lineage; timestamps.

Protected by **standard Postgres durability** (WAL / backups / replication) — load-bearing, because
provenance lives only here and is **not** fully rebuildable from the FS (#14c).

*→ from today:* the as-built DB was a thin topology store with `mandate_ref` and duplicated
`persistent`/`model` columns. The target DB *grows* (it owns provenance/versions/references) while the
agent row *shrinks* to topology; `mandate_ref` and the duplicated columns disappear.

### C. Runtime primitives — *behavior*, the cycle loop + action repertoire (#8/#13, gap 4)

The old 9-variant `Decision` enum collapses into **a research repertoire inside an inner loop** + a thin
**structured kernel surface**:

- **Research actions** (per cycle, near-term): `read`/`list`/`search` (own FS + descendant subtree,
  read-only), **call MCP tools directly** (results land in-context), `write_note`, `write_output`,
  **reconcile children**.
- **Terminal / topology actions** (structured kernel decisions): `set_cadence`/`idle` (ends the cycle →
  sleep), `spawn_child`, `retire_child`. *No model self-`Retire`* (#10).
- **(Deferred) `run_code`** — a future opt-in power tool for fan-out/quant work (§9).

*→ from today:* `CallTools`/`EmitOutput`/`RewriteFs` become repertoire actions; `Retire` leaves the
model's vocabulary; `Idle` becomes the cycle's terminal control outcome; spawn/reconcile/retire-child
stay structured.

### D. Durability primitives — *scheduling/control state*, in Temporal (#13)

- **`AgentWorkflow`** — one long-running workflow per agent; a now-*thin* loop.
- **Signals** — the inbox: external triggers, child-update wakes, human ops (deferred surface), retire.
- **Durable timers** — the cadence wake.
- **Carryover / continue-as-new** — survive history limits; fire at cycle boundaries.

---

## 3. The three storage planes (the key idea, re-cut)

| Plane | Backend | Owns | Written by | Read by |
|---|---|---|---|---|
| **Content** | per-agent **git** FS | mandate (prose), notes, outputs, evidence, conflicts | the agent (notes/outputs); the runtime (evidence) | the agent (pull-navigation), inspectors |
| **Metadata / provenance** | **Postgres** | topology, config, reference graph, index, versions, timestamps | `coral apply` (once); the write-path on every cycle | the runtime; the propagation subsystem; tooling |
| **Scheduling / control** | **Temporal** | timers, signals, child-workflow handles, carryover | the SDK | the SDK on replay |

Pithy version: **the filesystem knows the *content*; the DB knows *everything about it*; Temporal knows
the *clock and the cursor*.** Content and metadata are deliberately split — the file is what a human
reads; the DB is what the kernel reasons over.

*→ from today:* the as-built planes were "Postgres = shape, FS = work, Temporal = clock." The target
keeps three planes but **moves provenance + versions + references from the FS into the DB**, and adds
**git** as the content plane's versioning substrate.

---

## 4. The lifecycle of one agent — the cycle control loop (the centerpiece; #8/#13, gap 4)

A "tick" is no longer one decision — it is **one unit of mandate work**: a multi-step inner loop that
runs from wake to idle.

```
on wake (cadence timer fires, or a trigger arrives):
  seed = mandate + notes/outputs INDEX (pointers, not contents) + the triggers that woke me   # thin, once
  conversation = [seed]
  loop:
    action = decide(conversation)              # one LLM call = one Temporal activity
    match action:
      terminal  (set_cadence / idle)          → record kernel decision; BREAK   # cycle ends → sleep
      topology  (spawn_child / retire_child)   → workflow executes it; append result; continue
      research  (read / list / search /
                 <mcp tool> / write_note /
                 write_output / reconcile)     → observation = execute(action)   # one activity each
                                                 append observation; continue
    if interim step-cap hit                    → force terminal (idle)           # scaffolding until budget
  → idle until next wake
```

Load-bearing properties:

- **(A) In-cycle context is an *ephemeral conversation*; cross-cycle continuity is the *filesystem*.**
  The conversation grows within a cycle and is **discarded** at cycle end; the next cycle starts fresh
  from a new seed + whatever the agent wrote to `notes/`+`outputs/`. *This is why `notes/` is the primary
  continuity mechanism.*
- **(B) Pull, not push.** No `ContextBundle`/`ContextPolicy`. The agent reads what its intelligence says
  it needs (own FS + descendant subtree, read-only); a slug-named markdown FS is what makes this navigable.
- **(C) Failures are inner-loop observations**, not cross-tick corrections — a tool error comes back as an
  observation and the model adapts on the next step. (The as-built `staged_correction` machinery
  dissolves.)
- **(D) Termination** = the model calls a terminal action, or an **interim step-cap** trips. *The step-cap
  is temporary scaffolding until budget (#12) lands — not the end state.*
- **(E) Continue-as-new fires at cycle boundaries**; a single cycle's history is bounded by the step-cap.

*→ from today:* the as-built loop was one `decide → dispatch` per tick with a runtime-assembled
`ContextBundle`. The target is a multi-step ReAct-shaped inner loop with the agent pulling its own
context — same outer durability shape (wait → work → continue-as-new), different *inside*.

---

## 5. How Temporal fits — the thinned role (#13)

Temporal contributes the same five mappings as today, but the workflow body is now **thin** (it
orchestrates the inner loop and little else):

| Agent concept | Temporal mechanism |
|---|---|
| "an agent" | a workflow execution, id `graphs/<gid>/agents/<aid>` |
| "wake on signal or timer" | `select!` over signal-arrival vs. the cadence timer |
| "an inbox" | signals — external triggers, **child-update wakes (#14b)**, human ops (deferred), retire |
| "do a side effect" | activities — each inner-loop action (decide, a tool call, a write) is one activity |
| "spawn a child" | a child workflow (`ParentClosePolicy::Abandon`) |
| "live forever" | continue-as-new at cycle boundaries, carrying a small carryover |

What Temporal **keeps** (its original justification, undiminished): durable timers (sleep an hour or a
month), durable signal delivery to a *sleeping* agent, child-workflow topology, and — decisive —
**near-zero idle cost** (a parked workflow is DB rows + zero compute; this is why idle lives in Temporal,
not in a persistent sandbox). What it **sheds**: nothing essential — direct tool calls keep their own
per-tool activity boundaries; there is simply no per-cycle code-execution activity yet.

*Watch-item (not now):* the real alternative axis, if Temporal's ops weight bites, is a lighter
durable-execution engine (**DBOS** — Postgres-native, which we already run; Restate; Inngest) — **not** a
sandbox (different layer). Lean: keep Temporal.

---

## 6. The graph-research flow, end to end (who publishes, who consumes)

**Step 0 — Author.** A human writes a `graph.yaml`: the *initial* topology + tool definitions + initial
mandates.

**Step 1 — Apply (`coral apply`, a one-time bootstrap).** It consumes `graph.yaml` **once** — writes the
DB rows (topology / config / tool-defs) and **materializes the FS** (each agent's dir + git repo +
initial `mandate.md`) — then is **inert**. No runtime role thereafter; the graph evolves via the agents.

**Step 2 — The graph runs itself.** Each agent runs the §4 cycle loop on its own cadence:

```
CHILD agent wakes (seed trigger, or its cadence timer)
  └─ inner loop: read own notes → call web/X MCP tools (direct, in-context) → reason
       └─ each tool call → runtime writes evidence/<slug>.md (runtime-authored)
  └─ write_output { body: prose → outputs/<slug>.md (FS), citations: [evidence_ref] → DB }
       └─ runtime rejects unless every citation resolves   ← provenance enforced here
       └─ the new output is a new git blob sha
  └─ idle (set_cadence)

PROPAGATION (the continuous mechanism, #14b)
  └─ the child's new blob sha makes any reference that pinned the OLD sha stale
  └─ the runtime finds the dependents (the parent) and fires a "dependency changed" wake

PARENT agent wakes (on the staleness wake)
  └─ inner loop: read its children's latest outputs (descendant-subtree read, read-only)
       → reconcile (synthesize; optionally record conflicts/<slug>.md)
  └─ write_output citing the child outputs (citations → DB, version-pinned)
       └─ its own new blob sha → propagates upward … to the root
```

So the **producer/consumer relationship is still the edge**, but the *transport* changes: a child
**publishes** an output to its own git FS; the **reference graph in the DB** detects that dependents'
pinned versions are now stale and **wakes** them. The parent **consumes** by reading the child's output
file and re-publishing its own. The root's output is the always-current, reconciled top of this flow —
**the root output = the top-level output of the root node** (`…/agents/<root>/outputs/…`), the view the
user reads.

*→ from today:* the as-built flow pushed a `ChildOutput` signal carrying the output id, and reconcile
wrote *synthetic evidence* into the parent. The target replaces both with **staleness propagation** (the
reference graph is the dependency graph is the wake-propagation edges) and **structured citations in the
DB** (no synthetic-evidence copy). An all-"never" graph still assembles its result this way — updates
ripple up as each new version makes its parents' references stale.

---

## 7. Provenance by construction (the invariant that ties it together)

The chain is unbroken and enforced mechanically — same guarantee as today, relocated:

```
tool call ─► runtime writes evidence/<slug>.md  (runtime-authored, tamper-evident)
                  │ cited by (output-level)
write_output ──►  { body: prose → outputs/, citations: [evidence_ref] → DB }
                  │ runtime REJECTS unless every citation resolves
cross-agent ──►  parent reconcile reads child outputs → cites them (version-pinned) → DB reference edge
                  │ … and upward to the root
```

- **Authorship boundary** keeps it non-fakeable: evidence is *runtime-authored* (the model can't write
  `evidence/`), so a citation always resolves to a *real observation*, not prose the model invented.
- **Citations live in the DB**, never in the file (A1) — provenance is a DB/tooling overlay, not
  something read from raw prose. Granularity is output-level (the old `EmitOutput { content, evidence }`
  guarantee, with a markdown body).
- **Version-pinned + time-scrubbable:** a citation pins the exact blob sha it cited; old outputs stay
  pinned to the versions they cited; auto-follow-to-latest is rejected (that would silently change the
  evidence under a past claim).
- **Conflicts** remain first-class: a parent records `conflicts/<slug>.md` when it holds a disagreement
  open or picks a side over a recorded alternative.

*→ from today:* the as-built form was a sha-named JSON evidence file cited by id, with reconcile writing
synthetic evidence. The target keeps the *guarantee* (no claim without a resolvable, real-observation
trail) and changes the *form* (prose body + DB-side citations + version-pinned reference edges).

---

## 8. The continuous / persistent model

This is now the *default*, not a mode (#10/#11):

- **Persistence is universal** — no flag. An agent never self-terminates; termination is a kernel/human
  op (`retire` signal / `RetireChild` / teardown). The prompt has one lifecycle: *produce/refresh, then
  idle.*
- **Cadence** (per node) governs *only* the self-scheduled `ScheduledWake`; a **"never"** sentinel means
  "don't self-wake — only act on triggers." All other triggers (child-update, human, external) are
  always live: they're the *dependency graph*, not the clock.
- **A "one-off" graph** is just a graph where everyone's cadence is "never": each agent runs once on
  seed, idles, and re-wakes only when an upstream/external change arrives. The final result still
  assembles via staleness propagation (§6).
- **Budget** (#12, deferred) is the intended *sole* runaway guard / cycle-termination backstop; until it
  lands, the **interim step-cap** does the job.

*→ from today:* replaces the `persistent` flag + the `Retire`→`Idle` demotion + `max_ticks`. "Continuous"
stops being opt-in machinery and becomes the substrate's shape; a parked agent is near-free.

---

## 9. Honest gaps — deferred / not yet (so this doc doesn't over-promise)

**First and foremost: none of this is implemented.** This is the target; the as-built engine is described
in `architecture_walkthrough.md`. Beyond that, consciously deferred:

- **Code-execution + sandbox + MCP-code bridge** (#13, future). Near-term the Act is *direct tool calls,
  results in-context*. Code-execution returns later as an **opt-in power tool** for fan-out/quant work
  (sandbox spun up on demand, results → FS, an MCP-code bridge owning the tool→evidence write path). The
  heaviest single build; deferred until a real need shows up. *(The maintainer is drawn to a fuller
  code-first design eventually.)*
- **Budget** (#12). Load-bearing for cycle termination + runaway protection + context bounding, yet
  unspecified (scope / denomination / enforcement point / exhaustion behavior). The interim step-cap is
  scaffolding, not the end state.
- **The propagation subsystem's shape** (#14b). The "DB wakes the dependents" is a real reactive
  notification system over the reference graph — write-path enqueue vs. sweep vs. `LISTEN/NOTIFY`,
  coalescing (don't thrash a hot parent), fan-out bounds — to be designed.
- **Human-in-the-kernel** — override / inject / dispute / observe surfaces (VISION's load-bearing
  principle). Untouched here by choice.
- **Reconcile as a *structured primitive*** — its exact shape (conflict records, hold-open vs. resolve)
  is deferred; near-term reconcile is repertoire work (read subtree → synthesize → cite).
- **Forking / whole-graph snapshot** — per-agent fork is "copy content + fresh `git init`"; whole-graph
  snapshot/time-scrub deferred.
- **Observability** — the system is more dynamic (pull-navigation, universal persistence, no max_ticks);
  the operator surface to watch it is undesigned.
- **Object-store storage** — git is the *local/dev* versioning impl behind the `AgentStorage` trait; the
  production object-store equivalent ("git's object model on S3") is a future stage.
- **Context-size bound near-term** — with code + budget both deferred, nothing hard-bounds per-cycle
  context volume; the only discipline is "the agent pulls selectively." Fine at small scale; a real gap
  as state accumulates.

**Consequence to keep in view:** provenance lives only in the DB → **DB durability is load-bearing**
(Postgres WAL/backups/replication); provenance does not survive total DB loss the way FS-resident state
would.

---

## 10. Invariants that do NOT change (this is evolution, not a rewrite)

- **Temporal as the orchestration/scheduling layer** — workflow-per-agent, durable timers, signals,
  child-workflow topology, continue-as-new, near-zero idle cost (role thinned, layer stays).
- **Parent→child topology + child→parent output flow** — the mechanism generalizes to read-only subtree
  reads; transport shifts from signals to staleness propagation.
- **MCP-native tools** — data fetchers are MCP servers; per-graph registries.
- **Provenance by construction** — "no claim without a trail to evidence" is non-negotiable; only the
  *form* changes.
- **Content-addressing as the *mechanism*** — still content-addressed underneath (git blob shas) for
  integrity/dedup; just never the human surface.
- **The authorship boundary** — runtime-authored evidence vs. agent-authored prose.
- **Determinism / idempotency on the Temporal path** — retries stay byte-safe (git clean-tree no-op;
  content-derived blob shas; idempotent DB upserts; `scheduled_time` for any timestamps).

---

## 11. Where to look (concern map in `design_realignment.md`)

- The composed end-state in one place: **`design_realignment.md` → "Target shape"** (authoritative).
- The cycle loop: **gap 4 / the "harness" entry in Target shape** + Concern 8.
- The FS/metadata split + versioning: **A1** + Concern 1/4/5/9 + the git "Versioning mechanism" block.
- Provenance + consistency mechanics: **Concern 14** (14a citations, 14b propagation, 14c FS↔DB).
- Lifecycle: **Concerns 10/11** (universal persistence, cadence, no max_ticks).
- Model routing: **Concern 2**. DB role + fork: **Concerns 6/7 + A1**.
- What's deferred and why: the **"Consciously deferred"** + **"Open"** bullets at the end of the doc.
