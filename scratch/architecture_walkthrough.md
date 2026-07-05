# Coral Engine — Architecture Walkthrough

> A reading/iteration doc. How graph research currently works, how Temporal fits in,
> what the primitives are, how they connect, and who publishes vs. consumes.
> File:line anchors throughout so you can jump straight to code.
>
> Status: descriptive snapshot of the code as it stands (not aspirational). Section 8
> lists what is *not* there yet so the VISION framing doesn't over-promise.

---

## 0. The one-sentence model

> **A graph of long-lived agents, each one a durable Temporal workflow running an
> idle → wake → decide → act loop, whose state lives in three separate planes —
> topology in Postgres, working memory + provenance on a per-agent filesystem, and
> execution/control state in Temporal history — and whose outputs flow child → parent
> until the root holds a continuously-current, fully-sourced view.**

The codebase is four crates that map almost one-to-one onto `VISION.md`'s layers:

| Crate | ~Lines | Role | VISION layer |
|---|---|---|---|
| `coral_node` | 17k | Agent runtime + per-agent FS + evidence/conflict + MCP + model clients. **Pure library, knows nothing about Temporal.** | Agent runtime, Per-agent FS, Data layer |
| `coral_graph` | 5.6k | Structural store (Postgres) + the `graph.yaml` author format + the `coral apply` CLI | Graph layer |
| `coral_temporal` | 8k | Wraps the `coral_node` tick into a **durable Temporal workflow** + activities | Kernel (process model, scheduling, durability, lifecycle) |
| `coral_worker` | 1.5k | The **composition root**: a deployable daemon that wires the other three together and hosts the Temporal worker | (binds the kernel to concrete backends) |

Dependency edges point *inward*; the cycle is broken by inversion of control:

```
coral_worker  ──────────────►  composes everything, hosts the worker daemon
   │  │  │
   │  │  └──► coral_graph ──► coral_temporal (implements its StructuralDbStore trait)
   │  │              └──────► coral_node     (uses its types for YAML→runtime conversion)
   │  └─────► coral_temporal ──► coral_node  (Decision/Trigger/Mandate/Evidence + AgentStorage trait)
   └────────► coral_node                      (standalone — no workspace deps)
```

`coral_node` is the leaf. `coral_temporal` declares *traits* (`StructuralDbStore`,
`ToolRegistryProvider`, `Decide`, `AgentStorage`) but not their concrete impls;
`coral_graph` and `coral_worker` supply those at boot. The kernel declares the holes,
the composition root fills them — so there is no dependency cycle.

---

## 1. The primitives (the nouns)

Three families of primitives, one per storage plane. Keeping them separate is the single
most important thing to understand about this system.

### A. Structural primitives — *topology*, lives in Postgres (`coral_graph/src/types.rs`)

What agents exist and how they're wired — nothing about what they're currently doing.

- **`Graph`** (`types.rs:15`) — `{ id, name (unique), metadata (JSONB), created_at }`.
  Top-level container. `metadata` carries pass-through `policy:` (cost budgets etc., not
  yet enforced).
- **`AgentRecord`** (`types.rs:34`) — `{ id, graph_id, name, mandate_ref, persistent, model, created_at }`.
  One node. Note what's *not* here: the mandate **text** isn't in the DB — only an opaque
  `mandate_ref`. The authored instruction lives in the YAML (git-versioned); the DB stores
  topology + a couple of flags (`persistent`, per-agent `model` override).
- **`Edge`** (`types.rs:56`) — `{ id, parent_agent_id, child_agent_id }`, `UNIQUE(parent,child)`.
  Directed parent→child. This *is* the graph.
- **`ToolRecord`** (`types.rs:70`) — `{ id, kind ("builtin"|"mcp"), command, args (JSONB),
  env_refs (JSONB) }`, joined to agents via an `agent_tools` M:N table.

Schema is INSERT-only / CREATE-only (migrations under `coral_graph/migrations/`). Re-applying
a graph with the same name fails with `GraphAlreadyExists`. No mutation log here — mutation
history is not this layer's job.

### B. Runtime primitives — *behavior*, lives in code + per-agent FS (`coral_node`)

- **`Mandate`** (`mandate.rs:55`) — the standing instruction:
  `{ text, idle_period, max_ticks, retry_policy, context_policy, persistent, model }`.
  The agent's "job description" + operating envelope.
- **`Decision`** (`decision.rs:68`) — **the central verb. 9 variants:**
  1. `CallTools { calls }` — invoke ≥1 tools, in parallel, this tick
  2. `EmitOutput { content, evidence }` — publish a claim (runtime *refuses* if any cited
     evidence id doesn't resolve on disk — provenance enforced here)
  3. `RewriteFs { ops }` — mutate own filesystem (sandboxed to `notes/`)
  4. `Idle { next_after }` — sleep this long, do nothing
  5. `Retire { reason }` — terminate (auditable)
  6. `SpawnChild { agent_name, mandate }` — create a child agent
  7. `ReconcileChildren { sources, conflict? }` — fold child outputs into own context,
     optionally record a conflict
  8. `RetireChild { child_ref, reason }` — kill a child
  9. `ReplaceChild { child_ref, new_mandate }` — retire + spawn fresh (not in-place)

  Variants 1–5 are *self*-actions executable in-process; 6–9 are *topology* actions that
  only mean something on the workflow host (the in-process `agent_core` panics if it ever
  sees them — `agent_core.rs:167`).
- **`Trigger`** (`trigger.rs:39`) — what wakes an agent: `ScheduledWake` (idle timer fired),
  `External { kind, payload }` (webhook/kickoff), `HumanOverride { op }`,
  `ChildOutput { child_ref, output_id }`, `ChildRetired { ... }`. Drained in **priority
  order**: Human > External > Child* > Scheduled (`trigger_queue.rs:119`).
- **`ContextBundle`** (`decision.rs`) — the read-only snapshot handed to the model each
  tick: mandate + recent-outputs window + recent-evidence window + open claims + drained
  triggers + any staged correction. Assembled deterministically (sorted keys, fixed window
  sizes from `ContextPolicy`).
- **`EvidenceRecord` / `EvidenceId`** (`evidence.rs:16`) — one tool observation
  `{ tool, args, result, created_at }`; id = `sha256(canonical_json(tool, args, result))` —
  **content-addressed**, timestamp excluded. Identical tool calls dedupe to one file.
- **`Output` / `OutputId`** (`mandate.rs:215`) — a published claim; id =
  `sha256(content + sorted evidence ids)`. Same output twice → same file (retry-idempotent).
- **`ConflictRecord` / `ConflictId`** (`conflict.rs:99`) — a recorded disagreement:
  `{ alternatives (≥2), resolution (None = held open) }`. Content-addressed on
  `(alternatives, resolution)`.

The per-agent FS schema (`fs.rs:4`), rooted at `graphs/<gid>/agents/<aid>/`:

```
mandate.json              standing instruction
outputs/<sha256>.json     claims (immutable, must cite evidence)
evidence/<sha256>.json    raw tool observations (dedup via put_if_absent)
notes/                    mutable scratch (the only writable subtree)
claims/<slug>.json        open-claim registry
conflicts/<id>.json       reconciled disagreements
decisions/<tick>.jsonl    one line per tick — the decision log
health.json + health/     health state machine + archived incidents
retirement.json           terminal marker
```

This *is* "state is files, not hidden context" made concrete. Backed by a pluggable
`AgentStorage` trait (`storage/mod.rs`) — `LocalStorage` (POSIX dir) in production,
`MemoryStorage` for tests.

### C. Durability primitives — *execution/control state*, lives in Temporal history (`coral_temporal`)

- **`AgentWorkflow`** (`workflow.rs:322`) — the long-running workflow = one agent. In-memory
  state: pending signal queues, `next_wake`, `staged_correction`, `child_handles`, `tick`,
  cumulative counters.
- **Activities** (`activities.rs`) — the only place I/O is allowed (see §4):
  `assemble_context`, `decide_next_action`, `execute_tool`, `persist_output`, `apply_fs_ops`,
  `persist_retirement`, `append_decision_log`, `register_child_in_structural_db`,
  `reconcile_children`.
- **`Carryover`** (`workflow.rs:155`) — the serializable state that survives a
  continue-as-new restart.

---

## 2. The three storage planes (the key architectural idea)

Each primitive family lives in a distinct store, and they never overlap responsibilities:

| Plane | Backend | Owns | Written by | Read by |
|---|---|---|---|---|
| **Structural** | Postgres (`GraphStore`) | what agents/edges/tools exist | `coral apply` (once), `register_child_in_structural_db` (on spawn) | worker at boot, tool-provider per graph |
| **Working memory + provenance** | per-agent FS (`AgentFs`) | mandate, outputs, evidence, conflicts, notes, decisions, health, retirement | activities (`persist_output`, `execute_tool`, …) | `assemble_context` activity, TUI/inspectors |
| **Execution/control** | Temporal history | pending signals, schedule cursor, child handles, tick counter, in-flight progress | the SDK, automatically | the SDK on replay |

Pithy version: **Postgres knows the shape, the filesystem knows the work, Temporal knows
the clock and the cursor.** No claim ever lives only in Temporal — outputs and evidence are
durable on the FS, so Temporal history is "how we got here" while the FS is "what we found."
This is why a continue-as-new (which discards Temporal history) loses nothing material.

---

## 3. The lifecycle of one agent (the verb): the tick loop

There are *two* implementations of the same loop, and they're twins — that's the crux of
"how Temporal fits in":

1. **In-process loop** — `coral_node/src/agent.rs:155` (`Agent::run`). A plain async `tokio`
   loop. Used for unit tests and as the conceptual reference.
2. **Durable loop** — `coral_temporal/src/workflow.rs:446` (`AgentWorkflow::run`). The *same*
   logic re-expressed as a replayable Temporal workflow. This is what runs in production.

Both do the identical six-phase cycle. The durable version:

```
loop {
  ① max_ticks guardrail   — if tick >= max_ticks → retire (before waking, no decide spent)
  ② wait_for_tick         — race the idle timer vs. signal arrival (Temporal select!)
  ③ drain_buckets         — pull pending triggers/human-ops; retire-signal short-circuits here
  ④ assemble (activity)   — open FS, build ContextBundle from FS + drained signals
  ⑤ decide  (activity)    — run the Decide impl (LlmDecide) → a Decision
     log_decision(activity)— append decisions/<tick>.jsonl BEFORE dispatch (survives a crash)
     demote_retire_if_persistent — a persistent agent's Retire becomes Idle
  ⑥ match decision → dispatch via the right activity (the 9 variants from §1)
     bump tick
     if continue_as_new_suggested() → encode Carryover, continue_as_new (restart fresh)
}
```

Decision-handling nuances that matter:

- **Correction loop, not exceptions.** When a tool batch or a reconcile fails, the workflow
  doesn't error out — it stashes a `CorrectionContext` (`staged_correction`) describing the
  failure. Next tick's bundle includes it, so the *model itself* sees "your last decision
  failed because X" and produces a fix. The correction clears on the next
  `EmitOutput`/`RewriteFs`/`Idle`, but **not** on child-management ops (those don't satisfy a
  correction about the parent's own work — comments at `workflow.rs:507`).
- **Health state machine** (`health.rs`). Each tick gets a retry budget (1 inference retry +
  3 tool retries by default). Exhaustion flips the agent `Healthy → Unhealthy` with an
  archived incident; the agent stays in the loop awaiting recovery rather than dying.
- **Decide-side parsing.** `LlmDecide` (`decide_llm/llm_decide.rs:127`) itself retries once on
  a malformed tool-call response (synthesizing placeholder tool-results so the retry request
  stays schema-valid) before giving up.

---

## 4. How Temporal fits in — the episodic → continuous transform

Temporal contributes exactly five mappings:

| Agent concept | Temporal mechanism | Where |
|---|---|---|
| "an agent" | a **workflow execution** (`AgentWorkflow::run`), id = `graphs/<gid>/agents/<aid>` | `workflow.rs:446` |
| "wake me on a signal or a timer" | `workflows::select!` racing `wait_condition` (queue non-empty) vs. `ctx.timer(next_wake)` | `workflow.rs:679` (`wait_for_tick`) |
| "an inbox" | **signals** — `external_signal(Trigger)`, `human_override`, `mandate_update`, `retire` | `workflow.rs:384` |
| "do a side effect" | **activities** — the only code allowed to touch FS / DB / model / MCP | `activities.rs` |
| "spawn a child" | a **child workflow** with `ParentClosePolicy::Abandon` | `workflow.rs:1051` |
| "live forever despite history limits" | **continue-as-new** carrying a `Carryover` | `workflow.rs:561` |

**The determinism contract is *why* the code is split into workflow + activities.** Temporal
achieves durability by *replaying* a workflow's history after any crash/restart. For replay to
be deterministic, the workflow body must contain **zero** real I/O and zero wall-clock reads —
no `tokio::spawn`, no `Utc::now()`, no network. So:

- The **workflow** (`workflow.rs`) is pure orchestration: it decides *what* to do and in what
  order, using only `ctx.timer`, `ctx.wait_condition`, `workflows::select!`,
  `workflows::join_all`.
- Every actual effect is an **activity** (`activities.rs`) — model calls, tool calls, FS
  writes, DB writes. Activities may be non-deterministic and are retried independently.

Two consequences that show real care:

- **Timestamps come from `ctx.info().scheduled_time`** (stamped into history), never
  `Utc::now()`. Combined with content-addressing, activity retries write *byte-identical*
  files — a retried `execute_tool` produces the same `evidence/<id>.json`, a retried
  `persist_output` the same `outputs/<id>.json`. Idempotency by construction.
- **`decisions/<tick>.jsonl` is written *before* dispatch** (`workflow.rs:485`) so the record
  of what the model chose survives even if the dispatching activity then crashes.

**Continue-as-new = the "continuous" in continuous monitoring.** Temporal histories can't grow
unbounded. When the server suggests it (`continue_as_new_suggested()`), the workflow serializes
its live state into a `Carryover` (pending signals, `next_wake`, `staged_correction`,
`child_handles`, cumulative counters, and crucially the monotonic `tick`) and restarts itself
with a fresh history. To an operator the agent looks like it's run forever; under the hood it
has reincarnated many times. Nothing material is lost because outputs/evidence were always on
the FS, not in history.

**`ParentClosePolicy::Abandon`** (`workflow.rs:1113`) makes the parent a *process manager*, not
an *owner*: children survive the parent's restart, continue-as-new, or even retirement. A child
only dies when explicitly `RetireChild`-signaled. This is what lets the graph be genuinely
long-lived rather than a call tree that unwinds.

---

## 5. The graph-research flow, end to end (who publishes, who consumes)

What actually happens when you run a research graph:

**Step 0 — Author.** A human (or an agent) writes a `graph.yaml` (`coral_graph/src/yaml.rs`
defines the schema). It declares: tools (builtin or MCP server commands), a forest of agents
each with a mandate (`text`, `idle_period`, `max_ticks`, `persistent`, `model`) and nested
`children`, and a `seed:` block of kickoff triggers. Examples in
`scratch/nvidia_supply_chain.yaml` and `scratch/persistent_monitor_live.yaml`.

**Step 1 — Boot the worker** (the long-lived daemon, `coral_worker/src/bin/worker.rs`). It
reads env, then **installs four process-wide backends** into `coral_temporal`'s `OnceLock`
slots:

- `AgentStorage` ← `LocalStorage` rooted at `AGENT_FS_ROOT`
- `Decide` ← `LlmDecide` over Anthropic or Cohere (auto-detected from API keys / `CORAL_MODEL_VENDOR`)
- `StructuralDbStore` ← `GraphStore` over `DATABASE_URL` (Postgres)
- `ToolRegistryProvider` ← `DbToolRegistryProvider` (spawns MCP servers per graph, lazily)

Then it registers `AgentWorkflow` + `AgentActivities` on the `coral-agents` task queue and
blocks serving tasks.

**Step 2 — Apply the graph** (`coral apply`, `coral_graph/src/bin/coral_apply.rs` — a thin
Temporal *client*, hosts no worker). It:

1. `parse_and_validate` the YAML (`yaml.rs:570`),
2. runs migrations and does **one transaction** writing graphs/agents/edges/tools/agent_tools
   and allocating UUIDs (`store.rs:223`, `create_from_yaml`), returning an `AppliedGraph` with
   an `operator_id → (db_id, workflow_id)` map,
3. **starts the workflows** in DFS parents-first order (each child's input carries its parent's
   already-issued workflow id),
4. **signals the seed triggers** to kick the roots (or any named agent) into motion.

**Step 3 — The graph runs itself.** Now it's pure agent dynamics on the worker:

```
ROOT agent wakes (seed trigger)
  └─ decide → SpawnChild ×N
       └─ register_child_in_structural_db (writes agents+edges rows, mints child AgentId)
       └─ start child workflow (Abandon), remember AgentRef in child_handles
  └─ Idle

CHILD agent wakes (seed/own timer)
  └─ decide → CallTools { get-sum, web-search, ... }
       └─ execute_tool activity → tool_registry_provider().registry_for_graph(gid)
            └─ (first call per graph) read tools rows from Postgres,
               spawn the MCP servers over stdio, cache the registry
       └─ MCP RPC → result → write evidence/<id>.json, return EvidenceId
  └─ next tick: decide → EmitOutput { content, evidence:[id] }
       └─ persist_output activity (verifies every evidence id resolves) → outputs/<id>.json
       └─ signal_parent_with_trigger(ChildOutput{ output_id })   ← child PUBLISHES to parent

PARENT receives ChildOutput on its signal queue
  └─ wakes, decide → ReconcileChildren { sources:[child output ids], conflict? }
       └─ reconcile_children activity: read each child's outputs/<id>.json (cross-agent),
          write one synthetic evidence record per source into the PARENT's evidence/,
          optionally write a conflicts/<id>.json if children disagreed
  └─ next tick: decide → EmitOutput citing those synthetic evidence ids
       └─ if it has its own parent → signal ChildOutput upward … the change ripples to the root
```

So the **producer/consumer relationship is the edge itself**: a child *publishes* an `Output`
to its own FS and *signals* a lightweight `ChildOutput` trigger to its parent; the parent
*consumes* by reading the child's output file during reconciliation and re-publishing its own
synthesized output one level up. The signal carries only the pointer (`output_id`); the payload
stays on the FS. The root's output is therefore always the current, reconciled top of this flow.

---

## 6. Provenance by construction (the invariant that ties it together)

The chain is unbroken and enforced mechanically:

```
tool call ─► EvidenceRecord (id = hash of tool+args+result)  [evidence/]
                  │ cited by
EmitOutput ──────►  Output (persist refused unless every evidence id resolves)  [outputs/]
                  │ read during
ReconcileChildren ► synthetic EvidenceRecord in PARENT's evidence/ (tool="reconcile",
                    args carry child_agent_id + source output_id, result = child's output)
                  │ cited by
parent's EmitOutput ► parent Output … and upward
```

The clever bit: **reconciliation reuses the evidence mechanism**. A parent folding a child's
conclusion doesn't get a special "child link" type — the child's output becomes a normal
`EvidenceRecord` in the parent's own evidence dir, so cross-agent provenance is just an ordinary
evidence trail. Every claim at the root traces, hop by hop, down to a raw tool observation with
a content hash. There is no code path that emits a claim without resolvable evidence
(`persist_output` enforces it; `agent_core.rs:130`).

Conflicts are first-class: when children disagree, the parent records a `ConflictRecord` with
≥2 alternatives and either picks one (`resolution: Some`) or holds it open (`None`) — the
VISION's "parent owns the resolution, human can override" made concrete (`conflict.rs`).

---

## 7. The persistent-monitor / continuous model

The recently-landed CM-* work (`scratch/persistent_monitor_live.yaml`,
`coral_worker/tests/persistent_monitor_live.rs`). It's what turns a one-shot research run into
a forever-running monitor:

- **`persistent: true`** on a mandate flips two behaviors: (a) the system prompt uses a "refresh
  forever" lifecycle tail instead of "one-shot" (`decide_llm/prompt.rs:112`), and (b) the
  runtime **demotes any `Retire` decision to `Idle`** (`workflow.rs:486`). A persistent agent
  can only be stopped by an external `retire` signal or the `max_ticks` guardrail — never by
  its own choice.
- **Re-reconciliation** is the parent's steady state: child wakes on its timer → produces a
  *fresh* output → signals the parent → parent re-reconciles the newer output and re-emits.
  Across a continue-as-new the parent's `child_handles` carry over, so it keeps managing the
  same children. The live test asserts exactly this: each agent produces ≥2 distinct outputs,
  the parent reconciles ≥2 distinct child outputs, and everyone stops via `max_ticks` (never
  model-`Retire`).
- A validation guard catches dead configs: a `persistent` parent must have ≥1 `persistent`
  child, else it would refresh forever with no newer inputs to fold (`yaml.rs`,
  `PersistentParentWithOneshotChildren`).

Machinery is proven green on the live smoke path; the open question is real-model
loop-viability (a manual run).

---

## 8. Honest gaps (what's real vs. aspirational)

So the VISION framing doesn't over-promise — what's *not* there yet, from the code:

- **Human-in-the-kernel is partial.** `HumanOverride`/`HumanOp` triggers and the
  `human_override` signal exist and are drained at top priority, but
  `mandate_update`/`MandatePatch` is wired through the queue yet **not applied** ("unwired
  today"). Override as a fully first-class re-decomposition primitive is scaffolding, not
  finished.
- **No graph mutation/versioning store.** The structural DB is CREATE-only;
  "time-scrubbable, versioned graph" from VISION isn't implemented as a store — versioning is
  delegated to git-on-YAML, and the FS is content-addressed but not snapshotted/forkable yet.
- **The workflow never *reads* the structural DB** — it only writes (child registration).
  Topology-at-runtime lives in `child_handles` in Temporal state, not via DB queries.
- **Scale is architected-for, not yet proven.** "Millions of subagents," sibling inference
  batching, MCP traffic dedup/cache as a kernel concern, model routing beyond a per-agent
  override — these are VISION targets. What exists today is correct single-agent and
  small-graph machinery (parent + 2 children in the live tests) with the *right seams*
  (content-addressing for dedup, per-graph MCP registry caching, `model` override) to grow
  into them.
- **`policy:` (cost budgets) is pass-through only** — stored in `graphs.metadata`, not enforced.
- **One backend each:** Postgres (no in-mem prod alternative), local FS, two model vendors
  (Anthropic/Cohere), MCP over stdio.

---

## 9. Where to look first if you read code

- The whole story in one file: **`coral_temporal/src/workflow.rs:446`** (`run`) — the durable
  tick loop.
- The vocabulary: **`coral_node/src/decision.rs:68`** (the 9 `Decision` variants, heavily
  commented).
- The plumbing / IoC: **`coral_worker/src/bin/worker.rs`** (the four `install_*` calls) — where
  the abstract kernel becomes a concrete deployment.
- The author format → runtime bridge: **`coral_graph/src/bin/coral_apply.rs`** +
  **`yaml.rs:759`** (`build_workflow_starts`).
- The mental-model "in-process twin": **`coral_node/src/agent.rs:155`** — same loop, no
  Temporal, easiest to read.

---

## Open threads to iterate on (scratchpad)

Things we could go deeper on next — pick and we'll expand inline:

- [ ] Full sequence diagram for one reconcile cycle (child emit → parent fold → re-emit).
- [ ] Trace a single `CallTools` decision through every activity hop with exact line numbers.
- [ ] Walk `decide_llm` prompt assembly: how a mandate becomes an LLM call + tool schema.
- [ ] The `Carryover` field-by-field: what survives CAN and why each field is load-bearing.
- [ ] Failure taxonomy: correction loop vs. health state machine vs. Temporal activity retry —
      who handles what, and the boundaries between them.
- [ ] How `graph.yaml` defaults/validation work (the `parse_and_validate` invariants).
