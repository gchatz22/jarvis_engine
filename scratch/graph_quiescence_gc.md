# Graph quiescence GC — retire provably-dead graphs, keep continuous ones alive

## Problem

Every agent is a Temporal workflow that **never self-completes**. The code says
so at `workflow.rs`: *"The agent never self-terminates — termination is a
kernel/human decision."* An idled agent parks at the wake gate awaiting a
signal (~0 compute) rather than finishing. V3.1 made this universal by deleting
`Decision::Retire` and the `persistent` flag.

That is correct for the class the engine is *for* — continuous monitors and
re-propagation trees, where "done" is not a state (see "Why it's built this
way"). But the runtime has exactly **one** idle state — *parked-alive* — and
cannot tell apart:

- **idle, more signal can still arrive** (a real monitor), from
- **idle, and provably no signal can ever arrive again** (*quiescent*).

`scratch/rust_vs_go_minimal.yaml` is the second kind, and provably so: every
node is `idle_period: never` (self-wakes only the forced first tick, then waits
on signals alone), the kickoff triggers are one-shot (consumed at apply), and
once the parent folds both children and everyone idles, **nothing in the system
can originate another event** — no armed timer, no unfired trigger, no cadence,
no subscription. It is a *terminated computation* holding a *live workflow* —
history, timers, a worker slot — indefinitely. At the millions-of-agents target
this is a real leak: every finished graph is a permanent live object, and they
only accumulate.

Observed live: graph `rust-vs-go-minimal-14` reached all-idle-and-committed,
and its three workflows stayed `Running` in Temporal with the worker at ~0.1%
CPU and no pending activities — parked forever with nothing left to monitor,
alongside ~a dozen older finished graphs in the same state.

## Why it's built this way (keep this)

Always-alive is load-bearing for three things and must stay the default:

1. **Re-propagation (the V6 model).** A child can re-emit a `v2` `ChildOutput`
   later → that signal re-wakes the parked parent → re-reconcile → re-propagate
   up. The wake is delivered as `external_workflow(id).signal(...)` — a signal
   to a **running** workflow, best-effort, **swallowed on failure**
   (`workflow.rs`). If the parent had terminated, the wake lands nowhere and the
   graph silently fails to update.
2. **Cadence monitors** (`idle_period: <duration>`) wake on a timer forever.
3. **Human override** as a first-class mutation — the live workflow is the
   handle you scrub / re-drive through.

For that continuous class there is genuinely no "done," and per-agent
self-termination is **unsafe**: a parked agent cannot know a sibling or parent
won't signal it next. **Termination is a graph-global property.**

## Governing invariant (the whole doc bends to this)

The danger is **asymmetric**:

- A **false negative** (linger a graph that was actually quiescent) costs a
  little Temporal state. Self-correcting on the next sweep.
- A **false positive** (retire a graph that had a wake coming) drops that wake
  through the best-effort/swallowed signal path — a **silent correctness bug
  with no error surface**.

**Every choice biases to false-negatives.** When unsure, do not retire.

## The predicate (narrow scope: `never` + exhausted triggers only)

A graph is retire-eligible iff **all** hold:

1. **Every agent is `never`-cadence** (from graph config / structural DB, not
   from a snapshot). This is the scope gate — see soundness below.
2. **Every agent is parked at the wake gate** (not mid-cycle) with **no armed
   `next_wake`**.
3. **Every agent's pending queues are empty** (triggers, human-ops,
   mandate-patches all `0`).
4. **All agents' `cumulative_*_observed` counts are stable across K consecutive
   samples** spanning a margin longer than one full root-ward propagation ripple
   for the graph's depth (not two adjacent samples — see soundness).
5. **A live worker is polling the graph's task queue.** Without this,
   "appears idle" is indistinguishable from "unobserved" (a worker restart must
   not look like a graveyard).

On all-true: signal `retire` to every agent in the graph. The `retire` signal
and the `Retired { reason }` exit path **already exist** — the loop checks
`retirement_request` at the top of every tick and short-circuits to
`persist_retirement` + return. This is **wiring, not new lifecycle machinery.**

The durable record survives termination: the graph store + per-agent git FS
already hold outputs, evidence, decisions, and tick history. The workflow is
only the *running* representation; the graph store is the *durable* one. Today
the two are conflated — which is exactly why "done" costs forever.

## Why the predicate is sound (and its exact scope boundary)

The load-bearing fact: **an idle agent sends no signals until something wakes
it.** So the moment *all* agents are idle, the graph's send-set is **frozen** —
no new signal can originate. The only residual risk is a signal sent just
*before* its sender idled and not yet received. Condition (4)'s stability window
catches exactly that: if any in-flight `ChildOutput` were about to land, the
recipient's `cumulative_triggers_observed` would bump inside the window →
instability → no retire.

Two things this proof **requires**, both baked into the predicate:

- **The window must cover a full propagation *wave*, not one hop** (condition 4).
  A `ChildOutput` in flight to a mid-level parent wakes it → it works → emits to
  *its* parent → ripples root-ward. Two adjacent samples would miss the ripple;
  the margin must exceed a full depth-D root-ward wave.
- **The frozen-send-set argument holds ONLY for `never` + exhausted triggers**
  (condition 1). With a cadence timer or any re-propagation/subscription source,
  "all idle" is **not** a fixpoint — the graph can wake itself — so the
  send-set is not frozen and this predicate is invalid. That is the hard scope
  boundary of this first cut.

Honesty note: "no in-flight signal" is **not directly observable** by an
external sweep. Conditions (3)+(4) are the *translation* of it into observable
terms (empty queues + stable received-counts across a wave), valid only under
the frozen-send-set of condition (1). This is a translation, not a direct check.

## Mechanism (first cut = wiring)

1. **Enumerate** a graph's agents from the structural DB (already the source of
   truth for graph membership).
2. **Inspect** each via the existing `inspect_state` `#[update]` →
   `AgentSnapshot`.
3. **Snapshot extension:** `AgentSnapshot` is `#[non_exhaustive]`. Add an
   explicit *parked* indicator + armed `next_wake` (and `is_never`) so
   conditions (1)(2) are directly checkable rather than inferred. Small,
   backward-compatible field additions.
4. **Predicate** over the K-sample window (conditions 1–5).
5. **Retire:** `get_workflow_handle(id).signal(retire, reason)` to every agent
   (the same handle+signal path `coral-apply` already uses to deliver seed
   triggers). Agents short-circuit to `Retired` on their next tick.

### Host: a reaper task inside the worker (first cut)

The sweep runs as a background `tokio` task spawned at worker startup, next to
the Temporal poll loop. It already holds everything it needs in-process — the
Temporal client, the structural-DB pool, and (the load-bearing one) it **is** a
live worker, so condition (5) is satisfied for free for every graph on a task
queue this worker serves.

```
loop {
    sleep(GC_INTERVAL);                       // e.g. 30s
    for graph in structural_db.graphs() {
        let snaps = graph.agents().inspect_state();   // existing #[update]
        tracker[graph].observe(now, snaps);           // pure debounce, below
        if tracker[graph].eligible() { retire_all(graph); }
    }
}
```

Key properties:

- **The wave-window is a debounce, not a spin.** The reaper keeps a tiny
  in-memory `{graph → (last_digest, stable_since)}` map. Condition (4) is met
  when the digest (all-queues-empty + all cumulative counts + all parked +
  all-`never`) holds unchanged across a span ≥ `wave_margin`. Any change — a
  count bump, a queue fill — resets `stable_since`. One inspect per graph per
  interval; the margin is enforced across sweeps, not by a tight loop.
- **The reaper is not a workflow** — it's a plain task in the worker process, so
  it may use the wall clock (`Instant`) freely. No determinism constraint.
- **No durable state.** The stability map is in-memory. A worker restart makes
  every graph re-earn the full quiet window — correct, because it is
  false-negative-biased: a restart delays GC, never causes a false positive.
- **Multi-worker races are harmless.** `retire` is idempotent (sets
  `retirement_request`; a second signal is a no-op), so two reapers deciding the
  same graph just send a redundant signal.
- **Decision core is pure and unit-testable** (`GraphQuiescenceTracker::observe`
  over synthetic `AgentSnapshot` sequences); the reaper is a thin Temporal/DB
  shell around it, covered by the e2e test.

Rejected alternative — a standalone `coral gc` binary/cron: strictly more moving
parts (another deployable, and it must independently *prove* a live worker
exists rather than getting it for free) with no benefit at this stage.

**Don't probe closed workflows.** Before the (potentially blocking) `snapshot`
query, the reaper `describe`s each agent — a fast server-side metadata read — and
skips the graph if any agent is not `Running`. A closed workflow cannot belong
to a live quiescent graph, and querying one on a task queue with no live worker
would block until the query timeout. Every remote call is also wrapped in a
short per-call timeout as a backstop.

**Known limitation — serial O(graphs) sweep.** The first cut sweeps graphs
sequentially, so total sweep latency grows with the graph count (and with any
live-but-unresponsive workflow, bounded by the per-call timeout). Fine for now;
the scaling answer is either a concurrent sweep or, better, the event-driven
per-graph supervisor (future direction 1), which removes polling entirely.

## Scope

**In (first cut):**
- Quiescence predicate for `never` + exhausted-trigger graphs.
- `AgentSnapshot` parked/`next_wake`/`is_never` fields.
- A sweep that enumerates → inspects across a wave-window → retires.
- Live-worker guard.
- Tests (below).

**Out (this issue) — see "Future directions" for each.**

## Future directions

The first cut deliberately covers only the provably-decidable slice. Three
named evolutions extend it, in rough dependency order:

1. **Event-driven detection via a per-graph supervisor workflow (the scaling
   end-state).** The reaper polls O(graphs) per interval — fine now, wrong at
   the millions-of-subagents target, where a central sweeper walking the whole
   population is the wrong shape. The aligned end-state is event-driven: each
   graph gets a **supervisor workflow** that its agents notify on every
   idle↔wake transition. When the last agent reports idle and a stable interval
   passes with no new transition, the supervisor retires its children and then
   **completes itself** (so the supervisor is not a new leak). No scan — each
   graph self-monitors. Cost: a new workflow type, transition-reporting on the
   agent hot path, and apply-time wiring to spawn it. This is also the natural
   home for direction (2)'s counters, so the two land together.

2. **Cadence / re-propagation graphs (the general case).** For a graph that can
   wake itself (a cadence timer) or receive a legitimate late signal
   (re-propagation, an external subscription), "all idle" is **not** a fixpoint,
   so the narrow predicate is invalid by construction. The rigorous detector is
   Mattern-style distributed termination detection: **sent == received globally,
   and all idle.** Today only the *received* side exists
   (`cumulative_*_observed`); this needs a **new sent-counter** on each agent and
   a place to aggregate both graph-wide — which is the supervisor from (1). Until
   this lands, cadence/subscription graphs are simply never GC'd (correct:
   false-negative-biased).

3. **Terminate-and-rehydrate (reclaim even revivable graphs).** (1) and (2)
   still keep a workflow alive whenever a future signal is *possible*. To reclaim
   even those — retire a graph that could still be re-driven, and revive it on
   demand — the wake path must move from `external_workflow(id).signal(...)`
   (signal a **running** workflow, dropped if absent) to **`signalWithStart`**
   semantics: the signal re-starts the workflow, which re-hydrates from its
   durable FS exactly as continue-as-new resume already does. This turns the
   workflow into a purely lazy representation of durable state — the truest form
   of "the graph store is the system of record." Largest change; last.

**Also out of scope:** retiring **mid-cycle busy** agents (e.g. an inspection
doom-loop with empty queues). Condition (2)'s parked-check deliberately excludes
them — false-negative bias. Revisit only with explicit intent.

## Test plan

- **Predicate unit tests** over synthetic `AgentSnapshot` sets: all-idle-stable
  ⇒ eligible; one non-empty pending queue ⇒ not; a `cumulative` bump between
  samples ⇒ not; a non-`never` agent present ⇒ not; no live worker ⇒ not.
- **Wave-window test:** a two-hop chain where a mid-level count bumps on sample
  k must keep the graph ineligible through the window (guards the one-hop bug).
- **Live/e2e:** apply `rust_vs_go_minimal.yaml`, let it converge, assert the
  sweep retires all three workflows and that re-reading outputs from the durable
  store still resolves after termination (durable record survives).
- **Negative live:** a `never` graph that has NOT yet converged (a pending
  trigger, or an agent mid-cycle) is left untouched.

## Immediate cleanup (independent of the above)

Terminate the stale `Running` workflows now (graph `-14` + the older finished
graphs) so we're not carrying them while this lands. Pure ops, no code — via the
Temporal client (`retire` signal or terminate). Does not touch durable state.
