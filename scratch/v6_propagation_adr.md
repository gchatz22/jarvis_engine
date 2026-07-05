# ADR — Staleness→wake propagation (V6.1 / #148)

**Status:** Accepted (maintainer-directed, 2026-07-01). Revises the design's Concern 14b.
**No code.** Reshapes V6.2 (#149) into a *verification* task and informs V6.3 (#150).

> **This ADR reverses an earlier draft.** The first draft designed a reference-graph
> reactor (write-path enqueue over `citations` + a `stale_dependents` reverse lookup +
> coalescing + bounded fan-out + a sweep backstop), faithfully implementing the *written*
> Concern 14b. The maintainer revised the design: **propagation is topology-driven, not
> reference-graph-driven.** The reactor is rejected; its analysis is preserved in §6 as
> the road-not-taken (the point of an ADR).

---

## 1. Decision

**Propagation follows the parent→child spawn tree, one hop up.** A node's `write_output`
wakes its **single direct parent**; the parent re-reconciles its children and, if its own
output changes, wakes *its* parent — so a change ripples to the root hop by hop. The
**reference graph (`citations`/`file_index`) is provenance only** — a version-pinned audit
record — and is **never consulted to decide who to wake**.

This is the "child→parent output propagation" mechanism the design's Invariants section
already commits to (design:199), and it is **already wired end-to-end today**. V6 does not
build a new subsystem; it *verifies* the existing one at multi-level scale.

The maintainer's three governing choices (2026-07-01):
1. **Only wake the parent, not arbitrary referrers.** If the root cites a leaf directly,
   a leaf update should *not* wake the root — it wakes the leaf's parent, whose re-processed
   output propagates up. Wakes are hop-by-hop over topology, never long-range over citations.
2. **Always wake the parent on a child output** — unconditionally, not filtered on whether
   the parent currently cites that child.
3. **Let the woken parent decide for itself** whether anything material changed and move on.
   No DB staleness computation gates the wake.

---

## 2. What already exists (the mechanism is complete)

- **The wake fires.** On `write_output`, a child with a parent signals it `Trigger::ChildOutput`
  (workflow.rs:959) via `external_workflow(parent_wf).signal(external_signal, …)` — a
  **durable Temporal command**, not a lossy best-effort UDP-style send (the swallowed `Err`
  at workflow.rs:990 is essentially "parent not currently running", e.g. already retired).
- **The wake is visible to the decision layer.** Drained triggers flow
  `pending_triggers → DrainedBuckets.triggers` (workflow.rs:519) → `BuildSeedInput.triggers`
  (workflow.rs:822) → `agent_core::build_seed(&fs, triggers, …)` → `Seed.triggers`, and the
  prompt renders them (`render_triggers`, prompt.rs:93). The `ChildOutput` arm
  (prompt.rs:98-110) hands the model a **copy-paste instruction**: *"Child output: {name}
  emitted {id}. To fold it, pass this exact object in the `reconcile_children` `sources`
  array: {source}"*. So a woken parent both *sees* the child update and is *told how to fold it*.
- **The parent pulls current state.** `reconcile_children` reads the child's *current*
  output from the cross-agent FS (`sources: Vec<ReconcileSource>`, activities.rs:1067) and
  re-pins — so any wake self-corrects the parent to the child's current version.
- **Provenance stays consistent for free.** The parent re-pins on re-reconcile. If it decides
  nothing material changed and re-idles without re-emitting, its citation correctly still
  points at the version its *current* output actually rests on. No inconsistency.

**Net: the signal, the trigger-visibility, the fold instruction, and the pull-reconcile are
all present.** There is no reactive-subsystem gap to fill.

---

## 3. Why this is enough (and termination is free)

- **Termination is structural.** A node only ever wakes its *parent* (up the tree), the
  topology is a single-parent tree (`parent_handle: Option<ParentRef>`, workflow.rs:205 —
  exactly one parent), so a cascade is acyclic and bounded by tree depth, always ending at
  the root. No convergence machinery, no acyclicity guard, no depth counter needed.
- **No fan-out.** One parent per node ⇒ the wake is 1:1, never a storm. *(Narrowing
  assumption — see §7.)*
- **Coalescing is free.** A parent's `pending_triggers` drain-per-tick collapses a burst of
  child updates into one reconcile pass.
- **Steady-state churn is bounded and cheap.** A leaf micro-change can re-write the ancestor
  chain even when nothing materially changed; this is bounded by tree depth, and choice #3
  (the parent decides "nothing changed → idle, don't re-emit") cuts the cascade early. We
  accept this rather than build machinery to prevent it.

---

## 4. Reliability — honest tradeoff

The reliability model is **"the signal is durable; a miss self-heals on the node's next
write."** Stated precisely:

- **For recurring nodes** (any cadence that writes again, or whose own children re-poke it):
  fully covered. A dropped wake or a wake the parent chose not to act on is healed by the
  next write — a fresh `ChildOutput` re-wakes the parent, which pulls current state.
- **For an all-`never` node that writes exactly once:** there is **no next write** — the
  single signal+reconcile wave is the only shot. If that one signal doesn't land (parent
  retired at that instant) *or* the woken parent's decision layer declines to reconcile,
  that subtree's contribution can sit un-ingested until a human or an external trigger
  re-pokes the parent. This is not a rare-corner footnote: **the all-`never` graph is a
  named design AC** (design:172). We accept it, with these back-pocket options if a real
  graph strands (build none now):
  - **Seed-time safety net** — a parent does one idempotent reconcile of its children before
    its first idle. Closes the seed-time race with zero ongoing machinery.
  - **Low-frequency sweep** — the one piece of the rejected reactor worth keeping *in
    reserve*, added only if verification (§5) shows stranding.

---

## 5. What's verified vs unverified → this reshapes #149

**Verified/present:** the mechanism — durable signal, trigger-visibility, fold instruction,
pull-reconcile — end-to-end. Covered by existing 2-level, recurring-cadence, MockDecide
tests (`multi_agent`, `persistent_monitor_live` folds child outputs).

**Unverified:** whether a **multi-level (3+) all-`never` graph assembles its root output
end-to-end**, with a *real model* choosing to reconcile at each level. The existing tests are
2-level, recurring-cadence, and scripted — none exercises a real-model reconcile decision
propagating through multiple all-`never` hops.

**Therefore V6.2 (#149) is reshaped from "build the propagation subsystem" to a verification
task:**
> *Does a multi-level all-`never` graph assemble its root output end-to-end? Write the test;
> fix the mechanism if it's broken; add a safety net (§4) only if a real gap shows.*

This is the honest residual. The likely failure mode the test would expose is choice #3's
soft spot — a woken parent that *sees* the trigger but whose decision layer doesn't choose
`ReconcileChildren` (a prompt/mandate-guidance issue, not a plumbing one).

---

## 6. Road-not-taken — the rejected reference-graph reactor

Preserved because the rejection *is* the decision. The reactor would have:
- Extended `StructuralDbStore` with `stale_dependents(cited_agent, cited_path, current_sha)`
  (a reverse lookup via `GraphStore::citations_to`, store.rs:372, + a `citations`↔`agents`
  join for addressing), discovered dependents in an activity, and signalled each from the
  writing workflow with a `Trigger::DependencyStale`.
- Added coalescing/debounce and a bounded fan-out with spill-to-sweep.
- Run a periodic sweep comparing `file_index` current-sha vs `citations` pinned-sha as an
  at-least-once backstop.

**Rejected because**, on a single-parent tree, topology already carries propagation one-hop-up
and the reference graph reduces to a provenance record. The reactor's precision (waking exactly
who pinned a now-stale version) only pays off with **long-range** citations (root cites a leaf)
or **multi-parent** fan-out — and *neither arises in the current topology*: cross-agent
citations are minted only by `reconcile_children`, which cites a node's *direct* children, so
citations mirror the parent→child tree. The reactor solved a problem the topology doesn't create.

---

## 7. Narrowing assumption + revisit trigger

**"No fan-out / topology == reference graph" holds because `parent_handle` is singular today.**
If **multi-parent** is ever added (a node feeding several parents, or an agent citing a
non-child's output), then: fan-out and coalescing return, citations stop mirroring topology,
and topology-only propagation would miss long-range dependents. **This decision reopens** and
the §6 reactor becomes relevant again. Record this as the revisit trigger.

---

## 8. Handoff — issue reshape

- **#148 (this spike):** conclusion recorded here + in `design_realignment.md` (Concern 14b,
  the A1↔A4 "Stale shas" note, the target-shape propagation bullet, the open-items list).
  Close on maintainer approval.
- **#134 (V6 parent):** goal updated — propagation is topology-driven and largely built;
  V6 = verify + FS↔DB consistency.
- **#149 (was "propagation implementation", L):** reshaped to the **verification task** in §5
  (S/M) — write the multi-level all-`never` assembly test; fix if broken; safety net only if
  needed. Not a reactor build.
- **#150 (FS↔DB consistency, M):** unchanged and independent — largely landed already
  (`persist_output_impl` FS-first/DB-second, activities.rs:456; `commit_tick`, #160). Residual
  = the `filepath↔blob_sha` index-orphan reconciliation sweep (distinct from any propagation
  sweep).
