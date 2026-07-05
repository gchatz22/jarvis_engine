# #160 PR2 — commit-per-tick (cycle-boundary git commit activity)

Branch: `160-pr2-commit-per-tick` (off `78dd857`, PR1 merged).

## Goal
Deliver the git-versioning the design calls load-bearing: **one commit per tick
("commit = cycle")**. PR1 made `PerAgentGitStorage` the production backend but
nothing commits during a run. PR2 attaches a journaled, deterministic, idempotent
commit activity at the cycle boundary so each completed cycle becomes a git commit
— creating the history `read_at(prefix, sha)` resolves (unblocks #147's retrieval
half).

## Design (decided; maintainer approved "generalize + delete GitStorage")
1. **Generalize `VersionedStorage` to prefix-scoped.** `commit(&self, agent_prefix,
   message)` + `read_at(&self, agent_prefix, sha)`. PerAgentGitStorage implements it
   (fold the inherent `commit_agent`/`read_agent_at` bodies in, rename). This makes
   `VersionedStorage` the single versioning trait the design names ("surface on the
   trait, one trait, scoped to {commit-per-tick, read-blob-at-sha}").
2. **Delete dead single-repo `GitStorage`.** It only compiled against the old
   single-repo signature; nothing in production uses it (PerAgentGitStorage reuses
   the *free fns* `commit_blocking`/`read_at_blocking`, not the struct). Remove the
   struct + both impls + its test module + `pub use git::GitStorage` + doc refs.
   Port the 3 still-valid edge-case tests (absent/malformed sha → None; commit
   stages deletions; read_at doesn't mutate HEAD/worktree) onto PerAgentGitStorage
   so deletion loses no coverage. Keep `git.rs` as the plumbing-free-fns module.
3. **Second OnceLock, Option-returning accessor.** `AGENT_VERSIONED_STORAGE:
   OnceLock<Arc<dyn VersionedStorage>>` + `install_versioned_storage` +
   `agent_versioned_storage_opt() -> Option<...>` (NO panic — hermetic tests install
   only MemoryStorage; the boundary commit must no-op when absent). Production
   (worker + apply) installs PerAgentGitStorage into BOTH OnceLocks (two Arcs, one
   object).
4. **The activity.** `CommitTickInput { fs_handle, tick }` →
   `AgentActivities::commit_tick` (auto-registers via `#[activities]`). Body:
   `match agent_versioned_storage_opt() { Some(s) => commit_tick_impl(s, prefix,
   tick), None => Ok(()) }`. `commit_tick_impl(storage, prefix, tick)` calls
   `storage.commit(prefix, &format!("tick {tick}"))` and returns `()`.
   - Deterministic message: `"tick {tick}"` (tick is workflow state, not wall-clock).
   - Returns `()`: references pin blob shas at WriteOutput time, not commit time —
     nothing from the commit needs to enter deterministic workflow state.
   - Idempotent on retry: clean-tree commit is a git no-op.
5. **Call site.** workflow.rs, AFTER the inner loop `break` (line ~623), BEFORE the
   tick bump (line ~628). Helper `commit_tick(ctx, &input.fs_handle, tick)`.
   - Fires on every cycle completion: Idle (terminal), runaway-fuse break,
     force-idle (CAN-too-large) break. The tree is always dirty at the boundary
     (`log_decision` writes `decisions/<tick>-<step>.jsonl` each step), so the
     no-op only happens on a true retry.
   - Does NOT fire on retire (retire short-circuits and returns before the loop;
     any prior cycle's work was already committed at its own boundary; only
     retirement.json goes uncommitted and nothing pins it). DELIBERATE — noted.
   - Does NOT fire on mid-cycle CAN suspend (line ~618 `continue_as_new` terminates
     the run before line 624). The resumed run commits once at the cycle's true
     completion. Structural, not hermetically forceable (consistent w/ existing
     CAN-suspend test limitations).

## Out of scope (explicit)
- Wiring `read_at` into `resolve_cited` (#147's retrieval half — read side). PR2
  ships the trait method + the write side (commit-per-tick history). The read-side
  consumer is the #147 follow-up.
- Retire-path commit (noted deliberate omission above).

## Tests
- **Hermetic, no server:** unit test of `commit_tick_impl` with a recording spy
  `VersionedStorage` → asserts it calls `commit(prefix, "tick 7")` (deterministic
  message). Plus the None-path no-op via the activity wrapper is covered by the
  existing MemoryStorage live tests (they schedule the activity; it no-ops).
- **Storage conformance:** PerAgentGitStorage trait-impl correctness already
  covered by PR1 (resolvable sha, clean-tree no-op, historical read_at). Re-home
  the 3 GitStorage edge-case tests.
- **Live (env-gated `TEMPORAL_LIVE_TEST=1`):** add history-count assertions to the
  existing `workflow_loop` tests — `run_live_test` (1 cycle → retire) asserts
  exactly 1 `commit_tick` schedule (proves cycle→commit, retire→no commit);
  `run_resume_test` (resume → completion → retire) asserts 1 `commit_tick`
  (proves resume→completion→commit). No new live harness; reuses existing infra.

## Verify
- `cargo build`; `cargo clippy -- -D warnings`; `cargo fmt --check`.
- `cargo test -p coral_node` (co-compiled) + workspace test.
- `grep -rn GitStorage crates/` → only git.rs (plumbing) + (no struct) after.
- Banned-token self-check (no `#160`/`GH-`/stage labels in source).
