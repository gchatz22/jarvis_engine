# V1 #160 — Wire GitStorage (per-agent) into worker + coral apply

Parent: V1 #130 ("pure-content markdown FS, **git-versioned**"). Surfaced during C1 #142.

## Problem (confirmed by code read)
- Storage is a **single process-wide, prefix-keyed `Arc<dyn AgentStorage>`**: worker + apply
  both `install`/construct one `LocalStorage` at `AGENT_FS_ROOT`; `AgentFs` keys everything by
  `<prefix>/…` where `prefix = graphs/<g>/agents/<a>/`.
- `GitStorage` exists + is unit-tested (`VersionedStorage`: commit-per-tick / read-blob-at-sha),
  but is **single-repo-at-root** and **wired nowhere** — nothing in production calls `commit()`
  or `read_at()`. So the engine is not actually git-versioned.

## Design decisions
- **Topology = repo-per-agent** (design_realignment.md:83 `agents/<aid>/ ← a git repo`).
  Subtree-commits-in-one-repo is **out**: it needs a ref per agent, violating the
  no-branches invariant (design:688). Decided per issue ("Decide and implement the topology").
- **Cadence = commit-per-tick** ("commit = cycle", design:92/692, marked `(set)`). NOT
  per-write (would be 5–10 commits/cycle, wrecks cycle-granular time-scrubbing).
- **Design B (data plane untouched):** keep ONE shared `LocalStorage` at `<root>` with
  absolute keys (hot path + `list` cursors byte-identical), and add **prefix-scoped**
  commit/read_at that opens a git repo at `<root>/<prefix>` (reuse git.rs `commit_blocking`/
  `read_at_blocking`; lazy `open_or_init`). Avoids Design A's key/cursor translation risk.
  Do NOT change `VersionedStorage::commit(&self,msg)` (would break ~10 git.rs tests + the
  single-repo primitive).
- On-disk: `<root>/graphs/g/agents/a/{.git, mandate.md, notes/…}`. No `.git` at `<root>`.

## Split (like #147)
- **PR1 (this branch `160-pr1-per-agent-git-storage`):** substrate + wiring + storage tests.
  Versioning dormant (repos exist, working tree = files, reads unchanged) → non-regressive,
  satisfies all 4 literal acceptance criteria.
  1. `LocalStorage::list` skips `.git/` dirs in its recursive walk (required: a per-agent
     `.git` would otherwise surface as keys for any list at/above the agent root). + test.
  2. New `PerAgentGitStorage` (storage/per_agent_git.rs): data plane → shared `LocalStorage`
     at `<root>` (absolute keys); inherent `commit_agent(prefix,msg)` / `read_agent_at(prefix,sha)`
     → `<root>/<prefix>` repo. `git.rs` `commit_blocking`/`read_at_blocking`/`join_err` → `pub(crate)`.
  3. Worker: install `PerAgentGitStorage` instead of `LocalStorage`.
  4. apply binary: construct `PerAgentGitStorage`, materialize (unchanged, backend-agnostic),
     then commit each `start.input.fs_handle.prefix` as the tick-0 seed baseline.
  5. Tests: put+commit → blob sha; re-open idempotent; cross-agent reads resolve; `.git`
     never listed; data-plane round-trips.
- **PR2 (next):** cycle-boundary commit activity. Attaches after the inner loop breaks,
  before the `tick` bump (workflow.rs:624–628). Journaled activity, deterministic message
  `"tick {tick}"`, idempotent-on-retry via GitStorage clean-tree no-op. Delivers the goal +
  unblocks #147's retrieval half (read_at on pinned old shas).

## Verify
- Existing conformance suite + full `cargo test` (no LocalStorage regression from the `.git` filter).
- Confirm `<root>/<prefix>` LocalStorage writes and `<root>/<prefix>` git repo see the same
  working tree, no stray `.git` at `<root>`.
