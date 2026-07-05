# V5.3 #147 — write_output provenance: { body → FS, citations → DB }

*Status: implementing. Re-consult done (advisor + maintainer 2026-06-28). Authoritative working plan.*

## Maintainer decisions (2026-06-28, via AskUserQuestion)
1. **Cross-agent edges are IN #147** (not deferred to V6). The parent's consolidated output must
   cite the CHILD's output file directly (cited_agent = child, version-pinned to the child output's
   blob sha) — fixes the V5 reconcile synthetic-evidence self-edge hack; makes #147 the genuine V6
   prerequisite. → lands in **PR2**.
2. **One canonical Output per agent.** Each agent keeps ONE Output at a stable, runtime-controlled
   path, overwritten each cycle (NOT today's many accumulating content-addressed files). Matches the
   system prompt ("single deliverable, kept current"). → **PR1**.

## Why reslug-first, and why the OUTPUT path is the crux (advisor-confirmed)
V6.2 staleness = compare `file_index`'s *current* blob sha for a path against the *pinned* blob sha
in a citation. That comparison is only non-trivial on a path whose current sha can **change** — a
stable path that gets overwritten. Content-addressing never overwrites a path, so a citations-first
PR would pin to immutable paths nothing can ever make stale → V6 can't fire. **The stable path IS the
mechanism.** And a slug derived from the output's *content/title* would drift each cycle =
content-addressing under a prettier name. So the canonical output path must be **runtime-controlled +
fixed**: `outputs/output.md`, one per agent (the agent's directory already namespaces it = pinned to
the mandate's identity by construction). Lookup-free: a parent reads `<child-prefix>/outputs/output.md`
directly — no id→path map needed.

## Locked design decisions (mine, advisor-endorsed)
- **Git-flavored blob sha, computed from bytes** (PR2): `BlobSha::of_bytes` = `sha1("blob "+len+"\0"+bytes)`
  (what `git hash-object` produces). NOT the existing sha256 OutputId/EvidenceId — else #160/#150 see
  phantom mismatches when GitStorage lands. Guard test: `GitStorage.commit()`'s sha == `BlobSha::of_bytes`,
  with `core.autocrlf=false`, no `.gitattributes` filters. Sidesteps the #160 dependency entirely.
- **Validate+hash in coral_node, write-DB in the activity** (PR2): `persist_output` keeps the
  evidence-resolves rejection + computes shas (it has the bytes), returns a structured result; the
  Temporal activity writes `file_index` + `citations` via the extended `StructuralDbStore` port.
  Respects the coral_graph→coral_node dep direction (fs.rs can't reach the DB). Makes #150's "one
  idempotent activity" a clean later wrap.
- **2 PRs, split by layer** (no rework): PR1 = FS surface (no DB), PR2 = DB reference graph.

---

## PR1 — Canonical kept-current Output (FS surface, no DB)
**Branch:** `147-pr1-canonical-output`

### Scope
1. Rename `Decision::EmitOutput { content, evidence }` → `WriteOutput { body, citations }`
   (`citations: Vec<EvidenceId>`, still cited-by-id, still FS-resolved). Tool name `emit_output`→`write_output`.
2. **Canonical output**: `fs.persist_output` writes ONE file `outputs/output.md` (pure prose = body),
   **overwrite** (PUT not put_if_absent), no embedded citations/id/created_at (content-only in FS, A1).
   Still returns an `OutputId` (hash of body) for the `ChildOutput` trigger/signal.
3. **Reconcile reads the child's canonical path** `outputs/output.md` (not `read_output(<id>.json)`).
   Keep `ReconcileSource { child_ref, output_id }` shape for now (output_id becomes informational;
   repurposed/pinned in PR2) to minimize model-facing schema churn.
4. **Rejection stays FS-side**: each `citation` resolves to a real `evidence/<id>.json` (unchanged
   mechanism, renamed). Empty-citations + unresolved-citation → NeedsCorrection.
5. **Seed index**: outputs bucket now = the single canonical output. `recent_output_filenames` /
   outputs `_tail.json` / `outputs_more` simplify (≤1 output). Do NOT touch the notes bucket machinery
   (#169/#170) beyond what the outputs change forces.
6. **System prompt + snapshots**: `system_prompt.md` rule 3 + "What a good Output is" + `prompt.rs`
   snapshots/lead-phrases: `emit_output`→`write_output`.

### Known breakage to fix (from code map)
- `mandate.rs`: `Output { id, content, evidence }` → output FILE is pure prose; `OutputId::new`
  (keep hashing body for the trigger id; drop evidence from the *file*, decide id hash input).
- `fs.rs`: `persist_output` (overwrite canonical path, pure prose, blob-sha return deferred to PR2),
  `read_output` (canonical path), `recent_output_filenames`/outputs tail, `FsError` variants kept.
- `agent_core.rs`: `WriteOutput` dispatch arm; `execute_call_tools` observation unchanged in PR1.
- `decision.rs` + `schema.rs`: variant + tool def + required fields + parse tests.
- `activities.rs`/`workflow.rs`: `persist_output` activity + `emit_output` workflow fn + `reconcile_children_impl`
  (read canonical child path). `ChildOutput { output_id }` trigger kept.
- Tests: loop_smoke, llm fixtures, prompt snapshots, temporal integration (multi_agent, persistent_monitor,
  workflow_smoke, reconcile_children_live, coral_apply).

### Tests (PR1)
- output overwritten at `outputs/output.md` across two writes (stable path; second write replaces first).
- output file is pure prose (no `{`/id/evidence embedded).
- write_output with unresolved citation → NeedsCorrection; with resolved → persists.
- reconcile reads the child's canonical output.
- seed index surfaces the canonical output.

---

## PR2 — Version-pinned citation reference graph (DB layer)
**Branch:** stacked on PR1 (or off main after PR1 merges).

### Scope
1. `BlobSha::of_bytes` (git blob sha) + guard test vs `GitStorage.commit()`.
2. Extend `StructuralDbStore` (worker.rs) with `set_file_version(agent, path, blob_sha)` +
   `add_citation(citing.., cited..)`; `GraphStore` already implements the underlying store fns —
   add to the trait + the worker test-double.
3. `persist_output` activity: after FS persist, compute the output's git blob sha, upsert
   `file_index(agent, outputs/output.md, sha)`, and for each citation resolve cited side:
   - read own `evidence/<id>` record; if `tool == "reconcile"` → cited = (child_agent_id from args,
     child's `outputs/output.md`, **child_output_blob_sha** captured at fold time); else → cited =
     (self, `evidence/<id>`, blob sha of the evidence bytes). Write one version-pinned `citation` edge each.
   - citing_agent_id parsed from `fs_handle.prefix`.
4. **Reconcile reshape (cross-agent)**: `reconcile_children_impl` computes the child output's git blob
   sha at fold time and stores `child_output_blob_sha` in the synthetic evidence `args` (so the pin
   captures the version cited). The cited edge then points to the CHILD's output (not parent self-evidence).
5. **Observation-returns-handle**: `execute_call_tools` + reconcile observations return the minted
   evidence ids so the model can cite them same-cycle (fixes the V5 `List{evidence/}` hack).
6. **Evidence reslug** (cosmetic — evidence is immutable so not V6-load-bearing): `evidence/<sha>.json`
   → `evidence/<claim_slug>.md` via the `slug`/`claim_slug` helper; citation cited_path uses the slug path.
7. Version-pinning becomes load-bearing: old citations stay pinned to the sha they cited; auto-follow
   rejected. Tests: cross-agent edge points to child@sha; child re-emit → file_index current sha changes
   while old citation stays pinned (the V6-detectable staleness); unresolved citation rejected.

### Tests (PR2) — need Postgres (`#[sqlx::test]` / live temporal suite)
- citation row written with correct git blob shas (both ends).
- cross-agent reconcile edge: cited_agent = child, cited_blob_sha = child output's fold-time sha.
- file_index upserted on each output write; same path, new sha after re-emit.
- guard: `BlobSha::of_bytes(content)` == `GitStorage.commit()` sha.

---

## Out of scope / deferred
- Span-level "which sentence cites which evidence" (needs DB anchors) — design defers it.
- The propagation/wake subsystem itself = V6 (#148/#149). #147 only POPULATES the graph V6 walks.

## ⚠️ #160 (GitStorage) dependency — split by half (advisor-caught 2026-06-28)
The "one canonical Output, overwritten" model means LocalStorage (`put` overwrites; no `read_at`)
**physically discards old output bytes** on each refresh. So #160 is needed for *part* of PR2, not none:
- **Blob-sha COMPUTATION from bytes** — no git. ✓ (`BlobSha::of_bytes`).
- **V6 staleness→wake** — compares pinned `sha_c` vs `file_index` current sha = DB-metadata only, no bytes
  retrieved. ✓ So "#147 unblocks V6" survives without git.
- **Provenance RETENTION / RETRIEVAL / time-scrubbing** ("an old output stays pinned to the versions it
  cited" — #147's own AC + 14a's purpose) — needs the OLD bytes, which canonical overwrite destroys on
  LocalStorage. ✗ This needs **#160 (GitStorage retention)**: git keeps every committed blob, so a pinned
  sha stays resolvable after the child refreshes its canonical output.
**Consequence for PR2:** the cross-agent edge rows + V6 detectability land on LocalStorage today; but the
"resolve a pinned earlier version" guarantee (and its tests) sequence with/after #160. PR2 will write the
edges + flag that retrieval/scrubbing is git-gated, OR #160 lands first. Decide at PR2 with the maintainer.
(Evidence files stay content-addressed + retained, so within-agent evidence retrieval is fine — it is
specifically the now-overwritten OUTPUT whose history is gone without git.)
- #150 dual-write (git+DB idempotent activity) hardening is separate (V6.3).

## PR1 STATUS: COMPLETE + GREEN (2026-06-28)
Verified: workspace build + clippy `-D warnings` (anthropic all-targets; coral_node+graph cohere
all-targets) + fmt --check clean. Tests: coral_node lib 376 (anthropic) / 356 (cohere), loop_smoke 21,
llm fixtures 18+4, compute-evidence-id bin 3, coral_temporal lib 88 + integration. coral_graph 35 lib + 1
round_trip = pre-existing Postgres-gated (`DATABASE_URL`) environmental, untouched files. Holding for
maintainer review before PR2.

### Flagged out-of-scope (not fixed in PR1):
- `ContextPolicy` (mandate.rs) is vestigial post-V5 (used only within mandate.rs; the ContextBundle
  assembly path it configured was deleted). Its doc still references the removed `list_recent_outputs`.
  Candidate for wholesale removal in a separate cleanup — NOT touched (no drive-by refactor).
- PR2 note: reconcile records `source_output_id` = the SIGNALED version but reads/folds the CURRENT
  canonical body; when PR2 pins, pin the sha of the body actually READ (the current version) for
  consistency, and reconcile the minor signaled-vs-read mismatch.

## PR2 FINALIZED DECISIONS (2026-06-28, advisor ×2 + code-surface map)
PR1 (#172) MERGED. Branch `147-pr2-reference-graph` off main. Key map findings:
- **V4.2 (#137) already built the whole DB layer**: `GraphStore::{set_file_version, get_file_blob_sha,
  add_citation, citations_from/to, find_paths_by_blob_sha}` + migrations `file_index`/`citations` exist
  and are **idempotent** (set_file_version=upsert; add_citation=ON CONFLICT DO UPDATE no-op, append-only/
  retained for time-scrub). So PR2 is WIRING, not new schema.
- **`BlobSha` exists but has no compute-from-bytes**: add `BlobSha::of_bytes` via
  `git2::Oid::hash_object(ObjectType::Blob, bytes)` (pure, no repo; literally libgit2 → guaranteed to
  match `GitStorage.commit()`). Guard test = commit a file, assert manifest sha == of_bytes.
- **DB-from-activity seam already exists**: `STRUCTURAL_DB: OnceLock` + `structural_db_store()` (panics if
  absent) + the `_impl(store,...)`-takes-param / wrapper-reads-global pattern (spawn already uses it).
- **Dep constraint**: coral_temporal does NOT depend on coral_graph → extended `StructuralDbStore` trait
  methods use only coral_node types (`AgentId`, `BlobSha`), return `anyhow::Result<()>` (mirror add_edge);
  GraphStore impl discards the returned FileIndexEntry/Citation.

### Seam = HARD-PANIC (advisor-confirmed, NOT soft-skip)
Soft-skip = the A1 "softening rejected by maintainer 2026-06-22" (design doc:947); also asymmetric with
register_child (hard-requires DB). So persist_output activity wrapper hard-requires `structural_db_store()`.
- `persist_output_impl(storage, db, agent_id, prefix, body, citations)` — db + agent_id are params;
  ALWAYS writes file_index + citations. Hermetic tests inject `MemoryStructuralDbStore`. This is where the
  DB behavior is TESTED.
- One activity, FS-then-DB, both idempotent (#14c).
- Citation resolution per id: read `evidence/<id>.json` raw bytes (new `AgentFs::read_evidence_bytes`);
  if `tool=="reconcile"` → cited=(child_agent_id from args, `outputs/output.md`, child_output_blob_sha
  from args); else self-evidence → cited=(self, `evidence/<id>.json`, of_bytes(evidence bytes)).
  citing always =(self, `outputs/output.md`, of_bytes(body)).
- **Reconcile reshape**: `reconcile_children_impl` pins `child_output_blob_sha = of_bytes(read body)` into
  the synthetic-evidence args = **pin-what-you-read** (source_output_id stays informational).

### V6 equality (advisor — assert it, don't trust it)
Parent's pinned `child_output_blob_sha` (of_bytes of body read at fold time) MUST equal the child's own
`file_index` current sha for `outputs/output.md`. Same bytes → same sha; if it ever drifts, staleness
never fires. Hermetic `_impl` test asserts: child file_index sha == parent citation cited_sha == of_bytes(body).

### Test surface (verified)
- Default `cargo test` only hits `persist_output_impl` (hermetic) → inject the fake. ✅ CI green.
- Live tests that run the wrapper + WriteOutput but DON'T install a DB: **workflow_loop,
  child_parent_signal, workflow_smoke, reconcile_children_live** (4 files, coral_temporal/tests) →
  shared `tests/common/mod.rs` in-memory fake (FK-lenient) + 1-line install in each existing INIT block.
- persistent_monitor_live + mcp_graph_live already install a real GraphStore AND seed agents via
  `coral apply` (`applied.agents` / `root.db_agent_id`) → FK-safe, NO change.
- Make `fs::CANONICAL_OUTPUT` pub (avoid magic-string drift between fs.rs + activities.rs).

### PR2 explicitly DEFERS (out of #147 DB ACs; smaller diff)
- Observation-returns-handle (return minted evidence ids from execute_call_tools) — adjacent ergonomic
  debt, separate PR.
- Evidence reslug (`evidence/<sha>.json` → `<slug>.md`) — cosmetic, immutable, not V6-load-bearing.
- Retrieval-at-old-sha (the #160-gated half): write+detect only; no read_at path. Retention test ships
  `#[ignore]`, one line to flip once #160 lands.

## PR2 STATUS: COMPLETE + GREEN (2026-06-28)
Branch `147-pr2-reference-graph` off main. What landed:
- `BlobSha::of_bytes` (storage/mod.rs) via `git2::Oid::hash_object`; guard test
  `of_bytes_matches_committed_blob_sha` (git.rs) proves it == `GitStorage.commit()` sha. ✅
- `AgentFs::read_evidence_bytes` + `pub CANONICAL_OUTPUT` (fs.rs).
- `StructuralDbStore` trait += `set_file_version` + `add_citation` (anyhow::Result<()>, AgentId/BlobSha
  only — no coral_graph types); impls: GraphStore (delegates+discards row), PanicStructuralDbStore,
  hermetic MemoryStructuralDbStore (records + `current_sha`/`citations` accessors), 2 inline live fakes
  (spawn_child_live, lifecycle_ops_live), shared `tests/common/mod.rs` NoopStructuralDb.
- `persist_output_impl(storage, db, agent_id, prefix, body, citations)` = FS-then-DB; `resolve_cited`
  branches reconcile (cross-agent, child@fold-sha) vs self-evidence (within-agent, evidence@own-sha).
  Wrapper hard-requires `structural_db_store()`. `PersistOutputInput.agent_id` added; workflow threads it.
- reconcile pins `child_output_blob_sha = of_bytes(read body)` into synthetic-evidence args (pin-what-you-read).
- 4 live tests (workflow_loop/child_parent_signal/workflow_smoke/reconcile_children_live) install
  NoopStructuralDb in their INIT blocks. persistent_monitor_live + mcp_graph_live unchanged (real
  GraphStore + agents seeded via apply = FK-safe).
- New hermetic test `reconcile_then_persist_writes_cross_agent_edge_pinned_to_child_version`: asserts
  cross-agent edge cites child canonical@fold-sha; **V6 equality** (pin == child file_index == of_bytes);
  pin retained when child refreshes (staleness detectable). Plus file_index+self-citation rows in test 1,
  FS-gate-before-DB in test 2.

Verify: default workspace check ✅; clippy `-D warnings` --all-targets anthropic (workspace) + cohere
(coral_node+graph) ✅; fmt --check ✅. coral_node 352(anthropic)/357(cohere) incl. guard; coral_temporal
90 incl. cross-agent; loop_smoke 21; llm fixtures 4+1ign; bins 3/19/18; temporal_smoke 1; all 6 live
test files compile + hermetic-pass. coral_graph = pre-existing Postgres-gated (DATABASE_URL unset) only.

### Deferred to #160 (retrieval-of-bytes-at-pinned-sha) — NOT an #[ignore] stub here
PR2 tests DETECTION (pin vs current sha differ) + RETENTION (old pin kept in DB). The end-to-end
"resolve a pinned OLD output's bytes" needs commit-per-tick on GitStorage (#160/#14c) — there is no
commit path in PR2, so it is NOT one-line-flippable and belongs in #160's PR. GitStorage's read_at
historical-resolve is already covered by `read_at_resolves_historical_version_after_overwrite` (git.rs).

## Re-consult / gate
Per-issue gate satisfied (advisor ×3 + maintainer ×2 forks). Stop after each PR for maintainer review.
