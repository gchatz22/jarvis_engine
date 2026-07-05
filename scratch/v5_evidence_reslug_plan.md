# V5 closeout — #177 (evidence reslug, cite-by-path) + #178 (observation-returns-handle)

*Status: implementing. Maintainer chose **cite-by-slug-path** (AskUserQuestion 2026-06-28). Two stacked PRs
(#177 → #178), then close parent V5 #133. Advisor-pressure-tested.*

## Gating decision (maintainer, 2026-06-28)
The citation **handle** = the evidence file's **slug path**, not the content-sha `EvidenceId`.
- Resolution becomes identity (read the path) → the coral_graph→coral_node dep-direction problem
  dissolves (`fs.rs` never needs the DB to resolve a citation).
- `file_index` (path↔blob_sha, built in #137) gives the version pin directly.
- Matches Concern 9's 2026-06-13 decision ("interpretable filenames; hash in the DB index").
- Cost (accepted): reworks the citation contract PR2 (#173) just merged. Cross-CONTENT dedup degrades
  (two different seeds, same bytes → two files); RETRY dedup preserved (deterministic path + put_if_absent).

## Slug scheme (decided)
`evidence/<slug(claim_seed)>-<first 8 hex of EvidenceId>.json`
- Body = `fs::slug(claim_seed)` (interpretable, from the journaled `ToolCall.claim_seed`).
- Suffix = first 8 hex of `EvidenceId` (= sha256 of content) → **content-disambiguated**: different
  content ⇒ different path (no collision-detection logic); same content+seed ⇒ same path (idempotent).
  NOTE: this hashes CONTENT, unlike `claim_slug` which hashes the SEED. Do not reuse `claim_slug`.
- Extension stays `.json` (NOT `.md` as the issue title said): evidence is a structured record that
  `resolve_cited` parses as JSON, and the repo precedent for structured records is slug + `.json`
  (`claims/<claim_slug>.json`). The interpretability win is the slug, not the extension. ← note in PR.
- Reconcile synthetic evidence (no claim_seed): seed = deterministic label, e.g.
  `slug(format!("reconcile {child_workflow_id}"))` + `-<id[..8]>`.

## PR1 = #177 — reslug + cite-by-path (structural)
Branch: `177-evidence-reslug-cite-by-path` off main.

### coral_node
- `fs.rs`:
  - `record_evidence(record, slug_seed: &str) -> anyhow::Result<String>` — returns the **path** (handle).
    filename = `evidence/{slug(slug_seed)}-{id[..8]}.json` (empty slug body → just `{id[..8]}`).
    Keep `put_if_absent` + tail append (tail stores the filename, already does).
  - `evidence_must_exist` / `read_evidence_bytes`: key by **path** (validate `evidence/` prefix, no `..`),
    not by id. Rename to `*_at(path)` or keep names with `&str` path param.
  - `persist_output(body, citations: &[String])`: gate = each citation path is under `evidence/` AND
    resolves; reject empty / unresolved / out-of-scope. Keep `FsError::EmptyEvidence`; add/repurpose a
    variant for unresolved/out-of-scope citation path.
  - `evidence_key(id)` removed (path is now first-class).
- `decision.rs`: `WriteOutput { body, citations: Vec<String> }` (paths). Doc updates. `ClaimSeed` unchanged.
- `evidence.rs`: `EvidenceId` STAYS (content fingerprint → the shorthash + future DB dedup). `EvidenceRecord`
  unchanged (no new field — don't perturb the content hash).
- `decide_llm/schema.rs`: `write_output` def — `citations` = "evidence file paths (from your tool/reconcile
  observations), each under evidence/". `decide_llm/system_prompt.md`: cite-by-path wording.
- `agent_core.rs`: `execute_call_tools` threads `&call.claim_seed.0` into record_evidence; collects the
  returned paths (used by #178). Observation string unchanged in PR1 (paths surfaced in PR2/#178).

### coral_temporal
- `activities.rs`:
  - `ToolCallOutcome::Success { evidence_path: String }` (was `evidence_id`).
  - `execute_tool`: pass `input.call.claim_seed` into record_evidence; return the path.
  - `reconcile_children_impl`: `synthetic_evidence: Vec<String>` (paths); slug seed = reconcile label;
    keep pinning `child_output_blob_sha` in args (unchanged).
  - `resolve_cited(fs, self_agent, path: &str)`: read by path; parse; reconcile branch unchanged
    (redirect to child@pinned sha); self branch → (self, path, of_bytes(bytes)).
  - `persist_output_impl`: citations are paths; `add_citation(.., cited_path=path, ..)`.
  - `ReconcileChildrenOutput.synthetic_evidence: Vec<String>`; fix doc at ~291 (will fully de-hack in #178).
- `workflow.rs`: thread paths through `dispatch_call_tools` / `reconcile_children`; observation strings
  unchanged in PR1 (just compile-correct), enriched in #178.

### Tests (PR1)
- evidence lands at `evidence/<slug>-<hash>.json`; same content+seed → 1 file (idempotent); different
  content same seed → 2 files (content-disambiguated).
- citation to a slug path resolves; empty / non-evidence-prefix / missing path → NeedsCorrection.
- reconcile writes synthetic evidence at slug path; cross-agent edge still pins child@fold-sha (V6 equality).
- update loop_smoke, llm fixtures, prompt snapshots, temporal integration (workflow_loop,
  reconcile_children_live, workflow_smoke, child_parent_signal), coral_apply for the new citations type.

## PR2 = #178 — observation-returns-handle (additive)
Branch stacked on PR1.
- `dispatch_call_tools` (workflow.rs) + `execute_call_tools` (agent_core.rs): observation string lists the
  minted evidence **paths** so the model can cite them same-cycle.
- `reconcile_children` (workflow.rs): observation lists `synthetic_evidence` paths.
- Remove the `List{evidence/}` instruction from the `reconcile_children` doc (activities.rs ~291) and any
  prompt text; the handle now arrives in the observation.
- Tests: a CallTools→WriteOutput citing the returned path resolves; a reconcile→WriteOutput citing the
  returned synthetic path resolves; neither needs a List/Read of evidence/. Assert the observation content
  contains the minted path(s).

## Close parent V5 #133 after both merge.

## Constraints (standing)
- Replay-deterministic filenames (slug from journaled Decision; content shorthash from journaled result).
- No issue-ids / stage-labels in source comments.
- Smallest correct diff; tests ship with each PR.
- Stage only crates/ + examples/ + infra; never `git add -A`; never commit scratch/.
