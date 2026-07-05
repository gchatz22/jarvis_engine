# Seed Context Paradigm: FsIndex vs. Session Diary

*Status: design exploration. Prompted by PR #165 review discussion.*

---

## Problem Statement

PR #165 introduces a thin `Seed` that wakes the agent with:

```rust
pub struct Seed {
    pub mandate: Mandate,
    pub triggers: Vec<Trigger>,
    pub index: FsIndex,  // Filenames only: last 32 notes, last 32 outputs
}
```

**The tension:** When a long-running agent (continuous monitor, multi-day research task) wakes after idle, should it:

1. **FsIndex approach (current):** Get filenames of recent work, then dynamically fetch what it needs?
2. **Session Diary approach (proposed):** Get an explicit summary the agent maintains, ready to reason from?

This doc analyzes both paradigms and proposes a hybrid evolution path.

---

## Current Paradigm: FsIndex (32 pointers)

### Implementation

The seed includes filename pointers to recent notes and outputs:

```rust
pub struct FsIndex {
    pub notes: Vec<String>,      // ~32 most recent note filenames, reverse chrono
    pub outputs: Vec<String>,    // ~32 most recent output filenames, reverse chrono
}
```

Built in `agent_core::build_seed`:

```rust
let mut notes = fs.list_dir("notes/").await?;
if notes.len() > SEED_INDEX_NOTES {  // SEED_INDEX_NOTES = 32
    let start = notes.len() - SEED_INDEX_NOTES;
    notes.drain(..start);  // Keep only the tail
}
```

### Strengths

- **Full agency:** Agent chooses what to read. No forced summarization work.
- **Transparency:** All intermediate thinking preserved (breadcrumb trail visible).
- **Simple:** Implemented as `list_dir` + tail slice; no extra synthesis required.
- **Flexible:** Agent can read any note it wants via `Read { path }`.
- **Discoverable:** `List { path: "notes/" }` reveals full history if agent wants it.

### Weaknesses

- **Cold-start expensive:** Agent wakes up. "What was I doing 24 hours ago?" → Must `Read` outputs or notes to reconstruct context.
- **Arbitrary limit:** 32-file cap means older notes disappear from seed. A 2-month monitor with ~50 notes loses everything older than ~1 month.
- **Lost cross-cycle baselines:** For continuous monitors, "yesterday I found X; did it change?" requires re-reading yesterday's output (extra step).
- **Requires discovery work:** Each wake-up, agent must explore its own state before reasoning on new work.
- **Scaling question:** What happens at 6+ months (180+ notes)? The seed still shows only the last 32, but the agent needs to compare trends over time.

---

## Proposed Paradigm: Session Diary

### Concept

At the end of each cycle, the agent writes an **explicit summary** of its cognitive state:

```markdown
# SESSION_SUMMARY.md (written at cycle end)

## Last Cycle Conclusions
- TSMC CoWoS capacity: 85% (web_search_ev_001, reconcile_ev_002)
- Supply chain resilience: 6-month forward visibility confirmed
- GPU shortage secondary effects: not yet quantified
- Key sources: TSMC earnings call (2026-Q2), industry analyst reports

## State: Investigation Phase
Currently investigating: secondary effects on chiplet availability
Pending: quantitative model of GPU shortage cascade

## Next Cycle Hypothesis
If secondary effects are significant, we should model demand destruction.
Cross-check against historical parallel cases.
```

**Seed now includes the summary:**

```rust
pub struct Seed {
    pub mandate: Mandate,
    pub triggers: Vec<Trigger>,
    pub session_summary: Option<String>,  // Full content of SESSION_SUMMARY.md if it exists
}
```

On next wake-up, agent reads:

```
"Last Cycle Conclusions:
- TSMC CoWoS capacity: 85% ...
- Supply chain resilience: 6-month ...
State: Investigation Phase
Currently investigating: secondary effects ...
Next Cycle Hypothesis: If secondary effects ..."
```

### Strengths

- **O(1) warm-start:** Full context in seed. Agent doesn't need a discovery phase.
- **Explicit state handoff:** Agent declares "here's my thinking state" rather than having to infer it from scattered files.
- **Cross-cycle comparison:** "Comparing to yesterday's 85%, did capacity change?" is a direct read, not an extra step.
- **Scales infinitely:** One summary per cycle, regardless of how many notes exist. A 1-year monitor has 365 summaries, not 12,000 notes.
- **Forcing function for synthesis:** Agent must decide what matters. This is epistemic discipline.
- **Aligns with research practice:** Researchers keep lab notebooks with "current status" sections.
- **Supports state machines:** Agent can declare "I'm in phase X" explicitly (investigating → validating → monitoring).
- **VISION-aligned:** This IS "state as files... the way a human works in a filesystem."

### Weaknesses

- **Extra write at cycle end:** One more artifact to persist (negligible cost).
- **Requires agent discipline:** Agent must remember to write the summary before `Idle`.
- **Summary staleness risk:** If the summary is wrong/stale, that's the only context next cycle has.
- **Loses intermediate thinking:** Agent can't easily re-browse its own reasoning steps unless it explicitly includes them in the summary.
- **One more artifact:** Adds to the per-agent FS surface.

---

## Comparative Analysis

### Use Case 1: Continuous Monitor (CM motivation in design_realignment.md)

**Scenario:** Agent checks supply chain health daily.

**Cycle 1:**
- Task: "Check TSMC capacity"
- Cycle output: "TSMC CoWoS at 85%"
- Writes: `SESSION_SUMMARY.md` with conclusion + source citations

**Cycle 2 (next day):**

| Approach | Warmup | First Step | Total Steps to Answer |
|---|---|---|---|
| **FsIndex** | Agent sees ["SESSION_SUMMARY.md", ...] filenames | `Read { path: "SESSION_SUMMARY.md" }` to recall yesterday | 1 + reasoning steps |
| **Diary** | Seed contains full summary | (None — summary already in seed) | 0 + reasoning steps |

**Winner:** Diary. Saves one navigation step per wake-up. Over 365 cycles, that's 365 fewer steps.

---

### Use Case 2: Multi-day Research Task

**Scenario:** Agent explores 15 research angles, needs to consolidate findings.

**Day 1:** Explores angles A–O, writes 15 notes (`thought_001.md` → `thought_015.md`), emits output.

**Day 2:**
- Mandate: "Synthesize findings; identify top 3 angles for deep dive."

| Approach | Warmup | First Steps | Discovery Overhead |
|---|---|---|---|
| **FsIndex** | Agent sees ["thought_015.md", ..., "thought_001.md"] (oldest missing) | `List { path: "notes/" }` to see all 15 | Must re-read 15 notes to remember what each was about |
| **Diary** | Seed: "Day 1 explored 15 angles. Top candidates: angle_C (see thought_003.md), angle_G (thought_007.md), angle_M (thought_013.md). Rationale: ..." | (None — summary is strategic) | Zero; summary already identifies the 3 to deep-dive |

**Winner:** Diary. Discovery work moves from runtime (agent reconnoiters) to synthesis (agent reflects at cycle end).

---

### Use Case 3: 6+ Month Monitor

**Scenario:** Agent monitors supply chain continuously over a year.

**After 180 cycles:**

| Approach | Seed Size | Discoverability | Trend Analysis |
|---|---|---|---|
| **FsIndex** | 32 filenames (O(32)) | Can access last month; older files require manual pagination | To compare Q1 vs Q2 trends, must manually read Q1 summaries |
| **Diary** | 1 current summary (O(1)) | Summary can include "vs 6 months ago" comparisons | Trend data baked into current summary |

**Winner:** Diary. As the monitor runs longer, FsIndex becomes increasingly stale while the diary pattern scales.

---

### Use Case 4: Agent Debuggability

**Scenario:** An agent produced a wrong output. We want to understand why.

| Approach | Audit Trail | Reconstruction |
|---|---|---|
| **FsIndex** | All 180 days of notes preserved; full decision log | Can replay exact thinking but must read 180 files to reconstruct state |
| **Diary** | All 180 days of summaries + decision logs | Can see state transitions over time via summaries; notes are reference details |

**Winner:** Diary (for understanding), FsIndex (for completeness). Hybrid needed.

---

## Three-Phase Evolution

### Phase 1 (Now - PR #165): Minimal, Voluntary Summaries

**What ships:**

```rust
pub struct Seed {
    pub mandate: Mandate,
    pub triggers: Vec<Trigger>,
    pub index: FsIndex,  // Current implementation: 32 filenames
}
```

**Addition (low-friction):**

- No changes to `Seed` struct
- No requirement for agents to write summaries
- Agents can *voluntarily* write `notes/SESSION_SUMMARY.md` if they want
- Prompt (in LLM decide) can suggest: "Consider writing `notes/SESSION_SUMMARY.md` before sleeping to accelerate your next wake-up."

**Why:**
- ✅ PR #165 ships unchanged
- ✅ No forced behavior (agent-driven)
- ✅ Enables observational data (do agents naturally want to summarize?)
- ✅ Zero complexity overhead

---

### Phase 2 (After Real Model Run): Observe Patterns

**Run the NVIDIA multi-cycle evaluation** and collect data:

1. **Do agents naturally write summaries?**
   - If no: FsIndex is fine, design is complete.
   - If yes: Summarization is a real human-like behavior.

2. **Is warm-start latency a problem?**
   - Do agents spend steps doing discovery (List, Read summary notes)?
   - Or do they jump straight into new work?

3. **Do agents need history?**
   - Do they compare to prior cycles?
   - Do they re-read old conclusions?
   - Or is the most recent 32 enough?

4. **Scalability questions:**
   - Does the 32-file limit cause problems?
   - Do agents try to read "older" notes and fail?

**Outcome:** Inform Phase 3 design with real usage data.

---

### Phase 3 (Future Design Refinement): Formalize the Pattern

If Phase 2 shows summaries are valuable:

```rust
pub enum SeedContext {
    Summary(String),      // Full SESSION_SUMMARY.md if it exists
    Index(FsIndex),       // Fallback: 32 filenames if no summary
}

pub struct Seed {
    pub mandate: Mandate,
    pub triggers: Vec<Trigger>,
    pub context: SeedContext,
}
```

**Seed builder logic:**

```rust
pub async fn build_seed(fs: &AgentFs, triggers: Vec<Trigger>, cfg: &Mandate) -> Result<Seed> {
    // Check for explicit summary
    let context = match fs.read_file("notes/SESSION_SUMMARY.md").await {
        Ok(summary) => SeedContext::Summary(summary),
        Err(_) => {
            // Fallback to FsIndex
            let notes = /* ... get 32 recent notes */;
            let outputs = /* ... get 32 recent outputs */;
            SeedContext::Index(FsIndex { notes, outputs })
        }
    };
    
    Ok(Seed {
        mandate: cfg.clone(),
        triggers,
        context,
    })
}
```

**Prompt guidance:** Suggest to agent that writing a summary accelerates next wake-up (now with real evidence from Phase 2 data).

---

## Recommendation

### For PR #165 (Immediate)

**No changes.** FsIndex is the right stepping stone:
- Simple to implement
- Doesn't prescribe behavior
- Ships the ReAct loop without extra design assumptions

### For the NVIDIA Run (Next)

**Add voluntary summary support:**

1. In the decide prompt, include a suggestion:
   ```
   Pro tip: Before you call Idle, consider writing notes/SESSION_SUMMARY.md
   with your key conclusions and current state. This will accelerate your
   next wake-up, allowing you to jump straight into new work.
   ```

2. Don't require it. Let agents decide if it's useful.

3. Collect telemetry:
   - Do agents write summaries?
   - What do they include?
   - Do warm-starts feel slow?

### For Phase 2→3 (Post-NVIDIA)

If real usage shows summaries are valuable:

1. Update `Seed` to prefer `SESSION_SUMMARY.md` if it exists
2. Formalize the pattern in the prompt
3. Update design_realignment.md with the learned behavior

---

## Design Alignment

### With VISION.md

> "State as files… the way a human works in a filesystem."

**Diary approach aligns better:** Researchers maintain an explicit "current status" section in their lab notebooks. It's how humans actually manage cognitive state.

### With design_realignment.md §8 (Pull-Navigation + Agency)

**Current FsIndex:**
- ✅ Agent has agency (decides what to read)
- ⚠️ Warm-start requires discovery work

**Diary approach:**
- ✅ Agent has agency (decides what to include in summary)
- ✅ Warm-start is O(1)
- ✅ Forcing function for synthesis (epistemic discipline)

### With design_realignment.md Target Shape (Legibility)

**FsIndex:** Discoverable but requires exploration.

**Diary:** Explicitly legible — agent's declared state is right in the seed.

---

## Open Questions

1. **Summary discipline:** If agents forget to write summaries, does fallback to FsIndex create surprise latency?
   - *Mitigation:* Phase 2 observes this; seed builder can enforce minimum summary on first fail.

2. **Summary accuracy:** Can we trust an agent to write an accurate summary?
   - *Answer:* As much as we trust it to read files accurately. The summary is part of its audit trail.

3. **Cross-agent diaries:** If a parent reads a child's output and summarizes it (reconcile), should that become the child's summary?
   - *Punt to Phase 3:* Real usage will inform this.

4. **Retention policy:** Keep all 365 summaries, or prune old ones?
   - *Punt to Phase 3:* Depends on whether agents want historical comparison or just the current state.

---

## Summary

| Aspect | FsIndex | Diary | Phase Approach |
|---|---|---|---|
| **Complexity** | Low | Low | Very low initially |
| **Warm-start** | O(32) steps | O(1) | Improves over time |
| **Scaling** | ⚠️ Weak (loses history) | ✅ Strong (one per cycle) | Becomes clear in Phase 2 |
| **Agent agency** | ✅ Full | ✅ Full | Same |
| **Shipping now** | ✅ Yes | ✅ Voluntary | ✅ Yes |
| **Post-run data** | Informs refinement | Informs adoption | Better decision making |

**Recommendation:** Ship PR #165 as-is (FsIndex). Enable voluntary summaries in the NVIDIA run. Phase 2 data will show whether formalizing the diary pattern is worth the design complexity.
