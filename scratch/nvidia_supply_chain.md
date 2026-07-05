# Nvidia supply-chain research on Coral — what runs today, what's missing, target graph

*Status: ideation / design strawman, reconciled to the filed issue set. Captures (a) what is
runnable right now with real LLM inference, (b) the one feature-gap between the in-process path and
the graph.yaml path that blocks the "multi-agent research graph with web + X tools" shape, (c) the
filed issue breakdown that closes it, and (d) a target `graph.yaml` for an Nvidia supply-chain
monitor to shape against. Evidence cited to file:line as of the post-Stage-5 tree.*

---

## 1. Two run paths, and what each can do today

### Path A — in-process single agent (`node_run_llm`) — **real LLM + real arbitrary MCP, no infra**

- Binary: `crates/coral_node/src/bin/node_run_llm.rs`. No Temporal, no Postgres, no worker daemon.
- Spawns **one** MCP server as a stdio subprocess (`-- <cmd> [args]`) via
  `McpClient::connect_stdio` (`crates/coral_node/src/mcp/mod.rs:104`) and registers **all** its
  tools (`register_mcp_server`, `mcp/mod.rs:181`).
- Real LLM `Decide` via `LlmDecide` (`decide_llm/llm_decide.rs`). Vendors: **Anthropic**
  (default model `claude-haiku-4-5`, key `ANTHROPIC_API_KEY`, `model_client/anthropic.rs:25,42`)
  and **Cohere** (`command-a-03-2025`, `COHERE_API_KEY`, `cohere.rs`). Selected via `--vendor`.
- Output lands on disk: `<fs_root>/<ts>/{evidence,outputs,retirement.json}`.

**This is where real web/X research works today** — point it at a web-search MCP server. Limit:
one MCP server per run; single agent (no parent/child).

### Path B — graph.yaml → `coral apply` → Temporal worker — **multi-agent + durable, but echo-only tools**

- `coral apply graph.yaml` (`crates/coral_graph/src/bin/coral_apply.rs`) is a thin Temporal
  client: writes the structural DB, starts an `AgentWorkflow` per agent (parents-first DFS),
  signals seeds, exits. Multi-agent (parent/children, spawn, reconcile, conflict-log) is fully
  landed (Stage 5).
- **BUT tools are `builtin: echo` only.** The YAML validator rejects `kind: mcp` outright
  (`yaml.rs:419-422`, `GraphYamlError::McpToolRejected`, message: *"mcp-tool support is a planned
  follow-up"*), and the worker only installs `ToolRegistry with tools: echo`
  (`coral_worker/src/bin/worker.rs:99-104`).
- Requires: Docker stack (`make up` → Temporal + Postgres), worker daemon
  (`cargo run -p coral_worker --bin worker`, needs `DATABASE_URL` + a vendor key), then
  `coral apply`.

### The gap

The shape you want — **a multi-agent Nvidia graph whose children fetch real web/X data** — needs
Path B's topology *and* Path A's MCP tools. Today those don't intersect. The missing piece is one
focused feature: **MCP tools in `graph.yaml` + the worker.**

---

## 2. The unlock: "MCP tools in graph.yaml + worker" — VERIFIED scoping

Traced the real code (file:line below). The feature is **smaller at the data layer and larger at
the worker layer** than first assumed.

### What already works (no change needed)

- **DB schema models MCP fully.** `tools` table has `kind`, `command`, `args`, `env_refs`, plus
  the `agent_tools` join (`migrations/0001_initial.sql:72-91`; Rust mirror
  `coral_graph/src/types.rs:61-69`).
- **`create_from_yaml` has the MCP write path** — it iterates `graph.tools` and inserts
  `kind/command/args` + `agent_tools` (`store.rs:275-411`). The `ToolKind::Mcp` arm currently
  *errors* (`store.rs:283-289`) only because the validator never lets MCP through; flipping it to
  insert is mechanical. This is effectively **dead code today** behind the validator.

### The actual gaps

1. **Validator rejects `kind: mcp`** (`yaml.rs:419-422`, `McpToolRejected`). Lift it; flip the
   `create_from_yaml` arm to insert. The DB write path lights up. *Small.* Add an optional `env:`
   map to the `Mcp` YAML variant for scoped API keys (see gap 3).
2. **Worker registers only `echo`, synchronously, into a process-global `OnceLock` registry**
   (`coral_worker/src/bin/worker.rs:99-104`; `coral_temporal/src/worker.rs:101-121`). Needs to
   register MCP servers from the graph. *The big one.*
3. **No per-server subprocess env.** `McpClient::connect_stdio` does `Command::new(cmd).args(..)`
   with **no `.env()`** (`mcp/mod.rs:104`) — the MCP subprocess inherits only the worker's process
   env. For per-server API keys (X key for the x-search server, not the web one) we need a
   `connect_stdio_with_env`-style surface. *Small/medium.*

### Two facts that constrain the design

- **`execute_tool` dispatches global-by-name, no agent scoping.** The activity calls
  `tool_registry().call(name, args)` (`activities.rs:497-535`); the registry is a global
  `OnceLock<Arc<ToolRegistry>>`. There is no per-agent tool restriction at dispatch.
- **The model is never shown a tool catalog.** `decision_tools()` is deliberately static — a
  single free-form `call_tool` whose `name` the model fills in from the **mandate text**, validated
  against the registry only at dispatch (`decide_llm/schema.rs:43-71`); the prompt never lists
  tools (`decide_llm/prompt.rs`). So `agent_tools` scoping is *authored* (DB rows) but *unenforced*
  (runtime). The operator must name concrete callable tool names in the mandate (as the echo/get-sum
  examples already do). For MCP this means the mandate references the MCP server's own tool names
  (from `list_tools()`), which the operator may not know up front — a usability wrinkle, not a
  blocker.

### The architectural fork — resolved to Option B

One worker daemon serves the whole `coral-agents` queue → many graphs/agents. The registry is
process-global. Where does MCP-server lifecycle + registry scoping live?

- **Option A — worker-global from config/env (v1 expedient).** Worker registers a fixed MCP-server
  set at boot, shared across every graph it serves. graph.yaml mcp tools become advisory. Simplest;
  ships fast. Limit: one MCP tool set per worker deployment.
- **Option B — worker reads each graph's tools from the structural DB (chosen).** The worker
  already holds `StructuralDbStore` + `DATABASE_URL`. Register each graph's MCP servers into a
  **graph-scoped** registry — replaces the global `OnceLock` with a per-graph map. Faithful to
  "graph.yaml is the source of truth"; aligns with the millions-of-subagents/one-fleet vision.
  Bigger: registry refactor + lifecycle/dedup/teardown.
- **Option C — per-agent connection in the workflow/activity.** Most isolated, but an MCP subprocess
  per agent (or per tick) breaks the long-lived-subprocess model and is expensive. Wrong for v1.

**Maintainer chose Option B.** Under B the worker issue (MCP-3) *is* the per-graph registry
refactor; graph.yaml is the runtime source of truth.

## 2a. Filed issue breakdown — parent #102 with native sub-issues (Option B)

Shape: **medium** (~5 sub-issues, one session) → one **parent issue with native sub-issues**, per
`DEVELOPMENT.md` §6. Filed on `gchatz22/coral_engine`; dependency-ordered.

- **#102 — parent.** MCP tools in graph.yaml end-to-end (web search, X, arbitrary MCP servers).
- **#103 — MCP-1.** Lift `kind: mcp` rejection + add optional `env:` to the YAML `Mcp` variant;
  flip the `create_from_yaml` arm to persist mcp rows; regen `examples/graph.schema.json`.
  *(independent)*
- **#104 — MCP-2.** Per-server subprocess env in `connect_stdio` (`connect_stdio_with_env`); extend
  the inherited env, don't clear it. *(independent of #103)*
- **#105 — MCP-3.** Worker builds **per-graph** MCP registries by reading each graph's tool rows
  from the structural DB; replace the process-global `OnceLock<ToolRegistry>` with a per-`graph_id`
  map; thread `graph_id` to the `execute_tool` dispatch site; add a `StructuralDbStore`
  graph-tools read method; MCP-subprocess lifecycle/dedup/teardown. *(depends on #103, #104 — the
  biggest issue)*
- **#106 — MCP-4.** End-to-end MCP graph example + env-gated live smoke on the workflow path
  (against the free `@modelcontextprotocol/server-everything` reference server); README run recipe.
  *(depends on #103, #105)*
- **#107 — MCP-5 (optional/follow-up).** Enforce `agent_tools` scoping at dispatch + optionally
  surface the agent's tool catalog to the model so selection stops relying on mandate prose.
  *(after #105)*

(*#108 "MCP-6" was the Option-A→B follow-up; **closed as subsumed into #105** once Option B was
chosen.*)

Dependency order: #103 + #104 (independent) → #105 → #106; #107 optional after #105.

---

## 3. Target `graph.yaml` (design strawman — runnable once #103 + #105 land)

Decomposition follows VISION's *atomic monitorability*: one parent analyst + narrow children, one
supply-chain layer each, so each child's mandate is small enough to verify and its outputs carry
provenance the parent reconciles.

```yaml
apiVersion: coral.engine/v1alpha1
kind: Graph
metadata:
  name: nvidia-supply-chain
  description: |
    Continuous supply-chain risk monitor for NVIDIA's data-center
    accelerator stack. Narrow children research one layer each (fabrication,
    HBM, networking, export controls) via web search + X; the parent
    reconciles their findings into a provenance-grounded risk report and
    re-runs on schedule to catch new signals.
defaults:
  idle_period: 30m            # wake cadence when no signal arrives
tools:
  # kind: mcp is REJECTED by coral apply until #103 lands. Shape is exactly
  # what the MCP-in-YAML feature will accept (incl. the optional env: map).
  - id: web-search
    kind: mcp
    command: npx
    args: ["-y", "exa-mcp-server"]      # or a Brave/Tavily MCP server
    env:
      EXA_API_KEY: "${EXA_API_KEY}"
  - id: x-search
    kind: mcp
    command: npx
    args: ["-y", "x-mcp-server"]        # community/custom X API v2 MCP server
    env:
      X_BEARER_TOKEN: "${X_BEARER_TOKEN}"
agents:
  - id: analyst
    mandate:
      text: |
        You coordinate a supply-chain risk assessment of NVIDIA's data-center
        accelerator stack. Your children each cover one layer and report their
        findings to you as outputs. As findings arrive: reconcile them (flag
        conflicts, dedupe overlapping claims), then emit a consolidated risk
        report whose evidence cites the child outputs you used. Re-run on your
        schedule to fold in new signals. Do not research layers yourself unless
        a child is missing one — your job is synthesis and conflict resolution.
      idle_period: 1h
    tools: [web-search, x-search]
    children:
      - id: fabrication
        mandate:
          text: |
            Research NVIDIA's fabrication & advanced-packaging supply ONLY:
            TSMC wafer allocation for NVIDIA, CoWoS / SoIC packaging capacity,
            foundry lead times and bottlenecks. Emit findings with every claim
            cited to a source as evidence. Stay narrow — fabrication & packaging.
        tools: [web-search, x-search]
      - id: memory-hbm
        mandate:
          text: |
            Research high-bandwidth-memory supply for NVIDIA GPUs ONLY: SK Hynix,
            Samsung, Micron HBM3E/HBM4 capacity, qualification status, pricing,
            allocation. Cite every claim. Stay narrow — HBM only.
        tools: [web-search, x-search]
      - id: networking
        mandate:
          text: |
            Research NVIDIA networking/interconnect supply ONLY: NVLink/NVSwitch,
            InfiniBand/Spectrum, optical transceivers, switch silicon suppliers.
            Cite every claim. Stay narrow — interconnect only.
        tools: [web-search, x-search]
      - id: export-controls
        mandate:
          text: |
            Research geopolitical / export-control risk to NVIDIA's supply and
            demand ONLY: US/China export rules, country bans, customer
            concentration, second-source risk. Cite every claim. Stay narrow —
            policy/geopolitics only.
        tools: [web-search, x-search]
seed:
  triggers:
    - agent: analyst
      at: start
      external: { kind: kickoff, payload: { objective: "Initial NVIDIA supply-chain risk baseline" } }
    - agent: fabrication
      at: start
      external: { kind: kickoff, payload: {} }
    - agent: memory-hbm
      at: start
      external: { kind: kickoff, payload: {} }
    - agent: networking
      at: start
      external: { kind: kickoff, payload: {} }
    - agent: export-controls
      at: start
      external: { kind: kickoff, payload: {} }
```

Notes on the shape (verified against `yaml.rs`):
- Children declared in YAML are **co-started** by `coral apply` (parents-first DFS,
  `build_workflow_starts`), with `parent_handle` wired — the parent does **not** `SpawnChild` them.
  Child outputs flow up as `ChildOutput` triggers; the parent reconciles. Seeding each child makes
  them start working immediately rather than waiting for `idle_period`.
- `idle_period` uses humantime (`30m`, `1h`); `mandate.text` + `idle_period` (+ optional
  `max_ticks`) are the only mandate knobs in v1. Ids must match `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`.
- No `max_ticks` here → agents run until they `Retire`. For a continuous monitor that's intended;
  for a bounded experiment, add `max_ticks`.
- The `env:` keys (`EXA_API_KEY`, `X_BEARER_TOKEN`) are the per-server secrets #104 wires into the
  MCP subprocess. Whether values are literals or worker-env indirection is decided in #103/#105.
- The mandates must reference the MCP servers' **actual** tool names (from each server's
  `list_tools()`), because the model selects tools by name from the mandate text (no catalog is
  shown until #107). Adjust the prose to the real tool names once the servers are chosen.

---

## 4. What you can run for real *today* (Path A, single agent)

```bash
ANTHROPIC_API_KEY=sk-ant-... \
cargo run --features "mcp llm-anthropic" --bin node_run_llm -- \
  --vendor anthropic \
  <config.json> <triggers.jsonl> /tmp/coral-nvidia-fs \
  -- npx -y <a-web-search-mcp-server>
```

- `config.json` = `{ "text": "<nvidia research mandate>", "idle_period": 60000, "max_ticks": 12 }`
  (idle_period here is **milliseconds**, unlike the YAML's humantime).
- `triggers.jsonl` = `{"kind":"kickoff","payload":{}}`.

This is one agent, one MCP server, real inference — enough to prove the research loop end-to-end
before the multi-agent MCP feature (#102) lands.
