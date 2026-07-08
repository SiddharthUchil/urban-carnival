# 05 — Gen-AI Strategy & Akka Migration

> How Gen-AI is leveraged across both phases, and how the system later migrates to **Akka**. Phase flows are
> in [03](03-phase1-anomaly-detection.md) / [04](04-phase2-investigation-insights.md). Decisions:
> [ADR-0003](adr/adr-0003-genai-platform-and-guardrails.md), [ADR-0004](adr/adr-0004-akka-migration-strategy.md),
> [ADR-0005](adr/adr-0005-model-tuning-adaptive-ml.md). The model-tuning/serving layer (Adaptive ML) is
> detailed in [07-adaptive-ml-integration.md](07-adaptive-ml-integration.md).

## Part A — Gen-AI strategy

### A.1 Where Gen-AI adds value (and where it must NOT)
Gen-AI is used for **language, reasoning-over-evidence, and orchestration** — not for detecting anomalies or
computing statistics. Detection stays with `darts`/`pyod`/monitoring ([03](03-phase1-anomaly-detection.md));
the LLM **explains** and **recommends** over numbers the pipeline already produced.

| Phase | Gen-AI use | Value |
|---|---|---|
| **P1** | Generate/maintain **Lakeflow expectation** thresholds from metadata; suggest model hyperparameters from drift logs | Faster DQ setup, less manual tuning |
| **P1** | **Metadata enrichment** — the dictionary is nearly complete (`post_eVar.Name` 98.9%, `post_event_list.Friendly Name` 97.9% populated), so the job is targeted: backfill the ~10 events missing `Friendly Name`, normalize mixed-case `Status` values, and synthesize a business glossary from `Description` + the CoverMe variable PDFs for alert labels | Readable alerts grounded in the dictionary |
| **P2** | **Multi-agent RCA** (localize→correlate→hypothesize→recommend→narrate) | The core "Insights Agent" |
| **P2** | **RAG** over historical incidents/runbooks; **ChatOps** Q&A | Institutional memory, calmer triage |

### A.2 Multi-agent RCA topology (Phase 2)
Modeled on the `ai-data-science-team` **LangGraph Supervisor** pattern (maps ~1:1):

| Agent | Role | Tools / contract |
|---|---|---|
| **Supervisor** | Orchestrates; JSON route schema; avoids re-calling a worker, reroutes on empty data, caps recursion | LangGraph state graph |
| **Data/SQL Agent** | Generates **read-only SELECT** over Gold/anomaly tables; returns data + code; human-review + fix-code nodes | Unity Catalog SQL (RBAC-scoped, read-only) |
| **Change/System-Status Agent** | Queries `change_events` (deploys, tag publishes, campaigns, outages) in the lead-window | Change-log connectors |
| **Contribution Agent** | Runs Cramér's V + Pearson residuals (+ SHAP) and returns ranked drivers | Spark/SQL stats functions |
| **Narrative Agent** | Composes the structured report **only** over retrieved evidence | LLM + JSON schema |
| **Runbook Advisor** | Maps cause→actions via runbook/past-resolution RAG | Vector Search |

### A.3 Platform
- **Primary: Databricks Mosaic AI Agent Framework** — author tool-calling agents (LangGraph/LangChain),
  deploy as a Model Serving endpoint, trace with MLflow, and grade with **Agent Evaluation** (built-in LLM
  judges: correctness, **groundedness**, relevance, safety + SME labeling). Lakehouse-native → least data
  movement. **RAG via Mosaic AI Vector Search** (GA; hybrid keyword+vector, auto-sync from Delta, built-in
  reranking) over `anomaly_insights` + runbooks **+ the four data-dictionary sheets and
  [`metric-registry.yaml`](metric-registry.yaml)** (so agents resolve raw slot names to business terms).
- **Alternative / hybrid: Azure AI Foundry Agent Service** (GA) + **Responses API** with **Azure AI Search**
  — use if the agentic layer must live in the Azure plane. (Note: the older Azure **Assistants API is being
  retired** — do not build on it.)
- **Model tuning & serving — Adaptive ML / Adaptive Engine (Manulife-selected; the production model layer):**
  the Narrative, Runbook, and Data/SQL agents run on an **Adaptive-tuned open-weight SLM** served in
  Manulife's **private environment**, continuously RL-improved (PPO/GRPO/DPO) from the analyst-feedback reward
  signal. This is the **default**. → [07](07-adaptive-ml-integration.md), [ADR-0005](adr/adr-0005-model-tuning-adaptive-ml.md).
- **Frontier models for cold-start + hardest cases:** both Foundry and Databricks can host **Claude**
  (Sonnet-class for triage/narration, Opus-class for hard multi-step RCA) and **GPT**-class models — used to
  bootstrap/judge reward data and to escalate low-confidence cases. Tier by difficulty for cost. *Model IDs are
  time-sensitive — confirm current IDs against the `claude-api` reference at build time.*

### A.4 Guardrails (the part both prior drafts skipped)
RCA that invents a cause is worse than none. Layered controls, each killing a different failure mode:

1. **Strict grounding** — the Narrative/Runbook agents compose **only** over retrieved evidence (contribution
   tables, change logs, series). Decompose output into **atomic claims**; verify each against evidence;
   **abstain** ("insufficient evidence") rather than speculate.
2. **Structured output contract** — emit JSON validated against the `anomaly_insights` schema; reject+repair
   invalid output (don't hope for valid JSON).
3. **Confidence gating** — per-hypothesis confidence; below threshold → `needs_human_review = true`.
4. **HITL** — no external action (ticket creation, rollback *suggestion* acted on) without human approval.
5. **Fallback** — if an agent/LLM/endpoint fails or rate-limits, return the deterministic evidence bundle
   (contribution table + correlated changes) without narrative — the analyst still gets the facts.
6. **Evaluation + audit** — Agent Evaluation groundedness/correctness judges in CI on a labeled RCA set;
   every prompt + evidence + response logged ([02 §9](02-solution-architecture.md)).

### A.5 Example prompts (abridged)
**Online triage (concise, grounded):**
> *System:* "You are an analytics incident assistant. Use ONLY the EVIDENCE. If evidence is insufficient,
> say so and stop. Output JSON: {summary, top_causes[≤3], top_actions[≤3], confidence}."
> *User:* `EVIDENCE = { anomaly: {...}, top_dimensions: [...], recent_changes: [...], runbook_hits: [...] }`

**Cost control:** small model + cached system prompt for triage; large model only when severity=critical or
confidence is low; **Batch API** for offline report generation.

---

## Part B — Akka migration (future state)

### B.1 Why Akka, and when
Manulife publicly **selected Akka to operationalize agentic AI** (announced 2026; platform in beta) as a
"secure and scalable software foundation," targeting **$1B+ enterprise value by 2027** with governance, safety,
and operational SLAs for regulated environments. GMAI–Pulse aligns: the **online detection watchdog** and the
**Phase-2 agents** are the natural candidates to run as durable, governed Akka agents. **Migration happens
*after* the Databricks solution is validated** — Akka is the *runtime*, not a redesign.

### B.2 What Akka provides (verified) — and the honest caveats
- **Agent / Autonomous Agent components** — first-class LLM agents with **session memory**, **tool/function
  calling**, and a **durable decision loop** (iteration-limit safety); model providers (incl. **anthropic**,
  openai) configured in `application.conf`. Built-in **delegation, handoff, teams, moderation**.
- **Entities** (event-sourced durable state + snapshots), **automatic sharding** (one stateful instance
  cluster-wide, single-threaded), **multi-region replication**, **Workflows** (durable multi-step,
  retries/timeouts/compensation), **brokerless messaging + streams with backpressure**.
- **Governance** — access control, request validation, **interaction & intent logging**, OPA policy engine —
  directly supportive of Responsible AI / audit.
- ⚠️ **Caveat (corrects Gemini):** Akka is **not literally "exactly-once."** Its docs warn workflow step
  retries are **not idempotent by default** — the real guarantee is **durable execution + at-least-once steps
  + developer-implemented idempotency/compensation (saga)**. Design external actions (tickets, model calls) to
  be idempotent.
- ⚠️ **Inter-agent comms** (MCP/A2A/ACP) are routed **through a Workflow supervisor**, not direct agent-to-agent.

### B.3 Python(ML) ↔ JVM(Akka) boundary (corrects "port the models")
Akka is **JVM-only**; `pyod`/`darts` do **not** run in-process. The idiomatic boundary is **remote model
serving**:
- Serve detection models behind **HTTP or gRPC** — **MLflow Model Serving**, **Azure ML managed endpoint**, or
  a FastAPI microservice. Akka calls them via `HttpClientProvider`/`GrpcClientProvider` **from Workflows /
  Consumers** (not from inside an agent's own code path). **gRPC** preferred for low-latency scoring.
- High-throughput async scoring rides **Kafka**/PubSub (relevant only if the Phase-3+ streaming hot lane is
  ever built — [ADR-0001 v2](adr/adr-0001-near-real-time-microbatch.md)).
- **Databricks remains the data/training/feature plane and system-of-record.** Akka hosts only the online
  runtime + agents and calls back into the lakehouse via APIs.

### B.4 Migration strategy (strangler-fig)
| Step | Action | Guardrail |
|---|---|---|
| 1. Boundaries | Keep each agent/scorer a stateless service with a typed contract (done in P1/P2 design) | No redesign needed at cutover |
| 2. Dual-run | Akka services consume the same Gold increments / alert triggers as the scheduled jobs; scores/agents run **in parallel** (shadow) | Legacy path stays authoritative |
| 3. Shadow-validate | Compare Akka outputs vs. the Databricks scheduled-job output; promote only within tolerance | Akka **interaction/intent logging** aids diffing |
| 4. Cutover | Shift the online watchdog + agents to Akka **per-feature** via routing weight | **Saga/compensation** for external actions; rollback = flip the weight |
| 5. Consolidate | Retire the interim Databricks-hosted agent/watchdog services once stable | Batch/training/data plane stays on Databricks |

### B.5 Agent → Akka mapping
Supervisor → Akka **Workflow** (durable orchestration); Data/SQL, Change, Contribution, Runbook → **Agents**
or **Consumers** calling lakehouse/model endpoints; online detection watchdog → sharded **Entity** holding
rolling per-segment state + an **Autonomous Agent** loop; ChatOps bot → **Agent** with session memory.

See [ADR-0004](adr/adr-0004-akka-migration-strategy.md) for the decision record.
