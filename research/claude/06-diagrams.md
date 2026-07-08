# 06 — Diagrams (Mermaid) + Lucidchart Import Guide

> **Five diagrams** covering the end-to-end two-source architecture, Phase-1 detection, Phase-2 RCA +
> ChatOps, the three-layer platform + improvement flywheel, and the Akka target state + migration. They
> render natively in **GitHub** and **VS Code** (Mermaid preview), and import into **Lucidchart**.
> Architecture context: [02](02-solution-architecture.md); what changed on 2026-07-02:
> [10-data-profile-alignment.md](10-data-profile-alignment.md).

Reduced from the previous ten-diagram set — the prior research (Perplexity explicitly, Gemini implicitly)
converges on ~4–5 core views, and the old set redrew the same lanes repeatedly:

| Kept | Absorbs (old) | Rationale |
|---|---|---|
| **D1** end-to-end hero | old ① + top level of ②③⑧ | one authoritative system view |
| **D2** Phase-1 detection | old ②③⑧ detail | offline + online are now one batch-first pipeline |
| **D3** Phase-2 RCA + ChatOps | old ④⑤⑥ | agent topology, guardrails, and triage flow belong together |
| **D4** platform + flywheel | old ⑨⑩ | the flywheel *is* Layer 2's loop — one picture |
| **D5** Akka target + migration | old ⑦ | unique content, updated to batch-first |

## How to render / import

- **GitHub / VS Code:** render automatically in Markdown preview (VS Code: "Markdown Preview Mermaid Support").
- **mermaid.live:** paste a block (without the ```` ```mermaid ```` fences) to export PNG/SVG.
- **Lucidchart:** use Lucidchart's **Mermaid import** — *Insert → Diagram/Shape → Mermaid* (or the Mermaid
  shape via the marketplace; exact menu varies by version). Paste the diagram body **without** the
  ```` ```mermaid ```` fences, then Import; Lucidchart lays it out as editable shapes.
- **draw.io / diagrams.net (alternative):** *Arrange → Insert → Advanced → Mermaid*.

**Lucid shape conventions (apply after import):** cylinders = data stores (Delta tables, ADLS, Vector
Search) · rectangles = services/compute · diamonds = gates/decisions (severity, guardrails, identity/privacy gate) ·
lightning bolt = alerts/notifications · dashed borders = future/optional components (hot lane, Akka) ·
one swimlane per concern (Sources / CoverMe serving / Databricks plane / Consumers).

Each diagram below is self-contained — copy from the first line inside the fence to the last.

---

## D1 — End-to-end reference architecture (two sources, one Databricks plane)

```mermaid
flowchart LR
    subgraph SRC["Sources - Adobe Analytics data feeds"]
        GWAMT["GWAM: gwam_prod_catalog .inv_typed_common.adobe_hit_data - UC Delta on ADLS Gen2"]
        CMFILES["CoverMe files on ADLS Gen2"]
    end
    subgraph SYN["CoverMe existing serving - untouched"]
        SYNSQL["Synapse serverless SQL: martech.adobe_coverme.hit_data"]
        CONS["Existing CoverMe consumers"]
    end
    subgraph DBX["Databricks - single detection and ML plane (ADR-0006)"]
        EXTLOC["UC external location (primary) / Lakehouse Federation sqldw (fallback)"]
        BRONZE["Bronze: mirror + prune per domain"]
        SILVER["Silver: conform + dictionary decode + Identity & Privacy layer"]
        GOLD["Gold: registry-driven KPI series"]
        REGY["metric-registry.yaml (29 AD-tagged seeds)"]
        DET["Detectors: darts + pyod, scheduled micro-batch"]
        MON["Lakehouse Monitoring: freshness + completeness"]
        ANOM["anomalies Delta"]
        RCA["Phase 2 RCA agents + Vector Search"]
        INS["anomaly_insights Delta"]
        BI["AI/BI dashboards"]
    end
    HOT["Phase 3+ option: Edge Network to Event Hubs to Structured Streaming (ADR-0001 v2)"]
    TEAMS["Teams / Email / PagerDuty"]
    TICKET["ServiceNow / Jira"]
    CMFILES --> SYNSQL --> CONS
    GWAMT --> BRONZE
    CMFILES --> EXTLOC --> BRONZE
    BRONZE --> SILVER --> GOLD --> DET --> ANOM
    REGY --> GOLD
    SILVER --> MON --> ANOM
    ANOM --> RCA --> INS --> BI
    ANOM -->|severity| TEAMS
    RCA -.->|drafted, human-approved| TICKET
    HOT -.->|if SLA + volume proven| DET
```

*Both domains share one Adobe hit schema and one pipeline; CoverMe's Synapse surface keeps serving its
current consumers while Databricks reads the same ADLS files. The streaming hot lane is a dashed future
option, not a Phase-1 component.*

---

## D2 — Phase 1: batch-first detection pipeline (offline + online in one)

```mermaid
flowchart TD
    GATE["DATA GATE: needs >=30 (ideally 90) days production feed per domain - baselines/backtests blocked until then"]
    SRC["Typed hit tables: GWAM native UC / CoverMe via external location"] --> BR["Bronze: mirror + prune ~1,008 empty slot columns"]
    BR --> SV["Silver: conform + Identity & Privacy layer (keyed pseudonymization, ADR-0007) + dictionary decode + DQ gates (Lakeflow expectations)"]
    SV --> GD["Gold: registry KPI series - post_event_list aggregates, daily grain (hourly reserved)"]
    REGY["metric-registry.yaml"] --> GD
    GD --> TRAIN["Train: darts ForecastingAnomalyModel per metric; pyod ECOD/COPOD/IForest"]
    TRAIN --> BT["Backtest historical_forecasts + select via TSB-AD/MetaOD"]
    BT --> REG["MLflow UC Model Registry champion"]
    GD --> SCORE["Scheduled scoring at feed cadence + EWMA / robust-z control charts"]
    REG --> SCORE
    SCORE --> TH["Adaptive thresholds: QuantileDetector / PyThresh + severity tiers"]
    TH --> DB["Debounce / dedup per metric x domain x segment"]
    DB --> ANOM["anomalies Delta"]
    SV --> MON["Lakehouse Monitoring: freshness + completeness (both domains)"]
    MON --> ANOM
    ANOM --> ALERT["SQL Alert to Webhook to Logic Apps to Teams/PagerDuty"]
    GATE -.-> TRAIN
```

*One pipeline serves both planes: the "online" mode is the same scoring path scheduled at feed cadence
([ADR-0001 v2](adr/adr-0001-near-real-time-microbatch.md)). Intraday activates only when the data gate
clears and volume is confirmed.*

---

## D3 — Phase 2: RCA agents + guardrails + ChatOps triage

```mermaid
flowchart TB
    ANOM["Confirmed severe anomaly"] --> LOC["Localize: Cramer's V + Pearson residuals + SHAP"]
    ANOM --> COR["Correlate: change_events join - CAVEAT: 11 correlation keys + change sources not yet acquired (04 s2b)"]
    subgraph ORCH["Orchestration - Mosaic AI / LangGraph"]
        SUP["Supervisor JSON route schema"]
        DSQL["Data/SQL Agent read-only SELECT"]
        CHG["Change/System-Status Agent"]
        CON["Contribution Agent"]
        RUN["Runbook Advisor"]
        NAR["Narrative Agent"]
    end
    subgraph DATA["Lakehouse"]
        GOLD["Gold + anomalies"]
        VS["Vector Search: incidents + runbooks + dictionary + registry"]
    end
    subgraph GR["Guardrails"]
        GRD["Grounding + atomic-claim verify + abstain"]
        JSONV["JSON schema validate + repair"]
        CONF["Confidence gate"]
        HITL["Human-in-the-loop"]
        FB["Deterministic fallback"]
    end
    LOC --> SUP
    COR --> SUP
    SUP --> DSQL --> GOLD
    SUP --> CHG --> GOLD
    SUP --> CON --> GOLD
    SUP --> RUN --> VS
    SUP --> NAR
    VS --> SUP
    NAR --> GRD --> JSONV --> CONF --> HITL
    CONF -.->|low confidence or error| FB
    HITL --> INS["anomaly_insights + ticket draft"]
    INS --> MEM["Institutional memory feeds Vector Search"]
    MEM -.-> VS
    subgraph CHAT["ChatOps triage strip (severity-gated)"]
        T1["1 Alert card to Teams/Slack: summary + top-3 causes + top-3 actions"]
        T2["2 Analyst follow-up: show related / compare last week"]
        T3["3 Bot re-queries lakehouse - grounded answer or route to human"]
        T1 --> T2 --> T3
    end
    INS --> T1
```

*Merges the old agent-topology, offline-RCA, and ChatOps-sequence diagrams. The change-event join carries
an explicit caveat until deployment/campaign sources and the 11 missing keys exist.*

---

## D4 — Three-layer platform + continuous-improvement flywheel

```mermaid
flowchart TB
    subgraph L1["Layer 1 - Databricks: data, features, detection"]
        LH["Lakehouse Bronze/Silver/Gold (both domains)"]
        DET["Detection: darts + pyod + Lakehouse Monitoring"]
        ANOM["anomalies + anomaly_insights"]
        AFB["analyst_feedback: confirm / dismiss / edit"]
    end
    subgraph L2["Layer 2 - Adaptive ML / Adaptive Engine: RLOps"]
        RM["Reward model from feedback"]
        TUNE["RL tune: PPO/GRPO/DPO + synthetic data"]
        EVAL["Evaluate vs incumbent, promote if better"]
        SERVE["Serve tuned SLM in private env (PIPEDA)"]
    end
    subgraph L3["Layer 3 - Akka: agentic runtime (future)"]
        AG["Durable agents / workflows"]
    end
    FRONT["Frontier LLM Claude/GPT: cold-start + hard cases + teacher"]
    DET --> ANOM --> AG
    AG --> AFB
    AFB --> RM --> TUNE --> EVAL --> SERVE
    SERVE -->|RCA model endpoint| AG
    FRONT -.->|bootstrap + escalate| AG
    FRONT -.->|distill| TUNE
    AG -->|read-only| LH
```

*Databricks trains/detects, Adaptive ML tunes+serves the RCA model, Akka runs the agents — and every
triaged anomaly becomes training signal, closing the flywheel (see [07](07-adaptive-ml-integration.md)).*

---

## D5 — Akka target state + strangler-fig migration

```mermaid
flowchart LR
    TRIG["Gold increments + alert triggers (scheduled jobs; Kafka only if Phase 3+ hot lane exists)"]
    subgraph DBX["Databricks stays: data/training/features"]
        GOLD["Gold + Features"]
        REG["MLflow Model Serving HTTP/gRPC"]
        LH["anomalies + insights system of record"]
    end
    subgraph AKKA["Akka future runtime"]
        WF["Workflow Supervisor durable + saga/compensation"]
        ENT["Entity per-segment rolling state event-sourced"]
        AGENT["Agents / Autonomous Agent LLM + tools + memory"]
    end
    GOV["Audit / Responsible AI"]
    TRIG --> ENT
    ENT -->|score req gRPC| REG
    WF --> AGENT
    AGENT -->|read-only API| GOLD
    AGENT --> LH
    WF -.->|governance logging + OPA| GOV
    subgraph MIG["Migration strangler-fig"]
        M1["1 dual-run shadow"] --> M2["2 shadow-validate vs scheduled jobs"] --> M3["3 per-feature cutover routing weight"] --> M4["4 consolidate: retire interim Databricks-hosted agent services"]
    end
```

*Akka fronts the batch-first services; the data/training plane never leaves Databricks. Correctness =
durable execution + at-least-once + idempotency/saga — not "exactly-once"
([ADR-0004](adr/adr-0004-akka-migration-strategy.md)).*

---

*D1 is the executive/architecture view; D2–D3 are the Phase-1/Phase-2 working views; D4–D5 are the
platform-strategy views (Adaptive ML flywheel, Akka future state). All five import into Lucidchart via
Mermaid import; apply the shape conventions above after import.*
