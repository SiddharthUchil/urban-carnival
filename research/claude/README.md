# GMAI–Pulse — Finalized Anomaly Detection & Insights Solution (Canada Retirement + CoverMe)

**Claude's synthesis** of the Gemini and Perplexity research — re-grounded on the **2026-07-02 data
profiling** (`new_data/`) — into a single, decision-ready solution for detecting and explaining anomalies
in **Canada Retirement (GWAM)** and **CoverMe** web-analytics signals (Adobe Analytics data feeds) on an
**Azure + Databricks** stack, across two phases, each with **offline (batch)** and **online (scheduled
micro-batch)** designs — with a Gen-AI insights layer (models **tuned & served via Adaptive ML**) and a
planned **Akka** migration.

> **What changed 2026-07-02:** the `new_data/` profiling established that the two domains land on two
> different platforms (GWAM on Databricks/Unity Catalog; CoverMe on **Synapse serverless SQL**), that both
> share one canonical Adobe hit schema, and that no streaming collection or production time-series exists
> yet. Full traceability: [10-data-profile-alignment.md](10-data-profile-alignment.md).

## Executive summary

GMAI–Pulse shifts monitoring from reactive to **proactive, explained alerts** ("what changed, where,
when, why, and what to do"). The design rests on eight decisions:

1. **One Databricks compute plane over two source platforms** — detection/ML runs only on Databricks;
   CoverMe's ADLS-backed data is read via a **UC external location** (primary) or **Lakehouse Federation
   `sqldw`** (fallback), while its existing Synapse serverless surface stays untouched.
   *(new — reflects the profiled landing zones)* → [ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md).
2. **Batch-first micro-batch ingestion** — both feeds land as at-rest tables, so Phase 1 runs scheduled
   micro-batch jobs at feed cadence (**daily grain first**); the Edge Network → Event Hubs streaming hot
   lane is an explicitly optional **Phase-3+ upgrade** with defined trigger criteria. *(amends the v1
   two-lane design)* → [ADR-0001 v2](adr/adr-0001-near-real-time-microbatch.md).
3. **Registry-driven metrics** — detection targets come from a versioned
   [`metric-registry.yaml`](metric-registry.yaml) seeded from the **29 dictionary rows business-tagged
   "Anomaly Detection"**, validated against the machine-readable profile. → [03 §5](03-phase1-anomaly-detection.md).
4. **Right model per signal** — `darts` forecasting for seasonal KPIs, `pyod` ECOD/COPOD for multivariate,
   and Databricks **Lakehouse Monitoring** (built-in freshness/completeness anomaly detection) for the
   operational/tagging-health class. **Adaptive thresholds**, not a fixed cutoff. → [ADR-0002](adr/adr-0002-model-family-selection.md).
5. **Phase-2 RCA via a grounded multi-agent Gen-AI layer** — contribution analysis (Cramér's V + Pearson
   residuals) + change-event correlation, narrated by guard-railed agents (grounding, abstention, JSON
   contracts, confidence gating, human-in-the-loop) over a RAG knowledge base. *(Change-event correlation
   is gated on acquiring the 11 missing correlation keys — [04 §2b](04-phase2-investigation-insights.md).)*
6. **Operational rigor both drafts omitted** — evaluation (PR-AUC, range-based F1, MTTD/MTTR), drift &
   retraining, a cost model, and PIPEDA/Law 25 governance with a **hard PII-classification gate** (24
   flagged columns) before any ingestion.
7. **Model layer = Adaptive ML (Adaptive Engine); runtime future = Akka.** Phase-2 agents run on an
   **RL-tuned open SLM** in a private environment, continuously improved from the analyst-feedback loop
   (frontier LLMs for cold-start/hard cases); clean service boundaries let detection + agents re-host onto
   Akka durable agents later (models via gRPC, at-least-once/saga semantics). Three-layer platform:
   **Databricks → Adaptive ML → Akka**. → [07](07-adaptive-ml-integration.md).
8. **Identity & Privacy layer with keyed pseudonymization; stitching deferred.** Visitor identifiers are
   pseudonymized with a Key Vault-held HMAC key at Bronze→Silver (joinability preserved, crypto-erasure
   possible); identity stitching is **gated** on a person-level ID actually existing in the feed (none
   does today); Synapse serverless secure-view governance recommended for CoverMe's direct consumers.
   *(new 2026-07-04 — verdicts on the Perplexity/Gemini extension research)*
   → [11](11-privacy-identity-governance.md), [ADR-0007](adr/adr-0007-identity-privacy-layer.md).

> **#1 blocker:** the profiled corpus is schema + dictionary only — **no production time-series exists**.
> Baselines, thresholds, and model selection are blocked until a **≥30-day (ideally 90-day) hit-level
> feed** lands per domain ([03 §1](03-phase1-anomaly-detection.md), [10 §3](10-data-profile-alignment.md)).

## How to read this package

| Order | Document | What it covers |
|---|---|---|
| 1 | [01-critique-and-synthesis.md](01-critique-and-synthesis.md) | Critique of Gemini & Perplexity; what we keep/correct/add; **2026-07-02 reassessment** |
| 2 | [02-solution-architecture.md](02-solution-architecture.md) | End-to-end architecture; data grounding; stack; MLOps; **eval, cost, governance** |
| 3 | [03-phase1-anomaly-detection.md](03-phase1-anomaly-detection.md) | Phase 1 detection — offline + online; **metric registry** |
| 4 | [04-phase2-investigation-insights.md](04-phase2-investigation-insights.md) | Phase 2 RCA & insights — offline + online |
| 5 | [05-genai-and-akka.md](05-genai-and-akka.md) | Gen-AI strategy (agents, prompts, guardrails) + **Akka migration** |
| 6 | [06-diagrams.md](06-diagrams.md) | **5 Mermaid diagrams (D1–D5)** + Lucidchart import guide |
| 7 | [07-adaptive-ml-integration.md](07-adaptive-ml-integration.md) | **Adaptive ML** — the model-tuning/serving layer; how it changes the solution |
| 8 | [08-library-mapping.md](08-library-mapping.md) | pyod / darts / anomaly-detection-resources / ai-data-science-team → roles |
| 9 | [09-references.md](09-references.md) | Cited sources + confidence flags |
| 10 | [10-data-profile-alignment.md](10-data-profile-alignment.md) | **What the 2026-07-02 `new_data/` profiling changed** + open blockers |
| 11 | [11-privacy-identity-governance.md](11-privacy-identity-governance.md) | **Privacy, identity & governance** — Identity & Privacy layer, Law 25/C-27 mapping, Synapse secure views, erasure, roles |
| 12 | [12-eda-findings-analysis.md](12-eda-findings-analysis.md) | EDA findings from the real GWAM Databricks feed |
| 13 | [13-global-serving-topology.md](13-global-serving-topology.md) | **Global serving topology** — React/AKS surface + BFF, Azure AI Foundry Gen-AI plane, unstructured data lane (ADLS Gen2 → AI Search), global access/residency |
| — | [metric-registry.yaml](metric-registry.yaml) | Versioned Phase-1 metric registry (29 AD-tagged seeds) |
| — | [adr/](adr/) | ADR-0001 ingestion (v2) · ADR-0002 models · ADR-0003 Gen-AI · ADR-0004 Akka · ADR-0005 Adaptive ML · ADR-0006 compute plane · ADR-0007 identity & privacy · **ADR-0008 serving topology & Gen-AI plane** |

## Requirement-coverage map

| Your ask | Where it's addressed |
|---|---|
| **Phase 1 — offline** anomaly detection | [03 §3](03-phase1-anomaly-detection.md), diagram [D2](06-diagrams.md) |
| **Phase 1 — online** detection (scheduled micro-batch) | [03 §4](03-phase1-anomaly-detection.md), diagram [D2](06-diagrams.md) |
| **Phase 2 — offline** investigation & insights | [04 §3](04-phase2-investigation-insights.md), diagram [D3](06-diagrams.md) |
| **Phase 2 — online** triage & ChatOps | [04 §4](04-phase2-investigation-insights.md), diagram [D3](06-diagrams.md) |
| **Lucid diagrams** (reduced, effective set) | [06](06-diagrams.md) D1–D5 — Mermaid, Lucidchart-importable |
| **Both data domains** (Canada Retirement + CoverMe) | [02 §2–3](02-solution-architecture.md), [ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md), diagram [D1](06-diagrams.md) |
| **Leverage Gen-AI** | [05 Part A](05-genai-and-akka.md); also [03 §3.3](03-phase1-anomaly-detection.md), [04](04-phase2-investigation-insights.md) |
| **Azure + Databricks stack** | [02 §5](02-solution-architecture.md) |
| **Reference repos** (pyod, darts, resources, ai-data-science-team) | [08](08-library-mapping.md) |
| **Akka migration (later stage)** | [05 Part B](05-genai-and-akka.md), [ADR-0004](adr/adr-0004-akka-migration-strategy.md), diagram [D5](06-diagrams.md) |
| **Adaptive ML partnership** (model tuning/serving + leverage) | [07](07-adaptive-ml-integration.md), [ADR-0005](adr/adr-0005-model-tuning-adaptive-ml.md), diagram [D4](06-diagrams.md) |
| **Critique Gemini & Perplexity** | [01](01-critique-and-synthesis.md) |
| **Identify causes** (deployments, upgrades, outages, trends) | [04 §2(b)](04-phase2-investigation-insights.md) (change-event correlation — gated on key acquisition) |
| **Recommendations / actionable insights** | [04 §5](04-phase2-investigation-insights.md) |
| **Data-profile alignment + blockers** | [10](10-data-profile-alignment.md) |
| **Privacy/identity extension research** (Perplexity & Gemini `*_extending_GMAI.md`) | [11](11-privacy-identity-governance.md) verdicts + design, [ADR-0007](adr/adr-0007-identity-privacy-layer.md) |
| **React/TS AI-BI app for global users** (AKS serving, Foundry Gen-AI) | [13](13-global-serving-topology.md) D6, [ADR-0008](adr/adr-0008-global-serving-and-genai-plane.md) |
| **Unstructured data** (transcripts, PDFs via ADLS Gen2 → RAG) | [13 §4](13-global-serving-topology.md) D7, [ADR-0008](adr/adr-0008-global-serving-and-genai-plane.md) |

## Scope notes & assumptions

- **Both domains in scope:** Canada Retirement (GWAM) + CoverMe — one pipeline, one canonical Adobe hit
  schema, per-domain source bindings ([02 §2](02-solution-architecture.md)). Landing points are per
  `new_data/README.md` and marked **provisional pending data-platform-owner confirmation**.
- **Latency honesty:** detection latency = source feed cadence (daily first). No "real-time" claims; the
  streaming upgrade path and its trigger criteria live in [ADR-0001 v2](adr/adr-0001-near-real-time-microbatch.md).
- **Hard gates before build:** production feed acquisition (≥30/90 days) and the PII classification review
  of the 24 flagged columns ([10 §3](10-data-profile-alignment.md)).
- Open items needing business input: feed refresh cadence/SLA per domain, holiday/campaign calendars, the
  labeled incident set for evaluation ([02 §7](02-solution-architecture.md)), and owners for the 29
  registry metrics.
- Diagrams are **Mermaid** (render in GitHub/VS Code; import to Lucidchart) per the agreed format.

> This package is a solutioning blueprint, not running code. Detection logic, thresholds, and Gen-AI prompts
> are illustrative and must be backtested/tuned on real feed history before production.
