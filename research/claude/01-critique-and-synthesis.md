# 01 — Critique of Prior Research & Synthesis Rationale

> **Purpose.** This document evaluates the two prior research drafts (`research/gemini/` and
> `research/perplexity/`), then explains the judgment calls behind the finalized GMAI–Pulse
> solution. It is the "why" behind [02-solution-architecture.md](02-solution-architecture.md) and
> the phase designs. Citations are collected in [09-references.md](09-references.md).

## TL;DR verdict

Both drafts are **strong, convergent blueprints** and broadly agree on the right platform shape
(medallion lakehouse, `darts` + `pyod`, Event Hubs + Structured Streaming, MLflow, Unity Catalog,
a multi-agent Gen-AI layer for Phase 2, and a future Akka migration). **Neither is wrong; both are
incomplete and slightly dated in places.** Our finalized solution **keeps their shared backbone**,
**corrects four load-bearing technical claims**, and **adds the operational layer both omitted**
(latency-honest ingestion, adaptive thresholds, evaluation, drift/retraining, cost, governance,
LLM guardrails). A **2026-07-02 data-profiling reassessment** (`new_data/`) then re-grounded the design
on the actual landing zones — two domains on two platforms — and superseded several assumptions all three
earlier documents shared (see [the reassessment section below](#2026-07-02-reassessment--what-new_data-invalidated)
and [10-data-profile-alignment.md](10-data-profile-alignment.md)).

| | Gemini | Perplexity |
|---|---|---|
| Depth / rigor | **High** — explicit math (Cramér's V, Pearson residuals, Holt-Winters), Adobe TSV/sessionization, concrete incident example | Medium — clean structure, good breadth, lighter on specifics |
| Grounding in Adobe data | **Strong** (clickstream format, `post_*` tables, hitid de-dup) | Generic |
| Algorithm specificity | Isolation Forest + Holt-Winters only | Broader menu (ECOD/COPOD/AE, ARIMA/Prophet/RNN/TCN) but no per-KPI guidance |
| Gen-AI design | 4 specialized agents + Supervisor, concrete report | 4 agent roles, ChatOps + ticketing |
| Akka detail | **High** — 4-phase cutover, event sourcing | Lower — "expose APIs, model as agents" |
| Citations | Few | **Yes** (Databricks/MS/Adobe) |
| Online latency honesty | Assumes "Event Hubs/RT" works; no Adobe-feed reality check | Same gap |
| Cost / Eval / Governance | Missing | Missing |

**Net:** we adopt **Gemini's statistical rigor and Adobe grounding** as the technical spine, **Perplexity's
broader algorithm menu, citations, and ChatOps/ticketing** for breadth and operability, and we layer on
the missing production concerns ourselves.

> **Update (Adaptive ML).** Neither prior draft considered **Adaptive ML**, which Manulife publicly selected
> (Dec 22, 2025) as its model fine-tuning/serving layer — it post-dates their research. We add it as a
> net-new enhancement to the Gen-AI layer; see [07-adaptive-ml-integration.md](07-adaptive-ml-integration.md)
> and [ADR-0005](adr/adr-0005-model-tuning-adaptive-ml.md).

---

## What each draft proposed (condensed)

**Gemini** (`intial_gemini_research.md`, ~511 lines): Bronze/Silver/Gold medallion; Phase-1 offline =
`darts` Holt-Winters + `pyod` Isolation Forest in MLflow, daily retrain; Phase-1 online = Event Hubs →
Structured Streaming → **DLT continuous** → MLflow PyFunc UDF (score > 0.65); Phase-2 = Adobe-style
**Contribution Analysis** with Cramér's V + Pearson standardized residuals; Gen-AI = Supervisor +
SQL/System-Status/Interpretability agents producing an incident report; Akka = 4-phase
dual-write → port → stateful → cutover migration.

**Perplexity** (`intial_perplexity_research.md` + `perplexity_lucid_diagram.md`): a clean 2×2 matrix
(2 phases × online/offline); `darts` (ARIMA/Prophet/RNN/TCN) + `pyod` (IForest/COPOD/AE); ADF/Synapse →
ADLS → **DLT triggered** offline, Adobe RT APIs → Event Hubs → Autoloader online with rolling z-scores;
Phase-2 correlation joins + SHAP + a Gen-AI Insights Agent → narratives, dashboards, ServiceNow/Jira;
4 Gen-AI agent roles (Explainer, Root-Cause, Runbook Advisor, Knowledge Base); Akka via API exposure;
4 Lucid diagram specs.

---

## Strengths we carry forward

- **Medallion lakehouse + Delta** as the unifying substrate (both). Proven, governable, batch+stream.
- **`darts` (forecasting) + `pyod` (outlier detection)** as the dual model toolkit (both). Confirmed by
  our library research as the right split — forecasting for seasonal KPIs, tabular OD for cross-metric.
- **Cramér's V + Pearson standardized residuals for contribution analysis** (Gemini). This is exactly the
  Adobe "Contribution Analysis" pattern and is statistically sound for ranking which dimension values
  drove a categorical shift — we keep it verbatim and add SHAP for continuous features.
- **Session reconstruction / hitid de-dup grounded in real feed columns** (Gemini) — `hitid_high`,
  `hitid_low`, `visit_num`, `visit_start_time_gmt`. Still used where visit-level KPIs need it, though the
  TSV-parsing front-end Gemini assumed is obsolete: both domains now land as **typed tables**
  (see the 2026-07-02 reassessment below).
- **Multi-agent Gen-AI for Phase-2 RCA** (both) with **ChatOps + ticketing** (Perplexity). Matches the
  `ai-data-science-team` Supervisor pattern we researched.
- **Akka 4-phase strangler-fig migration** (Gemini). Sound shape; we tighten the guarantees (below).

## Four corrections (load-bearing, evidence-backed)

These are places where the prior drafts state something that our verified research contradicts. Each is
called out in the relevant design doc and in an ADR.

1. **"Online = Adobe Real-Time API / DLT continuous" → the feed cannot be a real-time source.**
   The **classic Adobe Analytics Data Feed is batch** (hourly/daily, "several hours … up to 12 hours or
   more," no SLA) — it **cannot** be the real-time hot path. Sub-10-minute latency requires collecting via
   the **Adobe Edge Network** (Web SDK / Edge Server API) and fanning out through **Event Forwarding** to
   Event Hubs. Our v1 answer split the architecture into a hot lane (Edge → streaming, minutes) and a cold
   lane (feed → Delta, authoritative). Neither draft made this distinction.
   *[Itself corrected 2026-07-02: the profiling showed no streaming collection exists for either domain,
   so the hot lane is now an explicitly deferred Phase-3+ option and batch-first micro-batch over the
   at-rest tables is the Phase-1 mode — see the reassessment below and
   [ADR-0001 (v2)](adr/adr-0001-near-real-time-microbatch.md).]*

2. **`trigger(continuous=…)` is experimental/unsupported on Databricks.** Gemini's "DLT continuous mode"
   for the online plane is not the recommended path. Use **fixed-interval micro-batch**
   (`trigger(processingTime=…)`) or the new **real-time mode** (`trigger(realTime='5 minutes')`, Public
   Preview, sub-second tail). Practical standard micro-batch floor ≈ 3–5 s. See [03](03-phase1-anomaly-detection.md).

3. **Gemini's fixed contamination threshold (0.65) is arbitrary and brittle.** KPI anomaly rates vary by
   segment and hour-of-week. Replace with **adaptive/quantile thresholds** (`darts` `QuantileDetector`,
   `pyod` + **PyThresh**) fit on a "normal" window, with seasonal awareness and severity tiers.
   See [ADR-0002](adr/adr-0002-model-family-selection.md).

4. **Akka "exactly-once execution" is overstated.** Akka's own docs warn workflow step retries are **not
   idempotent by default**; the real guarantee is **durable execution + at-least-once steps + developer-supplied
   idempotency/compensation (saga)**. Also, Akka is **JVM-only** — Python `pyod`/`darts` models are **not**
   in-process; they must be served behind **HTTP/gRPC** (MLflow Serving / Azure ML / FastAPI) and called from
   Akka **Workflows/Consumers**. See [05-genai-and-akka.md](05-genai-and-akka.md) and
   [ADR-0004](adr/adr-0004-akka-migration-strategy.md).

## 2026-07-02 reassessment — what new_data invalidated

The `new_data/` profiling run (data inventory, deep column profiles, joinability graph, ad-readiness
scores) invalidated five assumptions that Gemini, Perplexity, **and our own v1 synthesis** all shared.
Full traceability in [10-data-profile-alignment.md](10-data-profile-alignment.md).

| # | Invalidated assumption | New fact | Where fixed |
|---|---|---|---|
| 1 | **CoverMe lives on Databricks** | CoverMe lands in ADLS Gen2 behind **Synapse serverless SQL** (`martech.adobe_coverme.hit_data`); Synapse serverless runs no ML/streaming/monitoring. Detection stays on Databricks, reading CoverMe's ADLS files via UC external location (or `sqldw` federation) | [ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md), [02 §3](02-solution-architecture.md) |
| 2 | **A streaming hot lane is buildable in Phase 1** | Both feeds land as at-rest tables; no Edge/Event Hubs infra evidenced; no time-series data exists to validate intraday baselines | [ADR-0001 v2](adr/adr-0001-near-real-time-microbatch.md), [02 §3](02-solution-architecture.md) |
| 3 | **Raw TSV parsing + sessionization is the entry point** | Data is already **typed tables** (`adobe_hit_data`, `hit_data`); Bronze mirrors + prunes ~1,008 empty slot columns instead of parsing TSV | [02 §4](02-solution-architecture.md), [03 §3](03-phase1-anomaly-detection.md) |
| 4 | **Metric lists can be hand-authored from the dictionary** | The profiles give machine truth: 29 business-tagged "Anomaly Detection" rows, metric/dimension shortlists, grain hints, ad-readiness scores → a versioned **metric registry** replaces hand-picked lists | [`metric-registry.yaml`](metric-registry.yaml), [03 §5](03-phase1-anomaly-detection.md) |
| 5 | **Single-domain (CoverMe-only) scope** | Two domains, one canonical Adobe hit schema (dictionary decodes both at 95.98%/100%/100% coverage) → one pipeline, per-domain bindings | [02 §2](02-solution-architecture.md), README |

Also net-new from the profiles: hit timestamps are fully populated (intraday possible once volume is
proven); 24 sensitive columns require a **data-classification gate** before ingestion; **11 expected
correlation keys are missing**, deferring evidence-based root-cause correlation ([04 §2](04-phase2-investigation-insights.md)).
The single biggest blocker is unchanged by any architecture choice: **no production time-series exists yet**
— a ≥30-day (ideally 90-day) hit-level feed must be sourced before any model can be validated.

## Shared blind spots we add

Both drafts omit the layer that separates a slide-deck architecture from a production system:

| Gap | Why it matters | Where we address it |
|---|---|---|
| **Evaluation** | No precision/recall, no MTTD/MTTR, no way to know if detection works | [02 §Evaluation](02-solution-architecture.md) |
| **Adaptive thresholds** | Fixed thresholds cause alert storms or misses | [03](03-phase1-anomaly-detection.md), ADR-0002 |
| **Drift & retraining** | Web behavior + tagging change constantly | [02 §MLOps](02-solution-architecture.md) via Lakehouse Monitoring |
| **Cost model** | Streaming + LLM costs can balloon silently | [02 §Cost](02-solution-architecture.md) |
| **Governance / PIPEDA** | Insurance + Canadian PII; consent, RBAC, audit | [02 §Governance](02-solution-architecture.md) |
| **LLM guardrails** | RCA narratives can hallucinate causes | [04](04-phase2-investigation-insights.md), [05](05-genai-and-akka.md) |
| **Latency-honest ingestion** | "Real-time" on a batch feed is impossible | ADR-0001 |

## Synthesis decision summary

| Decision | Choice | Source of confidence |
|---|---|---|
| Substrate | Medallion lakehouse on Delta, Unity Catalog | both drafts + research |
| Compute plane | **Databricks only**, over both source platforms (CoverMe via UC external location / `sqldw` federation; Synapse untouched) | new_data profiling + [ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md) |
| Ingestion | **Batch-first micro-batch** over at-rest tables; streaming hot lane = deferred Phase-3+ option | new_data profiling ([ADR-0001 v2](adr/adr-0001-near-real-time-microbatch.md)) |
| Metric selection | Versioned **metric registry** seeded from 29 AD-tagged dictionary rows | profiling report + [`metric-registry.yaml`](metric-registry.yaml) |
| Forecasting | `darts` ForecastingAnomalyModel (ES/Theta/Prophet; global NBEATS/TFT) | library research |
| Tabular OD | `pyod` **ECOD/COPOD** (default), IForest (multivariate) | library research (ADBench/TSB-AD) |
| Tagging-health / freshness | **Databricks Lakehouse Monitoring built-in Anomaly Detection** + Lakeflow **expectations** | Databricks research |
| Thresholds | Adaptive/quantile + severity tiers | correction #3 |
| Phase-2 RCA | Cramér's V + Pearson residuals + SHAP; change-event correlation | Gemini + research |
| Gen-AI | LangGraph/Mosaic AI Supervisor + sub-agents; Vector Search RAG; grounded guardrails | GenAI research |
| Eval | PR-AUC, precision@k, **range-based** (not point-adjusted) F1, MTTD/MTTR | eval research |
| Future runtime | Akka durable agents (corrected guarantees), models via gRPC | Akka research |

Proceed to [02-solution-architecture.md](02-solution-architecture.md) for the end-to-end design.
