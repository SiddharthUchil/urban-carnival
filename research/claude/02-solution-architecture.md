# 02 — Solution Architecture (End-to-End)

> Finalized GMAI–Pulse architecture for **Manulife Canada Retirement (GWAM) + CoverMe** on **Azure +
> Databricks** (single compute plane over two source platforms — [ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md)).
> Read [01-critique-and-synthesis.md](01-critique-and-synthesis.md) first for rationale and
> [10-data-profile-alignment.md](10-data-profile-alignment.md) for what the 2026-07-02 data profiling
> changed. Phase detail is in [03](03-phase1-anomaly-detection.md) / [04](04-phase2-investigation-insights.md);
> diagrams in [06-diagrams.md](06-diagrams.md); sources in [09-references.md](09-references.md).

## 1. Design principles

1. **Two planes, not two products.** *Offline/batch* owns baselines, training, deep investigation, and
   institutional memory. *Online* (scheduled micro-batch at feed cadence) owns fast detection and triage.
   They share one lakehouse, one model registry, one anomaly schema — the online plane is a thin, cheap
   scorer over models trained offline.
2. **Latency honesty.** Both feeds land as **at-rest tables** (Delta/Parquet on ADLS Gen2); no streaming
   collection exists today. Phase 1 is **batch-first micro-batch** at the feed's refresh cadence; a
   streaming hot lane is an explicitly optional Phase-3+ upgrade with defined trigger criteria.
   → [ADR-0001 (v2)](adr/adr-0001-near-real-time-microbatch.md).
3. **One compute plane, N source platforms.** Detection/ML runs **only on Databricks**; CoverMe's data is
   read from its ADLS files via a Unity Catalog external location (or Lakehouse Federation as fallback)
   while its existing **Synapse serverless SQL** surface stays untouched. → [ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md).
4. **One canonical Adobe hit schema, per-domain bindings.** Both domains emit the same ~1,198-column Adobe
   data-feed shape, decoded by one shared dictionary. Build one pipeline; vary only per-domain source
   bindings and configuration.
5. **Registry-driven metrics.** Detection targets are not hand-picked in code — they come from a versioned
   **metric registry** ([`metric-registry.yaml`](metric-registry.yaml)) seeded from the 29
   business-tagged "Anomaly Detection" dictionary rows and validated against
   `new_data/data_profile_summary.json`. → [03 §5](03-phase1-anomaly-detection.md).
6. **Two anomaly classes drive two detection styles.** *Operational/data-quality* anomalies (broken tags,
   zero-volume, freshness) are best caught by **declarative quality + built-in monitoring**, not ML.
   *Business/CX* anomalies (KPI spikes/drops, conversion shifts) need **forecasting + outlier models**.
7. **Detect cheaply, explain richly.** Detection must be fast/cheap and run on everything; expensive
   Gen-AI RCA runs **only on confirmed, severe anomalies** (a key cost lever).
8. **Three-layer platform; build for portability.** Mirror Manulife's enterprise AI stack — **Databricks**
   (data/training/detection) → **Adaptive ML / Adaptive Engine** (tune + serve the Gen-AI models, RLOps) →
   **Akka** (durable agentic runtime). Keep clean service boundaries now so Phase-2 agents/models can be
   tuned, served, and re-hosted across these layers without a rewrite. → [05](05-genai-and-akka.md),
   [07](07-adaptive-ml-integration.md).

## 2. Data grounding (two domains, one Adobe Analytics hit schema)

Where the data actually lives (confirmed 2026-07-02, [10](10-data-profile-alignment.md)):

| Domain | Landing | Query surface | Fully-qualified table |
|---|---|---|---|
| **Canada Retirement (GWAM)** | ADLS Gen2 → Delta/Parquet → Unity Catalog | Databricks SQL | `gwam_prod_catalog.inv_typed_common.adobe_hit_data` |
| **CoverMe** | ADLS Gen2 → external table/view | Synapse serverless SQL | `martech.adobe_coverme.hit_data` |

Both are the **canonical Adobe data-feed hit-level schema (~1,198 columns)**, already typed (no raw-TSV
parsing needed). The CoverMe data dictionary decodes the schema for **both** domains: 95.98% column
coverage, 100% eVar-slot and 100% prop-slot coverage. Profile-verified facts that shape the design:

- **~1,008 of 1,198 columns are empty slots** (Adobe pre-declares all eVar/prop/event slots; ~2% are used).
  Bronze→Silver **prunes to the populated set** per domain.
- **No metric columns exist.** Adobe emits KPIs implicitly via `post_event_list`; every detection KPI is a
  **downstream aggregate** of decoded events (`Purchase`, `Product View`, `Cart Open/Add/Remove`, form and
  quote events, …) at a time grain.
- **Wide-integer columns (`browser`, `country`, `color`, `mobile*`, …) are lookup-ID codes, not metrics or
  timestamps** — resolved via Adobe sidecar lookups; the profiler's `is_metric`/`is_timestamp` flags on
  them are false positives to ignore.
- **Hit timestamps are fully populated** (`hit_time_gmt`, `date_time`, `post_cust_hit_time_gmt` at 0%
  null), so the schema supports intraday grains — but with no production time-series yet, **daily is the
  only defensible starting grain**.
- **29 dictionary rows are business-tagged "Anomaly Detection"** (9 `data_feed_columns` + 8 `post_eVar` +
  12 `post_event_list`) → the seed of the [metric registry](metric-registry.yaml).
- **24 columns are sensitive-flagged** (`ip`, `cookies`, `visid_*`, visitor counters, `geo_zip`, `userid`,
  …) → hard governance gate, §9.

**Granularity:** KPI time series at **{domain × registry-metric × dictionary-evidenced dimension} × day**,
tightening to hour when a ≥30-day production feed confirms volume ([03 §1](03-phase1-anomaly-detection.md)).
**Data-quality gates** (from the profile) are mandatory in Silver: normalize `Status` casing
(`Enabled`/`ENABLED`), do **not** treat `post_eVar.Name` as unique, backfill missing `Friendly Name`
metadata, and confirm event-slot ids are **ids, not timestamps**.

## 3. Ingestion: batch-first micro-batch (two sources, one plane)

```
GWAM     : gwam_prod_catalog.inv_typed_common.adobe_hit_data   (UC/Delta, native)
             → Bronze(mirror+prune) → Silver (conform+decode+identity/privacy layer) → Gold (registry KPIs)
CoverMe  : martech.adobe_coverme.hit_data files on ADLS Gen2
             → UC external location (primary) / Lakehouse Federation `sqldw` (fallback)
             → Bronze(mirror+prune) → same Silver/Gold pipeline, per-domain config
Cadence  : scheduled Databricks Jobs at source refresh cadence (daily first → hourly as confirmed)
```

- **GWAM binding** is native: the table is already in Unity Catalog on Delta.
- **CoverMe binding** ([ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md)): **Option A
  (primary)** — UC storage credential + external location over the *same* ADLS files backing the Synapse
  external table (multi-engine reads of one location are supported). **Option B (interim)** — Lakehouse
  Federation connection `TYPE sqldw` exposing the Synapse database as a foreign catalog. Synapse serverless
  keeps serving its current consumers unchanged.
- **Schema contract tests** run in the Bronze ingest job (dual-engine drift guard, per ADR-0006).

**Future option (Phase 3+): streaming hot lane.** Adobe Edge Network → Event Forwarding → Azure Event Hubs
→ Structured Streaming (`trigger(processingTime)`) can add single-digit-minute latency **if** a business
SLA tighter than feed cadence is confirmed **and** intraday volume is validated on ≥30 days of production
data. Deliberately not built now — [ADR-0001 (v2)](adr/adr-0001-near-real-time-microbatch.md).

## 4. Medallion lakehouse

- **Bronze** — per-domain mirror of the typed hit tables, pruned to populated columns, append-only.
- **Silver** — conformed **canonical cross-domain schema**; **Identity & Privacy layer** — keyed
  pseudonymization at this boundary (§9, [11](11-privacy-identity-governance.md),
  [ADR-0007](adr/adr-0007-identity-privacy-layer.md));
  dictionary decode (`post_evarN`/`post_propN`/event ids → business names); sessionization where
  visit-level KPIs require it; DQ gates enforced via **Lakeflow (DLT) expectations** (`@dp.expect`
  warn/drop/fail). Tagging-completeness counters computed here.
- **Gold** — **registry-driven** per-KPI time series + feature tables (KPI ratios, tagging-completeness %,
  hour-of-week encodings) registered to **Unity Catalog Feature Engineering** for train/serve parity.
- **Anomaly / Insight tables** — `anomalies`, `anomaly_insights` (Phase-2 RCA reports), `change_events`
  (deployments/campaigns/outages — see the correlation-key gap, [04 §2](04-phase2-investigation-insights.md)),
  `analyst_feedback` (labels).

## 5. Component map (Azure + Databricks)

| Layer | Service | Notes (verified naming) |
|---|---|---|
| Source (GWAM) | **Unity Catalog / Databricks SQL** | `gwam_prod_catalog.inv_typed_common.adobe_hit_data` — native |
| Source (CoverMe) | **Synapse serverless SQL** (existing, untouched) | `martech.adobe_coverme.hit_data`; GMAI-Pulse only adds a reader |
| CoverMe access | **UC external location** (primary) / **Lakehouse Federation `sqldw`** (fallback) | [ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md) |
| Batch landing | **ADLS Gen2** | Both domains' files already land here |
| Ingest/ETL | **Databricks Jobs** (scheduled micro-batch) + **Lakeflow Spark Declarative Pipelines** (formerly DLT) | `trigger(availableNow)` semantics; **avoid `trigger(continuous)` — unsupported** |
| Storage/format | **Delta Lake** + **Unity Catalog** | Governance, lineage, RBAC |
| Metric config | **`metric-registry.yaml`** (versioned in-repo) | Seeded from 29 AD-tagged dictionary rows; validated against `data_profile_summary.json` |
| Features | **UC Feature Engineering** (Feature Store) | Train/serve parity |
| Models | **`darts`** (forecasting + `darts.ad`), **`pyod`** (ECOD/COPOD/IForest) | → [08-library-mapping.md](08-library-mapping.md) |
| ML lifecycle | **MLflow** (UC Model Registry, Tracing) | Versioning, batch scoring, Model Serving |
| Data-quality / drift | **Databricks Lakehouse Monitoring** + built-in **Anomaly Detection** (freshness/completeness, Public Preview) | Applies to **both** domains once landed in the medallion |
| Gen-AI | **Mosaic AI Agent Framework** + **Vector Search** (RAG) / **Azure AI Foundry Agent Service** | → [05](05-genai-and-akka.md) |
| Model tuning / serving (RLOps) | **Adaptive ML / Adaptive Engine** — RL-tune (PPO/GRPO/DPO) + serve open SLMs in private env | Manulife-selected; → [07](07-adaptive-ml-integration.md), [ADR-0005](adr/adr-0005-model-tuning-adaptive-ml.md) |
| Alerting | **Databricks SQL Alerts** → notification destinations (Email/Slack/**Teams**/**PagerDuty**/**Webhook**) → **Azure Logic Apps/Functions** | Webhook destination fans out |
| BI | **Databricks SQL / AI-BI dashboards**, Power BI | Analyst + exec surfaces |
| Future upgrade | **Adobe Edge Network + Azure Event Hubs + Structured Streaming** (hot lane) | Phase 3+ option only — [ADR-0001 (v2)](adr/adr-0001-near-real-time-microbatch.md) |
| Future runtime | **Akka** durable agents | → [05](05-genai-and-akka.md) |

## 6. MLOps: training, serving, drift, retraining

- **Train offline** on Gold. Per-KPI forecasters + a multivariate `pyod` model; log to MLflow with backtest
  metrics; register in UC. **Backtest** with `darts` `historical_forecasts()` before promotion.
- **Score in scheduled batch jobs**: the registered model runs as an MLflow batch scoring step at each
  ingest cadence (no in-stream retrain); low-latency REST via **Model Serving endpoints** where Phase-2
  agents need synchronous calls.
- **Drift & retraining**: **Lakehouse Monitoring** TimeSeries/InferenceLog profiles produce drift metrics
  (consecutive + baseline). Retrain on schedule (e.g., nightly/weekly) **and** on drift triggers. Built-in
  **Anomaly Detection** auto-models table **freshness** and **completeness** — used directly for the
  operational/tagging-health anomaly class on both domains' Bronze/Silver tables (near-zero custom code).
- **Gen-AI model tuning (RLOps)**: the Phase-2 RCA/insight **SLMs** are tuned + served via **Adaptive Engine**,
  continuously improved from the `analyst_feedback` reward signal; reward models + tuned SLMs are versioned in
  MLflow. Detection models (`darts`/`pyod`) are unaffected. → [07](07-adaptive-ml-integration.md).

## 7. Evaluation framework (both drafts omitted this)

> **Hard gate:** model selection, thresholds, and all backtest claims are **blocked until ≥30 (ideally 90)
> days of production hit-level feed** are available per domain. The profiled corpus is schema + dictionary
> only — nothing in it can validate a detector. Data acquisition is the #1 project blocker
> ([10](10-data-profile-alignment.md)).

Anomalies are rare and labels are weak, so metric choice matters.

| Concern | Choice | Why |
|---|---|---|
| Headline quality | **PR-AUC / average precision**, **precision@k** | ROC-AUC is misleading under extreme imbalance; precision@k matches a finite analyst review budget |
| Don't use | **Accuracy**, **point-adjusted F1** | Accuracy ~99.9% by predicting "never"; point-adjustment is statistically broken (a random scorer becomes "SOTA") |
| Time-series | **Range/event-based** precision/recall + **detection delay** | Credits detecting an event without the point-adjustment flaw |
| Operational | **MTTD/MTTR**, **alert precision / FP rate**, alert-fatigue watch | Every alert should be actionable (SRE) |

**Getting labels without ground truth:** (1) **backtest** on historical incidents (tickets/postmortems);
(2) **synthetic anomaly injection** (spikes, level shifts, dips) to estimate recall by type; (3) an
**analyst feedback loop** — confirm/dismiss writes to `analyst_feedback`, growing a human-validated set
that tunes thresholds and (later) supervises models.

## 8. Cost model & levers

Batch-first removes the two classic cost sinks (always-on streaming clusters, stream transport). What
remains is dominated by job compute and LLM calls:

- **Databricks DBU**: per-second × VM; SKU order **Jobs < Lakeflow < All-Purpose < Serverless**; Photon
  raises DBU. **Biggest lever: scheduled Jobs with `Trigger.AvailableNow` micro-batch semantics** — pay
  only while processing. No Event Hubs / streaming-cluster line items exist in Phase 1
  ([ADR-0001 v2](adr/adr-0001-near-real-time-microbatch.md)).
- **CoverMe access**: Option A (external location) costs plain ADLS reads; Option B (federation) pushes
  compute to the Databricks side with per-query overhead — fine for exploration, not for daily full scans.
- **LLM (Azure OpenAI / Foundry)**: token-based. **Batch API ≈ 50% off** for async RCA; **prompt caching**
  for repeated system prompts; **model-size tiering** (small model triages, large model only on hard cases);
  **gate RCA to confirmed anomalies only**. **Structural lever: serve an Adaptive-tuned open SLM** in-VPC as
  the default model (~50–80% cheaper, sub-second, PIPEDA-resident), reserving frontier LLMs for cold-start /
  hard cases. → [07](07-adaptive-ml-integration.md).

## 9. Governance, privacy & Responsible AI (PIPEDA / Law 25)

Canadian PII + insurance context → treat seriously:

- **Hard pre-ingestion gate:** the profiler flagged **24 sensitive columns** — network/device (`ip`, `ip2`,
  `ipv6`), cookie/visitor identifiers (`cookies`, `persistent_cookie`, `visid_high/low/…`, daily→yearly
  visitor counters), geo (`geo_zip`, `post_zip`), `userid`, `post_tnt`, social/account ids. A formal
  **data-classification review** of this list with the data-platform owners must complete **before any
  production ingestion**; heuristic detection is not a compliance audit.
- **PII minimization — Identity & Privacy layer** ([ADR-0007](adr/adr-0007-identity-privacy-layer.md)):
  **deterministic keyed pseudonymization** (HMAC-SHA-256, Key Vault-held key) of visitor identifiers at
  the Bronze→Silver boundary — preserves joinability and the future stitching option; non-identifier
  sensitive columns are dropped or generalized per classification outcome; drop/sanitize query-string
  PII; no PII into analytical or LLM contexts. Per-column disposition table:
  [11 §2](11-privacy-identity-governance.md).
- **Privacy & identity design** — Law 25 / Bill C-27 mapping, erasure/crypto-erasure, Synapse serverless
  secure-view governance, and the role model live in [11](11-privacy-identity-governance.md).
- **Access**: Unity Catalog RBAC + row/column masking; least-privilege on anomaly/insight tables.
- **Consent**: respect Adobe consent signals; exclude non-consented hits from analytics.
- **Audit & lineage**: UC lineage end-to-end; log every LLM prompt/response + retrieved evidence for RCA
  (supports Manulife Responsible AI Principles and future Akka governance/intent-logging).
- **LLM safety**: grounding + abstention + human-in-the-loop (see [04](04-phase2-investigation-insights.md)).

## 10. Alerting & routing

Detector writes to `anomalies` → **Databricks SQL Alert** evaluates severity → fans out via **Webhook**
notification destination to **Azure Logic Apps/Functions**, which route by severity to **Teams / email /
PagerDuty** and (for high severity) trigger the Phase-2 RCA agent + optional **ServiceNow/Jira** ticket.
**Debounce/dedup** (per registry-metric × domain × segment, cooldown window) prevents alert storms.

Segmentation uses **dictionary-evidenced dimensions** (campaign/attribution, referrer, page, geo, product
eVars/props) — not invented hierarchies. Note: **11 expected correlation keys are absent** from the current
schema (`url`, `page_path`, `deployment_id`, `release_id`, `campaign_id`, `content_id`, `tag_name`,
`owner`, `site_id`, `brand`, `region`); acquiring them is a Phase-2 entry criterion
([04 §2](04-phase2-investigation-insights.md)).

---

**Next:** [03 — Phase 1 (Anomaly Detection)](03-phase1-anomaly-detection.md) ·
[04 — Phase 2 (Investigation & Insights)](04-phase2-investigation-insights.md).
