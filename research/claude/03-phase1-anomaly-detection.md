# 03 — Phase 1: Anomaly Detection (Offline + Online)

> Detection design for GMAI–Pulse on **Canada Retirement (GWAM) + CoverMe** — one pipeline, per-domain
> bindings ([ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md)). Backbone in
> [02](02-solution-architecture.md); model/library choices in [08-library-mapping.md](08-library-mapping.md);
> thresholds rationale in [ADR-0002](adr/adr-0002-model-family-selection.md). Diagram D2 in
> [06-diagrams.md](06-diagrams.md).

## 1. Scope & pilot

Phase 1 surfaces **meaningful deviations** in both domains' signals and routes them as **explained
alerts**. The pilot starts **narrow** — one domain first (whichever produces a production feed first), a
focused anomaly class (**tagging-health / data-collection**) — then broadens to business KPIs. We design
for both classes from day one but **ship the operational class first** because it needs the least ML and
protects data trust.

> **⛔ Data-acquisition gate (the #1 blocker).** The profiled corpus is a 10-row schema sample + the data
> dictionary — **no time-series data exists yet** ([10](10-data-profile-alignment.md)). Baselines,
> seasonality, thresholds, backtests, and model selection are all **blocked until ≥30 (ideally 90) days
> of production hit-level feed** land per domain. Everything below is designed to be validated against
> that feed the day it arrives. Detection grain is **daily first**; intraday is enabled only when the
> feed confirms sufficient volume.

## 2. Detection taxonomy → method → tooling

| Anomaly class | Examples (both domains) | Method | Primary tooling |
|---|---|---|---|
| **Operational / data-quality** | event tag stops firing, `post_event_list` slot drops to zero, eVar population collapses, feed late/incomplete | **Declarative rules + statistical monitoring** (not heavy ML) | **Lakehouse Monitoring built-in Anomaly Detection** (freshness/completeness) + **Lakeflow expectations** + zero-volume rules |
| **Business / CX — univariate** | registry-KPI volume spikes or drops vs. expected (visits, `Purchase`, `Product View`, cart/form/quote events) | **Forecast + residual scoring** | **`darts`** `ForecastingAnomalyModel` + `QuantileDetector` |
| **Business / CX — multivariate** | unusual *combination* across KPIs/segments, conversion-rate shift not visible per-metric | **Tabular outlier detection** | **`pyod`** ECOD/COPOD (default), IsolationForest |

This split is the core of the design: **don't model what a rule catches**, and **don't rule-threshold what
needs a seasonal forecast**. Remember the schema reality ([02 §2](02-solution-architecture.md)): **no
metric columns exist** — every business KPI is an aggregate of decoded `post_event_list` events at the
registry-defined grain (§5).

## 3. Offline (batch) plane

Runs on the **authoritative Gold KPI tables** built by the scheduled ingest jobs ([02 §3](02-solution-architecture.md)).
Owns training, baselines, backtests, and the full daily re-scan. Silver preparation is **conform + decode,
not parse**: both domains arrive as typed tables, so the job prunes ~1,008 empty slot columns, applies the
Identity & Privacy layer (keyed pseudonymization — [11](11-privacy-identity-governance.md),
[ADR-0007](adr/adr-0007-identity-privacy-layer.md)), decodes eVar/prop/event slots via the shared dictionary, corrects the profiler's
lookup-ID-as-metric false positives, and sessionizes only where visit-level KPIs require it.

### 3.1 Univariate KPI forecasting (`darts`)
Per registry-metric × segment series at daily grain (hourly once intraday is enabled):

```python
from darts.models import ExponentialSmoothing          # Holt-Winters: weekly seasonality
from darts.ad import ForecastingAnomalyModel, NormScorer, QuantileDetector

anomaly_model = ForecastingAnomalyModel(
    model=ExponentialSmoothing(seasonal_periods=7),     # 7 days = weekly at daily grain (168 if hourly)
    scorer=NormScorer(ord=1),                            # |forecast - actual| residual
)
anomaly_model.fit(train_series, allow_model_training=True)
scores = anomaly_model.score(recent_series)             # residual anomaly score
detector = QuantileDetector(high_quantile=0.99)         # ADAPTIVE threshold, not fixed 0.65
detector.fit(scores_on_normal_window)
flags = detector.detect(scores)
```

- **Model choice by signal:** strong seasonality + holidays → `Prophet`; clean weekly/daily → `ExponentialSmoothing`/`Theta`; many series at once → **global** `NBEATSModel`/`TFTModel` trained across all segments (one model, future covariates for holidays). Pick via **backtest**, not by default — which requires the production feed (§1 gate).
- **Backtest before promotion:** `historical_forecasts()` / `backtest()` over moving windows; compare models with the [02 §7 metrics](02-solution-architecture.md) (PR-AUC, range-based F1 on labeled incidents).
- **Probabilistic option:** probabilistic forecasts + `GaussianNLLScorer` give calibrated, distribution-aware scores.

### 3.2 Multivariate / tabular outlier detection (`pyod`)
On engineered feature vectors (see §7) per {segment × time-bucket}:

```python
from pyod.models.ecod import ECOD     # parameter-free, fast, tail-aware — DEFAULT
clf = ECOD().fit(X_train_normal)      # also COPOD; IsolationForest for higher-dim interactions
scores = clf.decision_function(X_new) # works on NEW data → reusable at any cadence
```

- **Defaults:** **ECOD** then **COPOD** (parameter-free, cheap, interpretable per-dimension); **IsolationForest** when many engineered features interact. Avoid KNN/LOF/AutoEncoder for scheduled scoring (cost/tuning) unless justified.
- **Thresholds:** **PyThresh** (data-driven) instead of a fixed `contamination`.
- **Model selection:** consult **TSB-AD** (time-series) / **ADBench** (tabular) benchmarks; **MetaOD** for automatic unsupervised selection when labels are scarce. No single algorithm dominates — keep 2–3 and aggregate.

### 3.3 Operational / tagging-health (mostly no custom ML)
- **Lakehouse Monitoring → Anomaly Detection (Public Preview)** auto-builds per-table models for **freshness**
  (predicts next commit; flags stale feeds) and **completeness** (predicts expected 24 h row-count range;
  flags shortfalls). Results land in `system.data_quality_monitoring.table_results`. Applies to **both**
  domains' Bronze/Silver tables once landed in the medallion ([ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md)).
- **Lakeflow expectations** (`@dp.expect`, warn/drop/fail) on Silver track per-rule pass/fail counts → direct
  **tagging-completeness** signal (e.g., % of sessions with the expected event firing).
- **Zero-volume / coverage rules**: event slot active historically but now 0; eVar/prop population rate drop > X%.
- **Schema contract tests** on the CoverMe external location (dual-engine drift guard, ADR-0006).

### 3.4 Promotion & registry
Best model per signal → **MLflow UC Model Registry** with backtest metrics, scaler, and threshold config as
artifacts. Champion/challenger; promote on backtest + (later) analyst-feedback wins.

## 4. Online plane: scheduled micro-batch at feed cadence (intraday deferred)

The online plane is **the same pipeline run more often**, not a separate streaming system
([ADR-0001 v2](adr/adr-0001-near-real-time-microbatch.md)): a scheduled Databricks Job triggers on each
source refresh, updates Gold increments, scores them with the champion models, applies adaptive thresholds
+ debounce, and appends to `anomalies`.

```python
model = mlflow.pyfunc.load_model("models:/gmai_pulse_ecod@champion")
fresh = build_features(new_gold_increment)              # same Feature Store definitions as training
scored = fresh.withColumn("score", predict_udf(struct(*feature_cols)))
write_anomalies(apply_thresholds_debounce(scored))      # adaptive threshold + dedup → anomalies
```

- **Cadence:** matches the source feed (daily first; hourly when confirmed). Detection latency is honestly
  bounded by feed cadence — no "real-time" claims.
- **Scorers:** pre-fit `pyod` ECOD/COPOD **plus** lightweight **EWMA / robust-z (MAD) control charts** for
  instant spike/drop detection without model load.
- **Debounce/dedup:** per metric×domain×segment cooldown to prevent alert storms; severity from score
  magnitude + persistence.
- **Future (Phase 3+):** if the streaming hot lane is ever justified (criteria in ADR-0001 v2), the same
  scoring logic moves into Structured Streaming `foreachBatch` with `trigger(processingTime='1–5 min')` —
  never `trigger(continuous)`, which is unsupported on Databricks. The `anomalies.reconciled` field is
  reserved for reconciling that lane's approximate flags.

## 5. Metric registry (what we detect on)

Detection targets are **configuration, not code**: [`metric-registry.yaml`](metric-registry.yaml) is the
single versioned source of truth, seeded from the **29 dictionary rows business-tagged
`"Anomaly Detection"`** (9 in `data_feed_columns`, 8 in `post_eVar`, 12 in `post_event_list`) and
validated against `new_data/data_profile_summary.json`.

Registry entry schema:

| Field | Meaning |
|---|---|
| `metric_id` | Stable key, e.g. `coverme_purchase_daily` — referenced by models, thresholds, alerts, dashboards |
| `domain` | `coverme` \| `gwam_retirement` \| `shared` |
| `source_sheet` / `source_ref` | Dictionary sheet + column/eVar/event the metric derives from |
| `decode_rule` | How the raw slot becomes a KPI (e.g., count of event id per grain; eVar population rate) |
| `grain` | `daily` now; `hourly` reserved (schema supports it — timestamps are fully populated) |
| `direction` | `higher_is_good` / `higher_is_bad` / `context_dependent` (profiler taxonomy) |
| `owner`, `status` | Business owner; `active` / `candidate` / `deferred` |

> **Extraction note.** The profiles confirm the 9/8/12 counts but per-column aggregates cannot map which
> specific rows carry the tag — the registry ships with the schema + counted placeholders and an
> extraction step (one pandas pass over the dictionary workbook's `Notes` columns) listed as an open
> task in [10](10-data-profile-alignment.md). The `post_event_list` commerce events (`Purchase`,
> `Product View`, `Cart Open/Add/Remove`) are the expected first `active` entries.

Pilot ordering uses the profiler's **ad_readiness** scores per file/domain rather than hand-picking;
today those scores say *nothing is detection-ready* until the production feed lands (§1 gate).

## 6. Adaptive, seasonal thresholding (replaces fixed 0.65)

| Technique | Where | Purpose |
|---|---|---|
| `QuantileDetector` / `IQRDetector` | darts residual scores | Per-series adaptive cut, fit on normal window |
| PyThresh | pyod scores | Data-driven contamination |
| Robust z-score (median/MAD) | control charts | Outlier-resistant fast detection |
| Seasonal baselines (day-of-week → hour-of-week when intraday, holiday calendar) | feature/threshold layer | Avoid flagging expected peaks (e.g., campaign launch, month-end) |
| **Severity tiers** (warn / minor / major / critical) | post-score | Route only major+ to Gen-AI RCA and paging |

## 7. Feature engineering (Gold → Feature Store)

Per {domain × registry-metric × dictionary-evidenced segment} × day (hour reserved): KPI **levels** +
**ratios** (e.g., submit/start, conversion/visit — both derived from decoded `post_event_list` events),
**tagging-completeness %**, **deltas vs. seasonal baseline**, **lags** (t-1, t-7, t-28 at daily grain),
**day-of-week** + **holiday** encodings, traffic-source mix from campaign/referrer dimensions. Stored in
**UC Feature Engineering** for train/serve parity (the same features feed training and scheduled scoring).
Segment dimensions come from the profile's dimension shortlist (campaign/attribution, referrers, pages,
geo, product eVars/props) — provenance: `data_profile_summary.json`, not invention.

## 8. `anomalies` output schema (shared by both planes)

`anomaly_id` · `detected_at` · `plane`(scheduled|batch-rescan) · `domain`(coverme|gwam_retirement) ·
`metric_id`(registry key) · `segment` · `observed` · `expected` · `score` · `threshold` · `severity` ·
`class`(operational|business) · `model_uri` · `reconciled`(bool, reserved for future hot lane) ·
`status`(open|triaged|closed). Feeds alerting ([02 §10]) and Phase-2 investigation
([04](04-phase2-investigation-insights.md)).

## 9. Phase-1 success criteria

- **Data acquisition first:** a ≥30-day (target 90-day) production hit-level feed landed per domain, and
  the PII classification review completed ([02 §9]) — nothing else counts until these pass.
- Operational/tagging-health detection live on the pilot domain (freshness + completeness + coverage).
- Business-KPI detection on ≥1 registry metric with **backtested** PR-AUC/range-F1 reported and adaptive
  thresholds.
- Metric registry populated (29 seeded entries extracted and owner-confirmed) and driving Gold builds.
- Alert precision tracked from day one via the analyst-feedback loop; target low false-positive rate before
  widening scope. Detailed metrics in [02 §7](02-solution-architecture.md).
