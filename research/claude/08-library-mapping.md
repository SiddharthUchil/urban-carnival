# 08 — Library & Repo Mapping

> How the four reference repos map to concrete roles in GMAI–Pulse, with recommended models per signal.
> Design context: [03](03-phase1-anomaly-detection.md) (detection), [04](04-phase2-investigation-insights.md)
> (RCA). Sources in [09-references.md](09-references.md).

## The four repos → roles

| Repo | Role in our solution | Where |
|---|---|---|
| **`yzhao062/pyod`** | Tabular / multivariate **outlier detection** | P1 multivariate + online scorer ([03 §3.2/§4](03-phase1-anomaly-detection.md)) |
| **`unit8co/darts`** | **Forecasting** + `darts.ad` residual anomaly detection | P1 univariate seasonal KPIs ([03 §3.1](03-phase1-anomaly-detection.md)) |
| **`yzhao062/anomaly-detection-resources`** | **Model-selection guidance** + benchmarks | Choosing/justifying models (this doc) |
| **`business-science/ai-data-science-team`** | **Multi-agent patterns** (Supervisor, SQL/analyst agents, HITL) | P2 Gen-AI RCA ([04 §2](04-phase2-investigation-insights.md), [05 §A.2](05-genai-and-akka.md)) |

## pyod — detector selection

All detectors share `fit(X)` → `decision_function(X)` / `predict(X)`, and **score new data**, so a batch-fit
model is reusable at any scoring cadence (and in a future streaming lane unchanged).

| Detector | Use for | Streaming cost |
|---|---|---|
| **ECOD** *(default)* | First-choice baseline; parameter-free, tail-aware on KPI counts | very cheap |
| **COPOD** | Parameter-free copula; interpretable per-dimension; pair with ECOD | very cheap |
| **IsolationForest** | Higher-dim engineered features (site×channel×page interactions) | cheap predict |
| KNN / LOF | Local-density anomalies (use sparingly; scaling/`k` sensitive) | costlier at scale |
| AutoEncoder | Many correlated KPIs / complex structure (needs GPU + tuning) | heaviest |

Helpers: **SUOD** (accelerate many detectors), **PyThresh** (data-driven thresholds instead of fixed
`contamination`), **LODA** (lightweight online), plus `TimeSeriesOD`/`MatrixProfile`/`SpectralResidual`.

## darts — forecasting + `darts.ad`

- **Forecasters:** `ExponentialSmoothing` (Holt-Winters daily/weekly), `Theta`, `AutoARIMA`, `Prophet`
  (holidays/level shifts); **global** `NBEATSModel`/`TCNModel`/`TFTModel` to train one model across many
  site×channel series with future covariates (holidays/campaigns).
- **`darts.ad` module:**
  - **Scorers:** `NormScorer` (non-trainable residual norm), `KMeansScorer`/`WassersteinScorer` (trainable),
    `GaussianNLLScorer` (probabilistic forecasts).
  - **ForecastingAnomalyModel** (wrap forecaster + scorer) / **FilteringAnomalyModel** (KalmanFilter/MA).
  - **Detectors:** `QuantileDetector`, `ThresholdDetector`, `IQRDetector` (+ `eval_metric()` for P/R/F1).
  - **Aggregators:** combine multiple binary anomaly series (Or/And). **`PyODScorer`** runs any pyod detector
    on a `TimeSeries` — the clean bridge between the two libraries.
- **Backtesting:** `historical_forecasts()` / `backtest()` (moving windows) before promotion.

## anomaly-detection-resources — model selection

Curated list; the parts we actually use: **ADBench** (30 algos / 57 tabular sets — *no single algorithm
dominates; supervision level matters more than model choice*), **TSB-AD** (40 algos / 1070 **time-series**
sets, NeurIPS 2024 — the go-to TS benchmark), and **MetaOD** / automatic unsupervised model selection
(NeurIPS 2021) for label-scarce selection. **Takeaway:** keep 2–3 detectors per signal and **aggregate**;
let benchmarks + backtests pick, not defaults.

## ai-data-science-team — agent patterns (maps ~1:1)

- `multiagents/supervisor_ds_team.py` → our **Supervisor** (LangGraph; JSON route schema; avoid re-calling a
  worker; reroute on empty data; recursion cap).
- `sql_database_agent.py` → our **Data/SQL Agent** (recommend steps → generate **read-only SELECT** → execute
  → return data+code; `node_func_human_review`, `node_func_fix_agent_code`).
- `data_visualization_agent` + `pandas_data_analyst` → **Contribution/Narrative** support.
- Repo practices we adopt: strip verbose tool/JSON messages + truncate history (`TEAM_MAX_MESSAGE_CHARS`) to
  avoid context bloat/rate limits; track `active_data_key` for data correctness; `human_in_the_loop` interrupt.

## Recommended model per registry-metric family (starting point — confirm by backtest)

Rows are keyed to [`metric-registry.yaml`](metric-registry.yaml) families, not hand-picked KPIs; all at
**daily grain** first (intraday models deferred until the ≥30-day feed confirms volume,
[03 §1](03-phase1-anomaly-detection.md)). Backtest confirmation is blocked on the same data gate.

| Registry family (source sheet) | Example metrics | Primary | Backup | Threshold |
|---|---|---|---|---|
| Traffic/behavioral columns (`data_feed_columns`, 9 tagged) | visits, visitors, page views | darts `ExponentialSmoothing` + `NormScorer` | global `NBEATS` | `QuantileDetector` 0.99 |
| Commerce/conversion events (`post_event_list`, 12 tagged) | `Purchase`, `Product View`, cart/form/quote event counts | darts `Prophet` (holidays) | `Theta` | `QuantileDetector` + severity |
| Conversion-context eVars (`post_eVar`, 8 tagged) + cross-KPI combos | eVar population rates, segment combinations | pyod **ECOD** | COPOD, IsolationForest | PyThresh |
| Tagging completeness / zero-volume (derived) | slot active→0, population-rate drops | Lakehouse Monitoring + Lakeflow expectations + rule | pyod ECOD on coverage features | rule + adaptive |
| Feed freshness / row-count (both domains) | stale feed, row shortfall | **Lakehouse Monitoring built-in Anomaly Detection** | — | built-in |

## Databricks integration notes

- **All four repos run on Databricks only** — Synapse serverless SQL cannot host ML; CoverMe data reaches
  the Databricks compute plane per [ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md).
- Run `pyod`/`darts` per-series via **pandas UDF / `applyInPandas`** (parallel across segments) for batch; use
  **global** darts models to train once across all series.
- Register the chosen model in **MLflow (UC)**; score in **scheduled batch jobs** at feed cadence
  ([ADR-0001 v2](adr/adr-0001-near-real-time-microbatch.md)), with a Model Serving endpoint for REST where
  Phase-2 agents need synchronous calls. Pin library versions in the model environment for train/serve
  parity. (PyFunc-in-Structured-Streaming applies only to the deferred Phase-3+ hot lane.)
