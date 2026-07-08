# ADR-0002 — Model families & adaptive thresholds

**Status:** Accepted · **Date:** 2026-06-30 · **Deciders:** GMAI–Pulse solutioning

## Context
GMAI–Pulse signals (Canada Retirement/GWAM + CoverMe) span three detection needs: **seasonal univariate
KPIs** (visits, conversions, funnel steps), **multivariate / cross-segment** anomalies, and
**operational / data-quality** anomalies (tagging, freshness, completeness). Gemini proposed a single
Isolation Forest with a **fixed 0.65 contamination threshold** — arbitrary and brittle across segments
and hour-of-week. Detection targets are enumerated in `metric-registry.yaml`
([03 §5](../03-phase1-anomaly-detection.md)), not hand-picked.

## Decision
- **Seasonal univariate →** `darts` **`ForecastingAnomalyModel`** (`ExponentialSmoothing`/`Prophet`/`Theta`;
  **global** `NBEATS`/`TFT` across many series) with residual scoring.
- **Multivariate / tabular →** `pyod` **ECOD** (default) / **COPOD** / **IsolationForest**.
- **Operational / data-quality →** Databricks **Lakehouse Monitoring built-in Anomaly Detection**
  (freshness/completeness) + **Lakeflow expectations** + simple zero-volume/coverage rules.
- **Thresholds:** **adaptive** — `darts` `QuantileDetector`/`IQRDetector`, `pyod` + **PyThresh**, streaming
  robust-z (MAD) — fit on a normal window, seasonal-aware, with **severity tiers**.
- **Selection:** keep 2–3 detectors per signal and **aggregate**; choose via **backtest** + **TSB-AD/ADBench**
  benchmarks and **MetaOD** when labels are scarce.

## Consequences
- (+) Right tool per signal; far fewer false positives than one fixed-threshold model.
- (+) Operational anomalies need almost no custom ML (managed monitoring). Lakehouse Monitoring applies to
  **both** domains once their data lands in the Databricks medallion per
  [ADR-0006](adr-0006-unified-databricks-compute-plane.md) — it is not available on Synapse serverless.
- (−) More models to train, register, and monitor; needs a backtest harness + drift-triggered retrain.

## Alternatives rejected
- **Single Isolation Forest + fixed threshold** (Gemini) — ignores seasonality; brittle.
- **Deep-learning-only** (LSTM/AutoEncoder everywhere) — cost + label/tuning burden unjustified for the pilot.
