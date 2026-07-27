# CoverMe Medallion Pipeline Port — Design Spec

Date: 2026-07-27
Status: approved (plan-mode review, Siddharth)
Scope: `databricks/` stages 00–03 + job JSON for the CoverMe domain. `04_detect` deferred.

## 1. Why

CoverMe is the second product onboarding to the anomaly-detection platform. EDA is complete and
verified (2026-07-27, doc-17 E1 closed), SME rulings are locked (doc 18 + `metric-registry.yaml`
v0.3.0), and `research/claude/17-coverme-eda-readiness.md` §5.B lists the medallion pipeline as
not started. `databricks/` is GWAM-hardcoded end-to-end. This port produces daily CoverMe KPI
series in gold as the foundation for the future detector.

## 2. Decisions

1. **Scope**: stages 00–03 + job JSON only. `04_detect` waits for real gold baselines to
   calibrate thresholds against.
2. **Architecture**: fork the thin notebooks (`cm_00`–`cm_03`), share the libs. GWAM files are
   untouched except additive `gold_lib.py` extensions guarded by the existing parity test.
3. **Hit eligibility (silver)**: full Analysis-Workspace parity per the Adobe data-feed doc
   supplied by Kerrian
   (<https://experienceleague.adobe.com/en/docs/analytics/export/analytics-data-feed/data-feed-contents/datafeeds-calculate>):
   `exclude_hit = 0 AND customer_perspective = 0 AND hit_source NOT IN (5,7,8,9)`.
   Bronze keeps raw scoped rows so the excluded volume stays observable.
4. **Registry**: new `detect/cm_registry.py` (SeriesSpec pattern), header-pinned to
   `research/claude/metric-registry.yaml` v0.3.0, which remains the governed source of truth.
5. **Backfill**: full history from `2023-02-28` (verified EDA basis: 57.7M scoped rows /
   1,211 daily points). `%insttrip.manulife.com%` stays in scope for baseline only —
   self-terminating after 2024-03-11 per SME ruling.

## 3. Metric semantics (Adobe doc + SME rulings)

- **Visits**: distinct `post_visid_high ‖ post_visid_low ‖ visit_num ‖ visit_start_time_gmt`
  (4-part key; Adobe recommends the 4th component against `visit_num` reuse — EDA found 0.29%
  duplicates on the adjacent 3-part key).
- **Visitors**: distinct `post_visid_high ‖ post_visid_low` (Adobe standard; not `mcvisid`).
- **Funnel events 228/229/232/269/240**: visit-distinct daily counts (SME ruling 2026-07-27).
  Funnel is non-monotonic — saved-quote resume means App Start/Confirm can fire in visits with
  zero quote events. Never compute within-visit sequences or step-drop.
- **Funnel ratios** (0-safe): 229/228, 232/228, 269/228, 240/269, 240/228 — population proxies
  only, not per-visitor conversion.
- **Language**: derived from coalesced-URL host (`pourmeproteger|manuvie|assurance-manuvie → fr`;
  `coverme.com|insttrip → en`; else unknown). eVar8 is not language-of-record (likely mis-tagged).
- **Not promoted**: events 510–514 (pending SME naming); event 20100 excluded from any
  value-based KPI (doc-17 E4).

## 4. Components

New files: `databricks/conf/coverme_settings.py`, `databricks/conf/coverme_bronze_columns.py`,
`detect/cm_registry.py`, `databricks/src/cm_silver_lib.py`,
`databricks/src/cm_00_freshness_guard.py` … `cm_03_gold_kpis.py`,
`databricks/jobs/coverme_pulse_daily.json`, `tests/test_cm_gold.py`.

Modified: `databricks/src/gold_lib.py` (additive: parameterized visit/visitor key exprs with
GWAM defaults, visit-distinct event basis, ratio series, event × dimension segmented counts),
`databricks/README.md`.

Key config: source `csdo_prod_catalog.adobe_coverme_bronze.hit_data`; partition `hit_date`;
URL scope = 2 prod domains + insttrip minus 7 UAT/AEM excludes; 4-way blank-guarded page_url-first
coalesce; `OVERLAP_DAYS = 5` (late-arrival p99); tables `adobe_hit_coverme` /
`hits_conformed_coverme` / `kpi_daily_coverme`; `DOMAIN = "coverme"`; tz pinned America/Toronto.

Silver DQ gates: hard-fail on 0 rows and `post_event_list` non-null < 0.90 (CoverMe baseline
93.2%; GWAM's 0.95 would false-fail); warn if eligible share leaves ~[0.90, 0.98]; backfill-only
sanity: both prod hosts non-zero, all 5 funnel events firing.

## 5. Governance

Job ships PAUSED. Production enablement gated on PII/consent sign-off (readiness item 9, open).
Sensitive columns dropped at bronze; identities pseudonymized at silver (keyed SHA-256, same
secret scope as GWAM).

## 6. Verification

Local: full pytest (new `test_cm_gold.py` + unchanged `test_gold_parity.py`).
Databricks backfill: bronze ≈ 57.7M rows / 1,211 days vs EDA manifest; silver eligible ≈ 94%;
gold funnel series non-zero with ratios spot-checked vs EDA S6b; language ≈ 50/50.
Expected offset: gold funnel counts read below EDA S6b (visit-distinct post-filter vs
hit-presence pre-filter) — documented, not a bug.
