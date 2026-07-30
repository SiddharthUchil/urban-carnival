-- CoverMe backfill verification
-- ============================================================================
-- Run this AFTER `databricks jobs run-now ... mode=backfill,start_date=2023-02-28`
-- completes all four tasks. Every query returns one row per check with a verdict
-- column, so you can paste the whole file into a Databricks SQL cell / notebook
-- and read down the `verdict` column: any FAIL or REVIEW needs an answer before
-- these KPIs are trusted.
--
-- Expected figures come from databricks/README.md "Expected (per the verified
-- EDA manifest)", which in turn comes from the verified 2026-07-27 EDA run.
--
-- Set your catalog once. Schemas are shared with GWAM; only table names carry
-- the product (conf/coverme_settings.py:61-68).
--
-- Parameter syntax differs by surface:
--   * notebook SQL cell  -- create a `target_catalog` text widget; ${...} resolves.
--   * SQL editor / query -- use a named parameter marker instead: USE CATALOG :target_catalog
--   * simplest           -- find/replace ${target_catalog} with the literal name.
-- `USE CATALOG` is Unity Catalog syntax; OSS Spark's parser rejects it, so every
-- statement below parses under plain Spark 3.5 but this first line does not.
-- ============================================================================
USE CATALOG ${target_catalog};

-- ---------------------------------------------------------------------------
-- 1. BRONZE volume and partition coverage
--    Expected ~57.7M rows over ~1,211 hit_date partitions from 2023-02-28.
--    The ~30 missing days are known feed gaps from the Databricks migration
--    (root-caused 2026-07-29), NOT site outages.
-- ---------------------------------------------------------------------------
WITH b AS (
  SELECT count(*) AS rows_total,
         count(DISTINCT hit_date) AS days_present,
         min(hit_date) AS first_day,
         max(hit_date) AS last_day,
         datediff(max(hit_date), min(hit_date)) + 1 AS days_in_span
  FROM gmai_pulse_bronze.adobe_hit_coverme
)
SELECT '1. bronze volume' AS check_name,
       rows_total, days_present, days_in_span,
       days_in_span - days_present AS interior_gap_days,
       first_day, last_day,
       CASE
         WHEN rows_total BETWEEN 50000000 AND 65000000
          AND days_present BETWEEN 1150 AND 1260
          AND first_day = DATE '2023-02-28'
           THEN 'PASS'
         WHEN rows_total < 50000000 THEN 'FAIL: bronze far below the 57.7M EDA baseline -- scope filter or source regression'
         WHEN first_day <> DATE '2023-02-28' THEN 'FAIL: first_day is not the 2023-02-28 backfill start'
         ELSE 'REVIEW: outside expected envelope (~57.7M rows / ~1,211 days)'
       END AS verdict
FROM b;

-- Gap-day count only. To list the actual dates (the README calls this "pending
-- SME confirmation" -- the data can answer it), swap the final SELECT for:
--   SELECT d FROM cal LEFT ANTI JOIN present USING (d) ORDER BY d
WITH bounds AS (
  SELECT min(hit_date) AS mn, max(hit_date) AS mx FROM gmai_pulse_bronze.adobe_hit_coverme
), cal AS (
  SELECT explode(sequence(mn, mx, INTERVAL 1 DAY)) AS d FROM bounds
), present AS (
  SELECT DISTINCT hit_date AS d FROM gmai_pulse_bronze.adobe_hit_coverme
)
SELECT '2. bronze interior gaps' AS check_name,
       (SELECT count(*) FROM cal) AS calendar_days,
       (SELECT count(*) FROM present) AS days_with_data,
       (SELECT count(*) FROM cal LEFT ANTI JOIN present USING (d)) AS gap_days,
       CASE
         WHEN (SELECT count(*) FROM cal LEFT ANTI JOIN present USING (d)) BETWEEN 20 AND 45
           THEN 'PASS: gap count matches the ~30 known migration feed gaps'
         WHEN (SELECT count(*) FROM cal LEFT ANTI JOIN present USING (d)) = 0
           THEN 'REVIEW: zero gaps contradicts the documented ~30 missing source days'
         ELSE 'REVIEW: gap count is not the expected ~30 -- re-check the source feed'
       END AS verdict;

-- ---------------------------------------------------------------------------
-- 3. Both production brand domains survived the scope filter.
--    This mirrors the backfill-only assertion in cm_01_bronze_ingest.py:87-97,
--    which already raises on a zero -- so this is a visible restatement.
--    insttrip is baseline-only (dead after 2024-03-11) and may legitimately be 0
--    if bronze was rebuilt without the baseline window.
-- ---------------------------------------------------------------------------
WITH u AS (
  SELECT lower(coalesce(nullif(page_url, ''), nullif(visit_start_page_url, ''),
                        nullif(first_hit_page_url, ''), nullif(post_page_url, ''), '')) AS url
  FROM gmai_pulse_bronze.adobe_hit_coverme
)
SELECT '3. brand domains present' AS check_name,
       count_if(url LIKE '%coverme.com%')        AS en_coverme_rows,
       count_if(url LIKE '%pourmeproteger.com%') AS fr_pourmeproteger_rows,
       count_if(url LIKE '%insttrip.manulife.com%') AS insttrip_baseline_rows,
       count_if(url LIKE '%uat.%' OR url LIKE '%localhost:5000%') AS excluded_leakage_rows,
       CASE
         WHEN count_if(url LIKE '%coverme.com%') = 0
           THEN 'FAIL: zero EN coverme.com rows -- scope filter regression'
         WHEN count_if(url LIKE '%pourmeproteger.com%') = 0
           THEN 'FAIL: zero FR pourmeproteger.com rows -- scope filter regression'
         WHEN count_if(url LIKE '%uat.%' OR url LIKE '%localhost:5000%') > 0
           THEN 'FAIL: excluded UAT/dev hosts leaked into bronze'
         ELSE 'PASS'
       END AS verdict
FROM u;

-- ---------------------------------------------------------------------------
-- 4. SILVER conformance ratio and event-list health
--    Expected: silver ~94% of bronze (Workspace-parity eligibility), and
--    post_event_list non-null ~0.93 against the 0.90 gate. Note cm_02 already
--    raises below 0.90 and only WARNS outside [0.90, 0.98] on eligible share --
--    so a run can succeed while sitting in the warn band. This surfaces that.
-- ---------------------------------------------------------------------------
SELECT '4. silver conformance' AS check_name,
       (SELECT count(*) FROM gmai_pulse_bronze.adobe_hit_coverme) AS bronze_rows,
       count(*) AS silver_rows,
       round(count(*) / nullif((SELECT count(*) FROM gmai_pulse_bronze.adobe_hit_coverme), 0), 4)
         AS eligible_share,
       round(count_if(post_event_list IS NOT NULL) / nullif(count(*), 0), 4)
         AS event_list_nonnull,
       round(count_if(language = 'en')      / nullif(count(*), 0), 4) AS share_en,
       round(count_if(language = 'fr')      / nullif(count(*), 0), 4) AS share_fr,
       round(count_if(language = 'unknown') / nullif(count(*), 0), 4) AS share_unknown,
       CASE
         WHEN count_if(post_event_list IS NOT NULL) / nullif(count(*), 0) < 0.90
           THEN 'FAIL: event_list_nonnull below the 0.90 gate (cm_02 should have raised)'
         WHEN count(*) / nullif((SELECT count(*) FROM gmai_pulse_bronze.adobe_hit_coverme), 0)
              NOT BETWEEN 0.90 AND 0.98
           THEN 'REVIEW: eligible share outside [0.90, 0.98] -- cm_02 warns but does not fail'
         WHEN count_if(language = 'en') = 0 OR count_if(language = 'fr') = 0
           THEN 'FAIL: a language bucket is empty -- domain rule or scope regression'
         ELSE 'PASS'
       END AS verdict
FROM gmai_pulse_silver.hits_conformed_coverme;

-- ---------------------------------------------------------------------------
-- 5. Identity columns are pseudonymised, not raw.
--    cm_02 applies a keyed SHA-256 to IDENTITY_COLS. A raw-looking value here
--    means the HMAC step silently no-opped.
-- ---------------------------------------------------------------------------
SELECT '5. pseudonymisation' AS check_name,
       count(*) AS sampled_rows,
       count(DISTINCT length(post_visid_high)) AS distinct_len_high,
       max(length(post_visid_high)) AS max_len_high,
       CASE
         WHEN max(length(post_visid_high)) = 64 THEN 'PASS: 64-char digest'
         WHEN max(length(post_visid_high)) IS NULL THEN 'REVIEW: column is all NULL'
         ELSE 'FAIL: post_visid_high is not a 64-char SHA-256 digest -- identity may be raw'
       END AS verdict
FROM (SELECT post_visid_high FROM gmai_pulse_silver.hits_conformed_coverme LIMIT 100000);

-- ---------------------------------------------------------------------------
-- 6. GOLD series count and calendar shape
--    Expected 53 distinct metric_id over a gap-free calendar. "No calendar gaps"
--    is trivially true because gold_lib zero-fills interior days -- see check 7,
--    which is the one that actually matters.
-- ---------------------------------------------------------------------------
SELECT '6. gold series' AS check_name,
       count(DISTINCT metric_id) AS n_series,
       count(DISTINCT hit_date) AS n_days,
       count(*) AS long_rows,
       count_if(value IS NULL) AS null_values,
       CASE
         WHEN count(DISTINCT metric_id) = 53 AND count_if(value IS NULL) = 0 THEN 'PASS'
         WHEN count_if(value IS NULL) > 0 THEN 'FAIL: NULL values in gold -- every series must be zero-filled or real'
         ELSE concat('REVIEW: expected 53 series, got ', cast(count(DISTINCT metric_id) AS string),
                     ' -- registry changed? check detect/cm_registry.py REGISTRY_VERSION')
       END AS verdict
FROM gmai_pulse_gold.kpi_daily_coverme;

-- ---------------------------------------------------------------------------
-- 7. ⚠ THE ZERO-FILL TRAP (README.md:169-182, known-wrong default still shipping)
--    gold_lib zero-fills the ~30 missing source days onto a gap-free calendar,
--    and nothing reads a gap mask. A zero-filled gap is indistinguishable from a
--    real zero-traffic day to anything reading gold. These dates MUST be masked
--    before any detector trains on this table.
--    Cross-check: an all-zero gold day that is absent from bronze IS a feed gap.
-- ---------------------------------------------------------------------------
WITH zero_days AS (
  SELECT hit_date
  FROM gmai_pulse_gold.kpi_daily_coverme
  GROUP BY hit_date
  HAVING sum(abs(value)) = 0
), bronze_days AS (
  SELECT DISTINCT hit_date FROM gmai_pulse_bronze.adobe_hit_coverme
)
SELECT '7. zero-filled gap days' AS check_name,
       (SELECT count(*) FROM zero_days) AS all_zero_gold_days,
       (SELECT count(*) FROM zero_days LEFT ANTI JOIN bronze_days USING (hit_date))
         AS confirmed_synthetic_days,
       (SELECT count(*) FROM zero_days LEFT SEMI JOIN bronze_days USING (hit_date))
         AS real_zero_traffic_days,
       CASE
         WHEN (SELECT count(*) FROM zero_days) = 0
           THEN 'PASS: no all-zero days, so nothing to mask'
         ELSE concat('REVIEW (expected): ',
                     cast((SELECT count(*) FROM zero_days LEFT ANTI JOIN bronze_days USING (hit_date)) AS string),
                     ' zero-filled synthetic day(s) present in gold. Mask these before training.')
       END AS verdict;

-- List them, so the mask has a source of truth.
WITH zero_days AS (
  SELECT hit_date FROM gmai_pulse_gold.kpi_daily_coverme
  GROUP BY hit_date HAVING sum(abs(value)) = 0
), bronze_days AS (
  SELECT DISTINCT hit_date FROM gmai_pulse_bronze.adobe_hit_coverme
)
SELECT '7b. synthetic day list' AS check_name, hit_date AS synthetic_day
FROM zero_days LEFT ANTI JOIN bronze_days USING (hit_date)
ORDER BY hit_date;

-- ---------------------------------------------------------------------------
-- 8. All 5 funnel events fire. cm_03:55-67 already raises on a zero total, so
--    this restates the assertion visibly and adds the per-series totals.
-- ---------------------------------------------------------------------------
SELECT '8. funnel events' AS check_name,
       metric_id,
       round(sum(value)) AS total_over_history,
       count_if(value > 0) AS days_firing,
       count(*) AS days_present,
       CASE WHEN sum(value) > 0 THEN 'PASS' ELSE 'FAIL: series is silent over all history' END AS verdict
FROM gmai_pulse_gold.kpi_daily_coverme
WHERE metric_id IN ('pel_228_quote_start', 'pel_229_quote_complete', 'pel_232_save_quote',
                    'pel_269_app_start', 'pel_240_app_confirm')
GROUP BY metric_id
ORDER BY metric_id;

-- ---------------------------------------------------------------------------
-- 9. Funnel conversion RATIOS -- the figure to compare against EDA S6b.
--    Gold funnel counts read BELOW the EDA totals by design (visit-distinct
--    post-eligibility vs hit-presence pre-filter), so compare ratios, never
--    raw totals. The funnel is non-monotonic by SME ruling (saved-quote resume),
--    so a ratio above 1.0 is expected behaviour, not a bug.
-- ---------------------------------------------------------------------------
WITH t AS (
  SELECT metric_id, sum(value) AS total
  FROM gmai_pulse_gold.kpi_daily_coverme
  WHERE metric_id IN ('pel_228_quote_start', 'pel_229_quote_complete', 'pel_232_save_quote',
                      'pel_269_app_start', 'pel_240_app_confirm')
  GROUP BY metric_id
), p AS (
  SELECT max(CASE WHEN metric_id = 'pel_228_quote_start'    THEN total END) AS quote_start,
         max(CASE WHEN metric_id = 'pel_229_quote_complete' THEN total END) AS quote_complete,
         max(CASE WHEN metric_id = 'pel_232_save_quote'     THEN total END) AS save_quote,
         max(CASE WHEN metric_id = 'pel_269_app_start'      THEN total END) AS app_start,
         max(CASE WHEN metric_id = 'pel_240_app_confirm'    THEN total END) AS app_confirm
  FROM t
)
SELECT '9. funnel ratios' AS check_name,
       round(quote_complete / nullif(quote_start, 0), 4)   AS quote_completion,
       round(app_start      / nullif(quote_complete, 0), 4) AS quote_to_app_start,
       round(app_confirm    / nullif(app_start, 0), 4)      AS app_completion,
       round(app_confirm    / nullif(quote_start, 0), 4)    AS end_to_end,
       CASE
         WHEN quote_start IS NULL OR app_confirm IS NULL THEN 'FAIL: a funnel endpoint is missing'
         WHEN app_confirm / nullif(quote_start, 0) > 1.0
           THEN 'REVIEW: end-to-end above 1.0 -- possible with non-monotonic resume, but sanity-check'
         ELSE 'PASS: compare these against EDA S6b ratios, not raw totals'
       END AS verdict
FROM p;

-- ---------------------------------------------------------------------------
-- 10. Language shares in gold match the ~50/50 EN/FR domain split.
--     Language is domain-derived and SME-approved 2026-07-29 (NOT eVar8).
-- ---------------------------------------------------------------------------
SELECT '10. language shares' AS check_name,
       metric_id,
       round(avg(value), 4) AS mean_daily_share,
       round(min(value), 4) AS min_share,
       round(max(value), 4) AS max_share,
       CASE WHEN sum(value) > 0 THEN 'PASS' ELSE 'FAIL: language bucket silent over all history' END AS verdict
FROM gmai_pulse_gold.kpi_daily_coverme
WHERE metric_id IN ('language_share_en', 'language_share_fr')
GROUP BY metric_id
ORDER BY metric_id;
