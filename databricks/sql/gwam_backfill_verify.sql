-- GWAM Canada Retirement backfill verification
-- ============================================================================
-- Run this AFTER the GWAM chain (01_bronze_ingest -> 02_silver_conform ->
-- 03_gold_kpis) completes with mode=backfill,start_date=2026-02-01. Every query
-- returns one row per check with a `verdict` column, so you can paste the whole
-- file into a Databricks SQL cell / notebook and read down that column: any FAIL
-- or REVIEW needs an answer before these KPIs are trusted.
--
-- Companion to sql/coverme_backfill_verify.sql, same conventions. Expected
-- figures come from databricks/README.md and the GWAM EDA (doc 15).
--
-- Parameter syntax differs by surface:
--   * notebook SQL cell  -- create a `target_catalog` text widget; ${...} resolves.
--   * SQL editor / query -- use a named parameter marker: USE CATALOG :target_catalog
--   * simplest           -- find/replace ${target_catalog} with the literal name.
--
-- WHAT IS AND IS NOT CHECKABLE HERE, because of the column policy:
--   * `rsid` and `post_page_url` exist in BRONZE only (conf/bronze_columns.py
--     KEY_COLUMNS). Silver drops them, so every scope check below reads bronze.
--   * `page_url` is in NEITHER layer -- the shipped `broad` mode (D12,
--     2026-08-04) filters on coalesce(page_url, post_page_url) at read time
--     but never lands page_url, so check 4 is partially blind (D4:
--     post_page_url is ~36.4% blank): it quantifies URL drift as REVIEW
--     rather than replaying the predicate as a hard FAIL. Only the D8
--     login-host rule keeps a FAIL verdict. An exact bronze-side replay
--     becomes possible only once page_url is landed (doc-16 5.2 item 2).
-- ============================================================================
USE CATALOG ${target_catalog};

-- ---------------------------------------------------------------------------
-- 1. BRONZE volume and partition coverage
--    README expects ~1,151,474 rows over ~157 process_date partitions from
--    2026-02-01. That figure is the en_only baseline: under the shipped
--    `broad` mode (D12) FR traffic is admitted too, so rows_total should land
--    ABOVE it -- the <500k FAIL floor still holds either way.
--
--    This check also SETTLES doc-16 backlog #4, an open discrepancy: the
--    unfiltered 2026-07-20 inventory put manulifeglobalprod's first day at
--    2026-03-10 (138 days), while BACKFILL_START and the earlier scoped EDA
--    runs say 2026-02-01 (~157 days). Backfilling from February simply yields
--    fewer partitions than the calendar span if the later date is right.
--    Whatever `first_day` reads here is the answer -- record it.
-- ---------------------------------------------------------------------------
WITH b AS (
  SELECT count(*) AS rows_total,
         count(DISTINCT process_date) AS days_present,
         min(process_date) AS first_day,
         max(process_date) AS last_day,
         datediff(max(process_date), min(process_date)) + 1 AS days_in_span
  FROM gmai_pulse_bronze.adobe_hit_gwam_ca_ret
)
SELECT '1. bronze volume' AS check_name,
       rows_total, days_present, days_in_span,
       days_in_span - days_present AS interior_gap_days,
       first_day, last_day,
       CASE
         WHEN rows_total < 500000
           THEN 'FAIL: far below the ~1.15M EDA baseline -- scope filter or source regression'
         WHEN first_day = DATE '2026-02-01' AND days_present BETWEEN 145 AND 175
           THEN 'PASS: 2026-02-01 start confirmed -- backlog #4 resolves to the EDA date'
         WHEN first_day = DATE '2026-03-10' AND days_present BETWEEN 125 AND 150
           THEN 'PASS: 2026-03-10 start confirmed -- backlog #4 resolves to the inventory date; update BACKFILL_START'
         ELSE 'REVIEW: first_day is neither documented candidate (2026-02-01 / 2026-03-10) -- record it and close backlog #4'
       END AS verdict
FROM b;

-- ---------------------------------------------------------------------------
-- 2. BRONZE interior gaps
--    Unlike CoverMe (~30 known migration feed gaps), GWAM has NO documented
--    missing days -- doc 15 profiled 2026-02-01..2026-07-08 with no gaps. Any
--    gap here is new information.
--    To list the dates, swap the final SELECT for:
--      SELECT d FROM cal LEFT ANTI JOIN present USING (d) ORDER BY d
-- ---------------------------------------------------------------------------
WITH bounds AS (
  SELECT min(process_date) AS mn, max(process_date) AS mx
  FROM gmai_pulse_bronze.adobe_hit_gwam_ca_ret
), cal AS (
  SELECT explode(sequence(mn, mx, INTERVAL 1 DAY)) AS d FROM bounds
), present AS (
  SELECT DISTINCT process_date AS d FROM gmai_pulse_bronze.adobe_hit_gwam_ca_ret
)
SELECT '2. bronze interior gaps' AS check_name,
       (SELECT count(*) FROM cal) AS calendar_days,
       (SELECT count(*) FROM present) AS days_with_data,
       (SELECT count(*) FROM cal LEFT ANTI JOIN present USING (d)) AS gap_days,
       CASE
         WHEN (SELECT count(*) FROM cal LEFT ANTI JOIN present USING (d)) = 0
           THEN 'PASS: gap-free, matching the doc-15 profile'
         ELSE 'REVIEW: interior gaps are NOT documented for GWAM -- gold zero-fills them, so these days will read as real zero-traffic days'
       END AS verdict;

-- ---------------------------------------------------------------------------
-- 3. SCOPE held: exactly one report suite
--    SCOPE_SUITE_MODE = 'current_only' (settings.py), so bronze must contain
--    manulifeglobalprod and nothing else. A second rsid means the suite mode
--    flipped -- which per D10 is a full re-baseline, never an incremental.
-- ---------------------------------------------------------------------------
SELECT '3. suite scope' AS check_name,
       count(DISTINCT rsid) AS distinct_rsids,
       max(rsid) AS rsid_seen,
       count_if(rsid <> 'manulifeglobalprod') AS foreign_suite_rows,
       CASE
         WHEN count(DISTINCT rsid) = 1 AND max(rsid) = 'manulifeglobalprod' THEN 'PASS'
         WHEN count_if(rsid = 'manugrs') > 0
           THEN 'FAIL: manugrs rows present -- SCOPE_SUITE_MODE flipped to with_legacy; every KPI re-baselines (D10)'
         ELSE 'FAIL: unexpected rsid in bronze -- the suite predicate did not hold'
       END AS verdict
FROM gmai_pulse_bronze.adobe_hit_gwam_ca_ret;

-- ---------------------------------------------------------------------------
-- 4. SCOPE held: URL predicate (broad, D12) and the D8 login-host exclusion
--    The live scope (D12, 2026-08-04) is SCOPE_URL_LIKE_BROAD --
--    '%/group-retirement%' OR '%/regimes-collectifs%' -- matched on
--    coalesce(page_url, post_page_url) at read time, minus
--    SCOPE_URL_LIKE_EXCLUDE and the six D8 hosts. Bronze lands only
--    post_page_url (~36.4% blank), so this check CANNOT replay the predicate
--    exactly: a row admitted via page_url may show a blank or divergent
--    post_page_url here. Off-pattern and noise counts are therefore REVIEW
--    (quantified drift), not FAIL. The D8 login-host rule keeps its hard FAIL
--    -- it applies in EVERY url/suite mode (01_bronze_ingest.py), and a
--    login-host page URL cannot legitimately reach bronze in any mode.
--    Note portail.manuvie.ca: the exclusion is an explicit host list, not a
--    '%portal%' pattern, precisely because the French host would not match one.
--    D14 (2026-08-04): that list is matched against the PAGE url only -- never
--    against link-target columns such as post_evar194.
-- ---------------------------------------------------------------------------
WITH u AS (
  SELECT lower(coalesce(post_page_url, '')) AS url
  FROM gmai_pulse_bronze.adobe_hit_gwam_ca_ret
)
SELECT '4. url scope (broad) + D8 login exclusion' AS check_name,
       count_if(url LIKE '%/group-retirement%' OR url LIKE '%/regimes-collectifs%') AS in_scope_rows,
       count_if(url = '') AS blank_url_rows,
       count_if(url <> '' AND url NOT LIKE '%/group-retirement%'
            AND url NOT LIKE '%/regimes-collectifs%') AS off_pattern_nonblank_rows,
       count_if(url LIKE '%adobeaemcloud.com%' OR url LIKE '%/ph/%') AS noise_host_rows,
       count_if(url LIKE '%portal.manulife.ca%' OR url LIKE '%id.manulife.ca%'
             OR url LIKE '%grsmembers.manulife.com%' OR url LIKE '%gsrs1.manulife.com%'
             OR url LIKE '%viproom.manulife.com%' OR url LIKE '%portail.manuvie.ca%') AS d8_login_rows,
       CASE
         WHEN count_if(url LIKE '%portal.manulife.ca%' OR url LIKE '%id.manulife.ca%'
                    OR url LIKE '%grsmembers.manulife.com%' OR url LIKE '%gsrs1.manulife.com%'
                    OR url LIKE '%viproom.manulife.com%' OR url LIKE '%portail.manuvie.ca%') > 0
           THEN 'FAIL: D8 login-host traffic leaked into bronze -- business rule violated in every mode'
         WHEN count_if(url LIKE '%adobeaemcloud.com%' OR url LIKE '%/ph/%') > 0
           THEN 'REVIEW: SCOPE_URL_LIKE_EXCLUDE noise on post_page_url -- benign only if page_url (unlanded) admitted these rows; investigate before trusting KPIs'
         WHEN count_if(url <> '' AND url NOT LIKE '%/group-retirement%'
                   AND url NOT LIKE '%/regimes-collectifs%') > 0
           THEN 'REVIEW: non-blank post_page_url outside SCOPE_URL_LIKE_BROAD -- expected ~0; page_url-vs-post divergence is the only benign source, a large count means the predicate drifted'
         ELSE 'PASS: every non-blank post_page_url matches the broad patterns; no D8 or noise host present'
       END AS verdict
FROM u;

-- ---------------------------------------------------------------------------
-- 5. SILVER conformance and event-list health
--    Silver is a projection of bronze with no eligibility filter (unlike
--    CoverMe), so the row counts must MATCH exactly. Gate is
--    event_list_nonnull >= 0.95 (02_silver_conform.py); EDA baseline ~1.0.
--    visid_pair_cardinality <= 1 is a documented, expected warning on this feed
--    (account-level ids) -- it is not a failure.
-- ---------------------------------------------------------------------------
WITH s AS (
  SELECT count(*) AS silver_rows,
         count_if(post_event_list IS NOT NULL) AS ev_nonnull,
         count(DISTINCT concat_ws('_', post_visid_high, post_visid_low)) AS visid_pair_card
  FROM gmai_pulse_silver.hits_conformed
), b AS (
  SELECT count(*) AS bronze_rows FROM gmai_pulse_bronze.adobe_hit_gwam_ca_ret
)
SELECT '5. silver conformance' AS check_name,
       bronze_rows, silver_rows,
       round(ev_nonnull / nullif(silver_rows, 0), 4) AS event_list_nonnull,
       visid_pair_card AS visid_pair_cardinality,
       CASE
         WHEN silver_rows <> bronze_rows
           THEN 'FAIL: silver row count differs from bronze -- GWAM silver applies no eligibility filter, so these must match'
         WHEN ev_nonnull / nullif(silver_rows, 0) < 0.95
           THEN 'FAIL: post_event_list non-null below the 0.95 gate -- every event series is understated'
         WHEN visid_pair_card <= 1
           THEN 'PASS (with the documented visid_pair_cardinality<=1 warning -- expected on this feed)'
         ELSE 'PASS'
       END AS verdict
FROM s CROSS JOIN b;

-- ---------------------------------------------------------------------------
-- 6. IDENTITY column state -- confirms which privacy branch actually ran
--    silver_lib.pseudonymize_expr emits a 64-char lowercase SHA-256 hex digest.
--    Raw mcvisid on this feed is a ~38-char digit string. So the shape of the
--    column tells you unambiguously which branch ran, and this check exists to
--    make the ADR-0007 deviation VISIBLE rather than assumed.
--
--    THIS BUILD RUNS pseudonymize=false, so 'raw' is the expected verdict.
--    Flip to pseudonymize=true and re-run silver+gold to change it; gold is
--    unaffected either way (the hash is deterministic -> distinct counts are
--    identical), so that flip re-baselines nothing.
-- ---------------------------------------------------------------------------
SELECT '6. identity column state' AS check_name,
       count_if(mcvisid RLIKE '^[0-9a-f]{64}$') AS hashed_rows,
       count_if(mcvisid IS NOT NULL AND NOT mcvisid RLIKE '^[0-9a-f]{64}$') AS raw_rows,
       count_if(mcvisid IS NULL) AS null_rows,
       CASE
         WHEN count_if(mcvisid RLIKE '^[0-9a-f]{64}$') > 0
          AND count_if(mcvisid IS NOT NULL AND NOT mcvisid RLIKE '^[0-9a-f]{64}$') = 0
           THEN 'PASS: pseudonymized (ADR-0007 satisfied)'
         WHEN count_if(mcvisid RLIKE '^[0-9a-f]{64}$') = 0
           THEN 'REVIEW: identity columns are RAW -- expected for this build (pseudonymize=false), an ADR-0007 deviation. Confirm it was intentional.'
         ELSE 'FAIL: mixed hashed and raw values -- silver was built across a flag flip; rebuild the whole window'
       END AS verdict
FROM gmai_pulse_silver.hits_conformed;

-- ---------------------------------------------------------------------------
-- 7. GOLD series count and calendar shape
--    Registry v0.7.0 builds 42 series (35 scored + 7 page-view candidates).
--    README still says 35 in places -- 35 is the SCORED count, not the built
--    count. gold_lib builds a gap-free calendar, so days_present here should
--    equal the calendar span by construction (check 8 tests what that hides).
-- ---------------------------------------------------------------------------
WITH g AS (
  SELECT count(DISTINCT metric_id) AS series_count,
         count(DISTINCT process_date) AS days_present,
         datediff(max(process_date), min(process_date)) + 1 AS days_in_span,
         count(*) AS long_rows,
         count_if(value IS NULL) AS null_values
  FROM gmai_pulse_gold.kpi_daily
)
SELECT '7. gold shape' AS check_name,
       series_count, days_present, days_in_span, long_rows, null_values,
       CASE
         WHEN series_count = 42 AND days_present = days_in_span AND null_values = 0
           THEN 'PASS: 42 series (35 scored + 7 page-view candidates), gap-free, no nulls'
         WHEN series_count = 35
           THEN 'FAIL: 35 series means the page-view family did not build -- registry or PAGE_VIEW_BASIS regression'
         WHEN null_values > 0
           THEN 'FAIL: null values in gold -- a ratio hit a zero denominator without the .where(den>0) guard'
         ELSE 'REVIEW: series count is neither 42 nor 35 -- reconcile against detect/registry.SERIES'
       END AS verdict
FROM g;

-- ---------------------------------------------------------------------------
-- 8. The ZERO-FILL TRAP
--    gold_lib builds a gap-free calendar and zero-fills days with no source
--    rows. A zero-filled gap is indistinguishable from a real zero-traffic day
--    to anything reading gold without a mask. This cross-joins all-zero gold
--    days against bronze partitions to separate the two.
--    Any row returned by the 8b variant is a SYNTHETIC day.
-- ---------------------------------------------------------------------------
WITH zero_days AS (
  SELECT process_date
  FROM gmai_pulse_gold.kpi_daily
  GROUP BY process_date
  HAVING max(abs(value)) = 0
), bronze_days AS (
  SELECT DISTINCT process_date FROM gmai_pulse_bronze.adobe_hit_gwam_ca_ret
)
SELECT '8. zero-fill trap' AS check_name,
       (SELECT count(*) FROM zero_days) AS all_zero_gold_days,
       (SELECT count(*) FROM zero_days LEFT ANTI JOIN bronze_days USING (process_date))
         AS synthetic_zero_days,
       (SELECT count(*) FROM zero_days SEMI JOIN bronze_days USING (process_date))
         AS genuine_zero_traffic_days,
       CASE
         WHEN (SELECT count(*) FROM zero_days) = 0 THEN 'PASS: no all-zero days at all'
         WHEN (SELECT count(*) FROM zero_days LEFT ANTI JOIN bronze_days USING (process_date)) > 0
           THEN 'REVIEW: synthetic zero-filled days exist -- list them with 8b and mask them before fitting any baseline'
         ELSE 'REVIEW: all-zero days that DO have bronze partitions -- genuine zero-traffic, worth explaining'
       END AS verdict;

-- 8b. List the synthetic days (empty result = nothing to mask).
WITH zero_days AS (
  SELECT process_date
  FROM gmai_pulse_gold.kpi_daily
  GROUP BY process_date
  HAVING max(abs(value)) = 0
), bronze_days AS (
  SELECT DISTINCT process_date FROM gmai_pulse_bronze.adobe_hit_gwam_ca_ret
)
SELECT '8b. synthetic zero-filled dates' AS check_name, process_date
FROM zero_days LEFT ANTI JOIN bronze_days USING (process_date)
ORDER BY process_date;

-- ---------------------------------------------------------------------------
-- 9. PAGE_VIEW_BASIS is inert, and the bucket shares partition
--    Shipped basis is 'all_hits', under which page_views_total is IDENTICAL to
--    hits_total by construction. That identity is the whole zero-re-baseline
--    guarantee (doc 20 Q6) -- if it does not hold, the basis moved and every
--    page-view series needs a full backfill with gold truncated.
--    The five pv_bucket shares are a partition over visits, so they sum to 1.0
--    on any day with traffic (and to 0.0 on a zero-traffic day).
-- ---------------------------------------------------------------------------
WITH p AS (
  SELECT process_date,
         max(CASE WHEN metric_id = 'hits_total'       THEN value END) AS hits_total,
         max(CASE WHEN metric_id = 'page_views_total' THEN value END) AS page_views_total,
         max(CASE WHEN metric_id = 'visits_total'     THEN value END) AS visits_total,
         sum(CASE WHEN metric_id LIKE 'pv_bucket_share_%' THEN value END) AS bucket_sum
  FROM gmai_pulse_gold.kpi_daily
  GROUP BY process_date
)
SELECT '9. page-view basis inert' AS check_name,
       count(*) AS days,
       count_if(abs(hits_total - page_views_total) > 1e-9) AS basis_mismatch_days,
       count_if(visits_total > 0 AND abs(bucket_sum - 1.0) > 1e-6) AS bucket_partition_violations,
       round(avg(CASE WHEN visits_total > 0 THEN page_views_total / visits_total END), 4)
         AS mean_pv_per_visit,
       CASE
         WHEN count_if(abs(hits_total - page_views_total) > 1e-9) > 0
           THEN 'FAIL: page_views_total <> hits_total -- PAGE_VIEW_BASIS is not all_hits; the zero-re-baseline guarantee is broken'
         WHEN count_if(visits_total > 0 AND abs(bucket_sum - 1.0) > 1e-6) > 0
           THEN 'FAIL: pv bucket shares do not sum to 1 -- the point_mass denominator is not visits_total'
         ELSE 'PASS: basis inert (page_views_total == hits_total), buckets partition cleanly'
       END AS verdict
FROM p;
