# GMAI-Pulse — Databricks pipeline (GWAM Canada Retirement + CoverMe)

Production medallion pipeline + scheduled anomaly detection for the GWAM Canada-Retirement
Adobe Analytics feed. Ports the locally-validated `detect/` prototype (5/5 injected scenarios
recalled, ≈2.3% business FP) onto Databricks without changing the detector code.
A second domain, **CoverMe**, shares the libs with its own conf + notebooks + job
(bronze→gold only for now — see the CoverMe section below).

```
source (read-only)                bronze            silver              gold
gwam_prod_catalog                 scoped +          conformed +         registry KPI series
.inv_typed_common      ──▶        pruned    ──▶     pseudonymized ──▶   + anomalies + run_meta
.adobe_hit_data                   mirror            + DQ                       │
                                                                              ▼
                                                              detect/run_detection (pandas+pyod)
```

Job DAG (one Databricks Workflow): `freshness_guard → bronze → silver → gold → detect`,
scheduled daily **06:00 America/Toronto**, created **PAUSED**.

## Layout
| Path | Purpose |
|------|---------|
| `conf/settings.py` | Table names, scope filter, windowing, secret/volume names; `resolve(dbutils)` reads job params |
| `conf/bronze_columns.py` | Column policy: required set, 24 sensitive cols dropped, optional full-width `POPULATED_COLUMNS` |
| `src/common.py` | sys.path setup, watermark, schema contract, freshness gate |
| `src/silver_lib.py` | Pure transforms: event-list normalization, HMAC pseudonymization, `event_ts` |
| `src/gold_lib.py` | `build_kpis_spark` — PySpark port of `detect/kpis.build_kpis` (parity-tested) |
| `src/00_freshness_guard.py` … `04_detect.py` | The five GWAM job notebooks |
| `jobs/gmai_pulse_daily.json` | GWAM Databricks Jobs definition (cron, cluster, params) |
| `conf/coverme_settings.py` | CoverMe: source table, URL-only scope, eligibility, windowing; same widget contract |
| `conf/coverme_bronze_columns.py` | CoverMe column policy (no `rsid`/`customer_perspective` in this feed; flagged eVars mirrored) |
| `src/cm_silver_lib.py` | CoverMe pure transforms: page_url-first coalesce, URL scope, domain language, hit eligibility |
| `src/cm_00_freshness_guard.py` … `cm_03_gold_kpis.py` | The four CoverMe job notebooks (no detect task yet) |
| `jobs/coverme_pulse_daily.json` | CoverMe Databricks Jobs definition |
| `../detect/cm_registry.py` | CoverMe series registry, pinned to `research/claude/metric-registry.yaml` v0.4.0 (CoverMe entries SME-confirmed at v0.3.0) |

## Prerequisites
- A writable Unity Catalog you can create schemas in (`<catalog>.gmai_pulse_bronze|silver|gold`).
- `SELECT` on the source `gwam_prod_catalog.inv_typed_common.adobe_hit_data`.
- Databricks CLI v0.2+ configured (`databricks configure`), run from this Windows box or any host.
- DBR **16.4 LTS** (or ≥15.4 LTS) job cluster; only extra library is **`pyod==2.0.5`** (on the
  detect task). `darts`/`statsmodels` are **not** needed — the detector uses neither.

## Deploy

### 1. Get the repo into the workspace (so `detect/*.py` import as modules)
**Preferred — Git folder** (arbitrary `.py` are importable):
```bash
# push this repo to a remote, then in the workspace:
databricks repos create <git-url> gitHub --path /Repos/<you>/anomoly-detection
# or update an existing one:
databricks repos update /Repos/<you>/anomoly-detection --branch main
```
The notebooks auto-resolve `repo_root` from their own path, so leave the `repo_root` job
parameter blank when deployed under `/Repos/...`.

**Alternative — import just the notebooks** (then set `repo_root` explicitly): import the repo,
and because `workspace import-dir` turns `.py` into notebooks (not importable modules), you must
also sync `detect/`, `databricks/conf/`, `databricks/src/*.py` (the non-notebook libs) as
**workspace files** and pass `repo_root=/Workspace/<path-to-repo>` as a job parameter. The Git
folder route avoids this — prefer it.

### 2. Provision the identity HMAC secret (ADR-0007)
```bash
databricks secrets create-scope gmai_pulse
databricks secrets put-secret gmai_pulse identity_hmac_key   # paste a long random key
```
Silver fails fast if this is missing.

### 3. Create the job
```bash
# edit jobs/gmai_pulse_daily.json first: replace __SET_ME__ (repo owner in notebook paths),
# __SET_ME_NODE_TYPE__ (e.g. Standard_DS3_v2), and __ALERT_EMAIL__.
databricks jobs create --json @databricks/jobs/gmai_pulse_daily.json
```
Set the real catalog on the job (or per-run): `target_catalog=<your_catalog>`. To update an
existing job, use `databricks jobs reset --job-id <id> --json @databricks/jobs/gmai_pulse_daily.json`.

## Backfill (first load) and smoke test
Run once with `mode=backfill` to load all history (2026-02-01 → latest):
```bash
databricks jobs run-now --job-id <id> \
  --job-parameters target_catalog=<catalog>,mode=backfill,start_date=2026-02-01
```
Expected (per EDA):
- **bronze** ≈ 1,151,474 rows across 157 `process_date` partitions.
- **silver** same row count; DQ prints `event_list_nonnull≈1.0`; a `visid_pair_cardinality<=1`
  warning is expected (account-level ids per EDA) — not a failure.
- **gold** `kpi_daily` = 35 series × 157 days (5,495 long rows), no calendar gaps.
- **detect** parity guard prints `unmatched=0 max_abs_diff≈0`; `anomalies` + `run_meta` populated.
- Re-run the same window → identical bronze/gold counts (idempotent `replaceWhere`).

After ≥1 successful backfill, un-pause the schedule (UI, or set `pause_status: UNPAUSED` and
`jobs reset`). Daily runs use `mode=incremental` (default): the freshness guard no-ops when no
new `process_date` has landed; otherwise bronze reprocesses the trailing 3 days for late
micro-batches.

## CoverMe pipeline (bronze → gold)
Second product domain, sharing `common.py`/`silver_lib.py`/`gold_lib.py`. DAG:
`freshness_guard → bronze → silver → gold` (4 tasks, **no detect yet** — the detector waits
for real gold baselines to calibrate thresholds). Job `gmai_pulse_coverme_daily`, same 06:00
America/Toronto cron, created **PAUSED**. Design spec:
`docs/superpowers/specs/2026-07-27-coverme-pipeline-port-design.md`.

Key differences vs GWAM (all EDA/SME-confirmed 2026-07-27):
- **URL-only scope** (single-suite feed, no `rsid` column): coverme.com + pourmeproteger.com,
  plus retired insttrip.manulife.com **date-bounded to `hit_date <= 2024-03-11`** (baseline
  history only — a resurrected host cannot re-enter go-forward scope), minus UAT/AEM noise.
  Matched on the blank-guarded **page_url-first** 4-way coalesce.
- **Hit eligibility at silver** (Analysis-Workspace parity per Adobe datafeeds-calculate):
  `exclude_hit = 0` and `hit_source not in (5,7,8,9)`; this feed has no `customer_perspective`
  column. Bronze keeps raw scoped rows. Baseline eligible share ≈ 94.33%.
- **Adobe metric keys**: visits = 4-part key (visid pair + visit_num + visit_start_time_gmt),
  visitors = visid pair (not mcvisid). **Event counts are visit-distinct** (SME ruling); the
  funnel is non-monotonic (saved-quote resume) — no within-visit sequencing anywhere.
- **Language is domain-derived** (en/fr/unknown from the URL host), not eVar8 — **SME-approved
  2026-07-29** (Kerrian), so this is the field of record rather than an interim rule. eVar8 is
  confirmed suspect (~96% EN against a ~50/50 domain split). *Forward note: she expects eVar149 to
  always be language and will confirm; if it becomes the field of record, rework
  `cm_silver_lib.lang_from_host_expr` and rebuild silver — the EN/FR shares would move.*
- Partition `hit_date`, `OVERLAP_DAYS=5` (late-arrival p99), backfill start **2023-02-28**.

Backfill — ↺ **UN-GATED 2026-07-29.** The PII/consent sign-off (readiness doc 17 item 9) that held
this is **cleared**: the CoverMe SME confirmed no PII comes from Adobe, and eVar65 is OneTrust
*cookie* consent (no PII, not an analytics-suppression flag), so opted-out rows may be included in
aggregate KPIs. The job still **ships PAUSED** — enabling the schedule is a deliberate manual action
in Databricks, not something this doc grants. The clearance was verbal; convert it to a written
data-owner approval if your governance process needs one on file.
**This is a first-ever deployment, not a `run-now` on an existing job** — the CoverMe job has
never been created. Pre-flight, in order; each row is a hard stop, not a nicety:

| # | Do this | How it fails if you skip it |
|---|---|---|
| 1 | Push the branch and register the repo as a Databricks **Git folder** at `/Repos/<owner>/anomoly-detection` (step 1 above) | arbitrary `.py` under `src/` will not import as modules |
| 2 | Provision `gmai_pulse` / `identity_hmac_key` (step 2 above) | silver raises `RuntimeError` — `cm_02_silver_conform.py:37-43` |
| 3 | Confirm `SELECT` on `csdo_prod_catalog.adobe_coverme_bronze.hit_data` — a **different** grant from the GWAM source | `assert_source_columns` raises — `cm_01_bronze_ingest.py:40` |
| 4 | Replace all four placeholders in `jobs/coverme_pulse_daily.json` (table below) | `resolve()` raises on the catalog; the rest fail at cluster/task creation |
| 5 | `databricks jobs create --json @databricks/jobs/coverme_pulse_daily.json` | — |

Placeholders — the committed JSON is a template on purpose, so these stay out of git:

| Token | Line(s) | Replace with | Must satisfy |
|---|---|---|---|
| `__SET_ME__` | 10 | your Unity Catalog name | writable; you can create the three `gmai_pulse_*` schemas in it. Left as-is, `coverme_settings.resolve()` raises rather than writing somewhere wrong. |
| `__SET_ME__` | 40, 51, 62, 73 | your Databricks Git-folder owner segment in `/Repos/<owner>/…` | must match the path from step 1 exactly, or the task cannot find the notebook |
| `__SET_ME_NODE_TYPE__` | 20 | a node type in your workspace's cloud (e.g. `Standard_DS3_v2` on Azure) | must exist in the workspace region |
| `__ALERT_EMAIL__` | 33 | on-failure notification address | — |

```bash
databricks jobs run-now --job-id <id> \
  --job-parameters target_catalog=<catalog>,mode=backfill,start_date=2023-02-28
```
Idempotent and safely re-runnable: bronze/silver use `replaceWhere hit_date >= start`, gold is a
full `overwrite`. `pause_status: PAUSED` gates only the **cron** — `run-now` works on a paused job.

Expected (per the verified EDA manifest):
- **bronze** `adobe_hit_coverme` ≈ 57.7M rows across ~1,211 `hit_date` partitions (~30 known
  missing source days — ↺ root-caused 2026-07-29 to the **Databricks migration** leaving a source
  file, most likely `hit_data`, un-refreshed; these are feed gaps, not site outages, and the exact
  date list is pending SME confirmation); the backfill run asserts both production brand domains
  are present.
- **silver** `hits_conformed_coverme` ≈ 94% of bronze; DQ prints `event_list_nonnull≈0.93`
  (gate is 0.90 here, not GWAM's 0.95).
- **gold** `kpi_daily_coverme` = 53 series × days, no calendar gaps; backfill sanity gate
  asserts all 5 funnel events fire and both language domains are present.
- Gold funnel counts read **below** the EDA S6b totals (visit-distinct post-eligibility vs
  hit-presence pre-filter) — expected, not a bug. Compare **conversion ratios**, not raw totals.

**Verify the run** with `sql/coverme_backfill_verify.sql` — 10 checks, each returning a `verdict`
column, covering every expectation above: bronze volume + partition coverage, interior gap count,
both brand domains present with no UAT leakage, silver eligible share + `event_list_nonnull`,
identity pseudonymisation, 53 gold series, the zero-fill trap, all 5 funnel events firing, funnel
ratios, and language shares. Set `target_catalog` and read down `verdict` for `FAIL`/`REVIEW`.

Two things it surfaces that the pipeline's own gates cannot:
- **Check 4** flags an eligible share inside the `[0.90, 0.98]` warn band. `cm_02:76-79` only
  *prints* a WARNING there, so a run can succeed while the `exclude_hit`/`hit_source` mix has moved.
- **Check 7** cross-joins all-zero gold days against bronze partitions to separate genuine
  zero-traffic days from `gold_lib`'s zero-filled synthetic ones, and **7b lists the dates** — which
  is the gap list the "Deferred" note below wants and the mask the detect task will need. Nothing in
  the pipeline detects interior gaps today.

## Local verification (dev box)
The gold KPI build is unit-tested for exact parity with the pandas detector, and the CoverMe
series kinds (visit-distinct counts, ratios, evar4 cube) have their own hand-computed suite:
```bash
pip install pyspark==3.5.1            # dev-only; needs a JDK (11/17)
python -m pytest tests/test_gold_parity.py tests/test_cm_gold.py tests/test_cm_silver.py tests/test_registry_yaml.py -q
```
On Windows set `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` to your venv python and
`SPARK_LOCAL_IP=127.0.0.1` (the test fixture does this automatically). The existing
`tests/test_detect.py` continues to guard detector recall/FP (≤3%).

## Notes & out of scope (v1)
- **Widen bronze** for Phase-2 investigation by pasting the EDA populated-column census into
  `conf/bronze_columns.POPULATED_COLUMNS` (sensitive columns stay excluded).
- **Alert delivery** (anomalies → Databricks SQL Alert → webhook → Teams/email) is designed in
  `research/claude/02-solution-architecture.md` but not wired here — add a SQL Alert on
  `<catalog>.gmai_pulse_gold.anomalies` filtered to `severity in ('major','critical')`.
- Not included: DLT/Lakeflow expectations, hourly grain, the CoverMe **detect** task
  (bronze→gold shipped; thresholds wait for real baselines), MLflow registry,
  dev/prod split, Asset Bundles. The pseudonymization is a keyed SHA-256 (not RFC-2104 HMAC);
  swap `silver_lib.pseudonymize_expr` for a `hashlib.hmac` UDF if governance mandates strict HMAC.

### Deferred (tracked)
- **Missing-day imputation policy** — `gold_lib` zero-fills the ~30 missing CoverMe source
  days on the gap-free calendar, while doc 17 §4 item 8's working assumption is "feed gaps →
  impute/interpolate". ~~Decision deferred until the SME rules on outage-vs-gap~~; zero-fill is
  the current, documented behavior and the detect task must mask those dates before training.
  ↺ **The outage-vs-gap question is ANSWERED (2026-07-29): they are feed gaps** — the Databricks
  migration left a source file un-refreshed. So the "impute/interpolate + mask before training"
  branch is confirmed correct and this is no longer waiting on anyone. **Still deferred, now on us:**
  zero-fill remains in force in code, which is the one known-wrong default still shipping — a
  zero-filled gap is indistinguishable from a real zero-traffic day to anything that reads gold
  without the mask. Two things to do when this is picked up: encode the confirmed gap dates
  somewhere the detect task reads (nothing does today — `cm_00_freshness_guard.py` checks the
  watermark only, not interior gaps), and decide imputation vs mask-only. Blocking neither the
  backfill nor the pipeline; it blocks trustworthy baselines.
  ↺ **The date list no longer needs the SME**: `sql/coverme_backfill_verify.sql` check **7b**
  derives it from the data — all-zero gold days that have no bronze partition are exactly the
  synthetic ones. Run it after the backfill and that half of the item is closed; what remains is
  the design decision (impute vs mask) and somewhere for the detect task to read the list from.
- **7 SME-confirmed feed columns not mirrored to bronze** (`campaign`, `geo_city`,
  `geo_country`, `geo_region`, `os`, `referrer`, `user_agent`; metric-registry
  `data_feed_columns`, Kerrian's calculated-metrics follow-up). Promoting any of them
  requires widening bronze and a source re-backfill — batch with the follow-up mapping.
