# 15 — GWAM Canada Retirement: Consolidated EDA Report (both suites)

> **Purpose.** A single, stakeholder-facing account of the exploratory data analysis (EDA)
> performed for the GWAM Canada-Retirement anomaly-detection build. It explains, in plain terms,
> **what the two profiling notebooks did, which widgets and columns we used, and what every EDA
> step tells us about the data** — then goes to analyst depth on the findings.
>
> **Scope.** Two report suites in one source table:
> the **shipped** suite `manulifeglobalprod` (new site, profiled in
> [`eda/gwam_canada_retirement_eda.ipynb`](../../eda/gwam_canada_retirement_eda.ipynb)) and the
> **legacy** suite `manugrs` (old site, profiled in `eda/gwam_canada_retirement_eda_manugrs.ipynb`
> — notebook retired 2026-07-20, recoverable at `git show 408de5a:<path>`).
>
> ⚠️ **Superseded in part (2026-07-20):** "legacy" describes the *site*, not the suite. `manugrs`
> is still live — 320,304,305 hits through 2026-07-19, concurrent with `manulifeglobalprod`.
> See doc-16 §1 D7 / §2.
>
> **Data runs.** Production `gwam_prod_catalog`. New suite confirmed 2026-07-09, re-run 2026-07-12
> (adds the S4c URL audit). Legacy suite run 2026-07-10, independently re-run 2026-07-13 (all
> figures reproduced). **Grain of record: daily.** **Privacy regime: ADR-0007** (shape-only for
> sensitive columns). This document consolidates and does not replace the two analyses of record,
> [`12-eda-findings-analysis.md`](12-eda-findings-analysis.md) (new suite) and
> [`14-manugrs-cross-suite-analysis.md`](14-manugrs-cross-suite-analysis.md) (legacy / cross-suite);
> governance detail lives in [`10-data-profile-alignment.md`](10-data-profile-alignment.md) and
> [`11-privacy-identity-governance.md`](11-privacy-identity-governance.md).

---

## Executive summary

- **We profiled one giant Adobe Analytics table (3.18 billion rows, 1,198 columns, 357 GB) down to
  the small slice that is Canada-Retirement web traffic**, from two angles: the suite that is live
  today and the legacy suite it replaced.
- **The live suite (`manulifeglobalprod`) has only 158 days of history** (2026-02-01 → 2026-07-08,
  no gaps). That is enough to see the strong weekly rhythm (weekends ≈ 40 % of weekdays) but **too
  short to fit yearly/holiday seasonal models.**
- **The legacy suite (`manugrs`) recovers ~2.5 years of history** (2023-12-31 → 2026-07-07,
  5.57 M rows) and shows a **clean, datable cutover on 2026-02-01** — legacy traffic collapses in
  the exact month the new suite switches on. Splicing the two on suite-agnostic metrics lifts the
  158-day ceiling.
- **The business's filter column is wrong.** Scope is currently defined on `post_page_url`, which is
  **~37 % blank** on the new suite (**~48 %** on the legacy suite). The same filter finds **2–3×
  more traffic on `page_url`**. Recommendation, reconfirmed on both suites independently:
  **scope on `coalesce(page_url, post_page_url)`.**
- **The two suites tag *different* columns.** Only **12 of ~90 custom dimensions (eVars) are live in
  both**, so a naïve union would show high missing rates. Cross-suite splicing is safe only on
  suite-agnostic signals (hit/visit counts, geography, language, page names) — not on eVar KPIs.
- **The data is clean but anonymous.** No server-side bot filtering, near-real-time ~3-minute
  batches, Eastern-time timestamps, ~0 duplicate rate — but **no person-level identity** exists
  (`cust_visid` 100 % null, `userid` constant); analysis is device-level only, under strict
  privacy masking.
- **Every eVar's *meaning* is masked** (ADR-0007) and the only data dictionary on hand is for a
  *different* product (CoverMe). Several decisions below wait on the business supplying the GWAM
  eVar dictionary and confirming scope.

---

## 1. What we did, in plain terms

**One table, two report suites, two notebooks.** All Canada-Retirement web analytics land in a
single Adobe "hit-level" table, `gwam_prod_catalog.inv_typed_common.adobe_hit_data`. A "hit" is one
tracked interaction (a page view or click). The table mixes **every** Manulife/John-Hancock web
property; a *report suite* (`rsid`) plus a *URL filter* is how we carve out just Canada-Retirement.
We profiled two suites:

| Notebook | Report suite | What it is | Why we profiled it |
|---|---|---|---|
| `gwam_canada_retirement_eda.ipynb` | `manulifeglobalprod` | The **new** Storefront site, live today | It is the suite the anomaly detector will actually watch. |
| `gwam_canada_retirement_eda_manugrs.ipynb` | `manugrs` | The **legacy** pre-Storefront site | To recover history from before the new suite existed, and to test the manager's challenge that the filter/suite might be wrong. |

**The profiling philosophy.** The table is far too large to scan column-by-column, so each notebook:

1. Does exactly **one full-history scan** for exact daily volume (section S3), then
2. Draws a **5 % random sample** (seed 42) of a rolling profiling window and computes every
   per-column statistic on that sample — cheap, and representative for shares/percentages, while
   exact counts come from the full scans where it matters.
3. Emits each result as a machine-readable **`SHAREABLE` JSON block** so the findings can be lifted
   into these documents without re-running Databricks, plus an integrity **run-manifest** (byte
   length + sha1 per block).
4. Applies **ADR-0007 privacy** throughout: sensitive columns are reported **shape-only** (how full,
   how many distinct values, string lengths) — **never their values**.

The two notebooks are structurally identical; they differ only in the widget values that point them
at each suite. A companion notebook, `gwam_canada_retirement_charts.ipynb`, renders the same data
as interactive charts (traffic over time, day-of-week heatmap, country map, language mix, event
timeline, RRSP seasonality) — it is a visualization layer, not new analysis.

---

## 2. The widgets we used

The notebook is parameterized by **11 Databricks widgets** (dropdown/text inputs at the top). Ten
are tuning knobs; the two that define the population are `rsid_filter` and `url_filter`.

| # | Widget | Default (new suite) | What it controls |
|---|---|---|---|
| 1 | `table_fqn` | `gwam_prod_catalog.inv_typed_common.adobe_hit_data` | Which table to profile. |
| 2 | `window_months` | `13` | Length of the deep-profiling window (counts back from the last CA date). |
| 3 | `sample_fraction` | `0.05` | Sample size for per-column stats (5 %). |
| 4 | `col_batch_size` | `150` | Columns aggregated per pass (all 1,198 in batches). |
| 5 | `top_n` | `15` | Cap on how many top values to list per column. |
| 6 | `hourly_days` | `35` | Days used for the hour-of-day profile. |
| 7 | `max_csv_lines` | `450` | Row cap on emitted daily CSV series. |
| 8 | `top_events_k` | `6` | How many top events get their own daily series. |
| 9 | `cache_sample` | `false` | Whether to persist the sample dataframe. |
| 10 | **`rsid_filter`** | `manulifeglobalprod` | **The report suite to keep** (empty = off). |
| 11 | **`url_filter`** | `manulife.com/ca/en/personal/group-plans/group-retirement` | **The URL substring to keep** (empty = off). |

**The two runs differ only in the population widgets:**

| Widget | New suite (`manulifeglobalprod`) | Legacy suite (`manugrs`) |
|---|---|---|
| `rsid_filter` | `manulifeglobalprod` | `manugrs` |
| `url_filter` | `manulife.com/ca/en/personal/group-plans/group-retirement` | `manulifeim.com/group-retirement/ca/en` |

**What is *not* a widget (common misconception).** The sample **seed is hard-coded (42)**, the
window is **derived** from `window_months` (not explicit start/end dates), and there is **no
`SCOPE_URL_MODE` / `SCOPE_SUITE_MODE` toggle in the notebook** — those production scope toggles live
downstream in the Databricks pipeline ([`databricks/conf/settings.py`](../../databricks/conf/settings.py)),
not in the EDA. Explicit `start_date` / `end_date` and geo dropdowns exist only in the *charts*
companion.

---

## 3. How we scoped the data

The notebook picks the real column names defensively (`rsid` from `rsid|report_suite|reportsuite…`,
URL from `post_page_url|page_url`) and builds the scope as:

```
lower(trim(rsid_col)) == rsid_filter   AND   lower(url_col) contains url_filter
```

Either widget left empty drops that condition; a missing schema column is dropped **and flagged** in
the run metadata, so the notebook never silently profiles the wrong population. Two consequences the
audit sections (S4/S4b/S4c) quantify:

- The **URL contains-test silently excludes blank-URL hits**. On the new-suite window,
  `post_page_url` is blank on **88.2 %** of rows; scoping on it alone therefore throws away most of
  the table before the substring is even tested.
- Within the rsid-only window, the same English filter matches far more traffic on `page_url` than
  on `post_page_url`. **The recommended production scope column is `coalesce(page_url,
  post_page_url)`** — the live notebook still filters on `post_page_url` first, but the evidence (§5.3)
  says to switch. This recommendation is reconfirmed independently on both suites.

---

## 4. Which columns we profiled, and how we chose them

The census (S5) runs over **all 1,198 columns** on the 5 % sample and classifies each by how full it
is (Adobe writes **empty strings, not NULL**, so "full" means trimmed non-empty):

| Class | Rule (`pop_pct` = % non-blank in sample) | Meaning |
|---|---|---|
| **core** | `pop_pct ≥ 99` | Reliably populated → usable as a stable time series. |
| **populated** | `pop_pct ≥ 0.1` | Carries data, but not always present. |
| **sparse** | `0 < pop_pct < 0.1` | Almost always empty. |
| **dead** | `pop_pct = 0` | Never populated in this suite. |

Only ~16 % of the 1,198 columns carry any data for Canada-Retirement; the rest are dead for this
population. **Custom dimensions** (`evarN`/`post_evarN`, `propN`/`post_propN`) are collapsed to their
logical form and counted as "live" (≥0.1 %) and "core" (≥99 %). **Dimension candidates** for slicing
anomalies (S9) come from a fixed allowlist filtered to columns that are actually populated.

**Privacy (ADR-0007) governs how each column is printed:**

- **24 sensitive columns** (visitor/device IDs, IP addresses, fine geo/ZIP, user-agent, account IDs,
  personalization/social) are reported **shape-only** — never values, not even masked. A regex net
  (`visid|cookie|ip|user_agent|zip|userid|…`) catches any the list misses.
- A small **allowlist of low-cardinality technical dimensions** (`geo_country`, `geo_region`,
  `browser`, `os`, `connection_type`, `language`, `hit_source`, visit flags…) is printed **raw**.
- Everything else is **masked** as `<masked:sha1[:8]>`; emitted text is additionally scrubbed of
  emails, IPs, and long hex strings.

---

## 5. Section-by-section: what each EDA step tells us

Each notebook step (S0–S12) emits one named `SHAREABLE` block. The table maps step → data it reads →
what it tells us; the narrative below carries the numbers for both suites.

| Step | Title | Block | Key columns | What it informs |
|---|---|---|---|---|
| S0 | Config & helpers | — | (setup) | Widgets, privacy tables, scope + frame builders. |
| S1 | Unity Catalog discovery | `uc_discovery` | catalog metadata | Confirms the table resolves; finds candidate tables. Metadata only. |
| S2 | Delta metadata & cadence | `delta_meta` | `DESCRIBE HISTORY` | Table size and **write freshness** — no data scan. |
| S3 | Full-range daily volume | `daily_volume` | date, scope cols | The one full scan: **history depth, missing days, DOW & monthly shape**. |
| S4 | Window + sample frames | `window_frame` | date, scope cols | Builds the sample; cross-checks counts; measures blank-URL loss. |
| S4b | URL scope audit | `url_scope_audit` | `page_url`, `post_page_url` | **Which URL column to scope on**, and what the English filter excludes. |
| S4c | Multi-URL / no-third-suite | `url_column_audit` | 5 URL cols, `pagename` | No retirement traffic hides beyond the coalesce or beyond the two suites. |
| S5 | Population census | `population_census` | all 1,198 cols | **What is populated / core / dead**; live eVars & props. |
| S6 | Event decode | `event_decode` | `post_event_list` | **What events fire, how many per hit**, how IDs decode. |
| S7 | Live custom dims | `live_custom_dims` | live `evar`/`prop`/campaign | Shape + masked top values of live dimensions. |
| S8 | Time-series pack | `ts_daily` / `ts_events` / `ts_profiles` | date, visid, events, hour | The **model-ready daily series** (hits/visits/visitors) + DOW×hour profile. |
| S9 | Dimension candidates | `dim_candidates` | pages, geo, browser, language… | Cardinality + top values of **slicing dimensions**. |
| S10 | Data-quality baseline | `dq_baseline` | exclude_hit, skew, dupes | **Bot filtering, clock skew, duplicate rate, late arrival**. |
| S11 | Identity evidence | `identity_evidence` | visid / cust_visid / userid | **Whether any person-level ID exists** (it doesn't). |
| S12 | Synthesis spec | `synthesis_spec` | all prior blocks | One machine-readable spec for the synthetic-data generator. |

### 5.1 Source, scale & freshness (S1, S2)

One canonical table: **Delta, ~1,198 columns, ~357 GB, ~16,700 files, partitioned by
`process_date`, all columns typed `string`**, ~3.18 B rows total. Canada-Retirement is a **tiny
slice**: **1,159,010 rows (0.036 %)** on the new suite. Writes arrive as a **near-real-time
micro-batch — median inter-arrival ≈ 3 minutes** (37 K–472 K rows each), so freshness is minutes,
not hours. **There is no per-row ingest/load timestamp**, so late arrival can only be inferred from
write cadence, not measured directly.

### 5.2 Volume, history & seasonality (S3, S8)

| Signal | New suite (`manulifeglobalprod`) | Legacy suite (`manugrs`) |
|---|---|---|
| Rows (exact, full history) | 1,159,010 | 5,568,271 |
| Date range | 2026-02-01 → 2026-07-08 | 2023-12-31 → 2026-07-07 |
| Days present / missing | **158 / 0** | 821 / 99 (almost all "missing" are post-collapse) |
| DOW means Mon→Sun (hits) | 9751, 9141, 9115, 8602, 7681, **3523, 3441** | 7560, 7597, 7535, 7429, 6607, **2689, 2950** |
| Weekend vs weekday | ~40 % | ~37 % |
| Weekly autocorrelation | lag-7 = **0.72**, lag-28 = 0.62 | lag-7 = 0.61, lag-28 = 0.25 |
| Coefficient of variation | 0.52 | 0.97 (straddles the collapse) |

**Monthly CA totals (new suite):** Feb 256,530 · Mar 311,632 · Apr 216,952 · May 152,433 ·
Jun 166,999 · Jul 54,464 (partial). **RRSP season (Feb–Mar) is the annual peak.** The legacy suite
shows the same RRSP peak across **two** full seasons (Feb 2024 = 519,373, Feb 2025) — seasonality the
158-day new suite cannot show even once. **Takeaway for modelling:** a day-of-week-aware baseline is
mandatory; 158 days is too short for yearly decomposition; month-over-month comparisons will
false-alarm every RRSP season.

### 5.3 Scope & URL columns (S4, S4b, S4c) — the manager's filter challenge

| Signal (rsid-only window) | New suite | Legacy suite |
|---|---:|---:|
| `post_page_url` blank % | 36.98 % | 47.99 % |
| `page_url` blank % | 0.013 % | 0.0 % |
| Filter matches on `page_url` | 2,573,024 | 3,996,490 |
| Filter matches on `post_page_url` | 1,165,431 | 1,322,835 |
| Rows matched **only** via `page_url` | 1,407,600 | 2,673,658 |
| Retirement rows outside current filter | **+629,754 (+19.7 %)** | **+314,249 (+7.3 %)** |
| Recommended scope column | `coalesce(page_url, post_page_url)` | `coalesce(page_url, post_page_url)` |

**`post_page_url` misses 2×–3× of retirement traffic.** The addable traffic outside the current
English filter is largely **French `/ca/fr`** (new: 229,928 rows, top path
`/ca/fr/particuliers/regimes-collectifs` = 208,297; legacy: essentially all
`manulifeim.com/group-retirement/ca/fr`) plus some EN business/advisor group-plans pages. So the
French-scope decision now spans **two domains** (legacy `manulifeim.com` + new `manulife.com`).

**S4c closes the "is there a third suite?" question.** A window-wide, rsid-agnostic sweep of strict
retirement traffic (7,523,186 rows) splits **`manugrs` 57.30 % + `manulifeglobalprod` 42.70 % =
99.997 %** (residual 195 rows across three unrelated rsids). **No third suite carries retirement
traffic**, and none of the five URL columns holds retirement rows beyond the coalesce
(`first_hit_page_url`/`visit_start_page_url` "extras" are visit-entry attribution, not hit-level
pages; `site_url` is a single constant; `pagename` is ~40 % blank — unusable for scope).

### 5.4 Schema population & custom dimensions (S5, S7)

| Signal | New suite | Legacy suite |
|---|---:|---:|
| Populated columns (of 1,198) | 197 | 272 |
| Core (≥99 %) | 158 | 186 |
| Dead | 995 | 912 |
| Live eVars (logical) | 20 (15 core) | 96 (52 core) |
| Live props (logical) | prop51–57 (5 core) | 26 (14 core) |

The legacy suite lights up **more** columns because it carries a larger live-eVar set. **Cross-suite
overlap is small: only 12 logical eVars are live in both** (evar101–103, 105–109, 131, 138, 162,
200); **50 are legacy-only, 8 are new-only**. That is the "different columns are populated → high
missing rate on a naïve union" case the manager anticipated — *not* a rename. **All eVar *content*
is masked** (ADR-0007): e.g. `evar107` = `<internal>` free-text (card ~734, ≤262 chars), `evar140` =
`<hash>` (sensitive join-key), `evar200` ≈ visitor cardinality (identity-like). The only dictionary
in `new_data/` is the **CoverMe** suite (93 eVars) — a *different* product, **not authoritative
here**. The 12 shared eVars are the only splice candidates, and only after the business confirms each
means the same thing in both suites.

### 5.5 Events & KPI candidates (S6)

**`post_event_list` is populated on 100 % of hits.** New-suite hits carry **16–18 events (p50/p95),
max 22**; legacy hits carry **more, 32/37, max 41** — consistent with the legacy suite's larger
eVar set (each live eVar fires a presence flag). **Event IDs decode by the standard Adobe convention**
(not a suite dictionary): `20` = Campaign View, `500–504` = clickmap events, and **`10000+k` =
"Instance of eVar(101+k)"** (e.g. 10036 = eVar137). These instance-of-eVar events fire but carry
**no numeric value** (`has_value_pct = 0`) — they are presence flags. **No custom business events
(200–1000) are firing** in either suite. KPI-worthy events on the new suite: `ev500` "clickmap page"
15.8 %, `ev20` "Campaign View" 9.5 %, `ev501–504` ~3.9 % each.

> **Instrumentation change-points (new suite).** `ev501–504` start **2026-02-24**; `ev20` starts
> **2026-03-03**; `ev500` fires **only 2026-04-02 → 2026-06-15** then stops. These are tag changes,
> not anomalies — the detector must treat them as known change-points, not alerts.

> **Note on the notebook's event labels.** The notebook has **no custom-event lookup** loaded, so
> IDs print as "unknown" (0 of 23 resolved on the new-suite re-run, 0 of 40 on the legacy re-run).
> That is a display gap, not a data gap — the decode convention above is authoritative.

### 5.6 Geography, language & slicing dimensions (S9)

| Dimension | New suite | Legacy suite |
|---|---|---|
| `geo_country` | **CAN 84.8 %, USA 11.5 %**, HKG 0.6 % | **CAN 95.7 %, USA 3.3 %** |
| `geo_region` | ON 46.7 %, AB 15.3 %, BC 11.5 %, QC 3.6 % | ON 51.6 %, AB 16.2 %, BC 13.6 %, QC 4.5 % |
| `language` | 45≈EN 63.3 %, 39≈FR 30.1 % | 45≈EN 53 %, 39≈FR 40.5 % |
| `pagename` | `ca-ret:personal:overview` 57.5 %, `…enrol-now` 13.9 % | `crt-public:home` 40.5 %, `…enroll-now` 8.9 % |

Two business flags: **the new suite's 11.5 % US share** (vs the legacy 3.3 %) is worth checking — real
users, or internal/test traffic? And **`language` values are Adobe numeric lookup IDs** (45/39), not
ISO codes — the EN/FR mapping needs confirmation. Note the `pagename` namespace was **renamed across
the migration** (`crt-public:` → `ca-ret:personal:`), more evidence the suites are distinct
instrumentation. **Caveat:** geo columns are populated in the *source* table but are **not yet in the
production bronze layer** — surfacing geo KPIs requires widening `databricks/conf/bronze_columns.py`.

### 5.7 Data quality (S10)

Uniformly clean on both suites: **key-column null/blank ≤ 0.024 %**; **zero server-side bot
filtering** (`exclude_hit × hit_source` = (0,1) for 100 % of hits, so `clean_hits == hits` — bots are
*not* removed, we must own that heuristic); **clock skew p5/p50/p95 = −5h/−4h**, confirming
`date_time` is **Eastern (EST/EDT)** and `hit_time_gmt` is epoch GMT; **duplicate rate ≈ 0** on an
exact one-day check (e.g. new suite 2026-07-07: 9,113 rows / 9,113 distinct keys). No load-timestamp
column ⇒ late arrival is inferred from S2 cadence.

### 5.8 Identity & privacy (S11, ADR-0007)

**There is no person-level identity.** `cust_visid` / `post_cust_visid` are **100 % null**;
`userid` / `username` / `user_hash` / `cookies` are **constant (cardinality 1)**. Visitors are
approximated device-level from `mcvisid` (new suite card ≈ 49,975; legacy ≈ 52,072) and `visid_*`.
Daily ratios: **~1.36 hits/visit, ~1.09 visits/visitor** (new); ~1.43 / ~1.08 (legacy). Under
**ADR-0007**, visitor/device IDs are **pseudonymized** with keyed HMAC-SHA-256 (key in Azure Key
Vault, deterministic, key-versioned for crypto-erase); IPs are dropped; fine geo/ZIP is generalized
to region; user-agent is generalized to browser family. **Pseudonymized data is still personal
information** under Law 25 / PIPEDA, so masking and RBAC stay mandatory, and a **PIA is required
before production ingestion**. Person-level stitching is **deferred** pending a MarTech ask (capture
a login/customer ID in a dedicated eVar) and classification approval.

---

## 6. The cross-suite story — a clean, datable cutover

Monthly CA hits around the handover show the migration end-to-end:

| Month | `manugrs` (legacy) | `manulifeglobalprod` (new) |
|---|---:|---:|
| 2025-12 | 227,951 | — (0) |
| 2026-01 | 300,898 | — (0) |
| **2026-02** | **10,806** | **256,530** |
| 2026-03 | 2,136 | 311,632 |
| 2026-04 | 13 | 216,952 |
| 2026-05 | 11 | 152,433 |
| 2026-06 | 5 | 166,999 |
| 2026-07 | 3 | 54,464 (partial) |

The legacy suite ran healthy from **Jan 2024 → Jan 2026** (~100–520 K hits/month) and **collapsed in
Feb 2026 exactly as the new suite switched on (2026-02-01)** — one population handed to another, not
two overlapping feeds. Legacy is a **material suite in its own right** (3rd-largest rsid in the
window at 7.8 %; the new suite is not even top-10). **The prize is history depth:** splicing the
pre-Feb-2026 legacy series to the new suite yields **~2.5 years** instead of 158 days — enough to fit
the seasonal/holiday models the new suite alone cannot support — *but only on suite-agnostic metrics*,
with **2026-02-01 encoded as a hard change-point**.

---

## 7. Implications for the anomaly-detection build

| Finding | Design consequence |
|---|---|
| 158 days, strong weekly cycle, RRSP monthly peak | **Day-of-week-aware baseline** (trailing same-weekday median / seasonal-naïve); short-memory methods only; no yearly decomposition yet. |
| Clean, datable cutover at 2026-02-01 + tag change-points (2026-02-24, 2026-03-03, ev500 window 2026-04-02→06-15) | Encode these as **known change-points, excluded from training**; never compare eVar KPIs across the suite cutover. |
| ~2.5 yr recoverable under `manugrs` | Splice **suite-agnostic KPIs** (hits, visits, geography, language) to lift the 158-day ceiling; keep old/new eVars as separate series. |
| Only 12 shared eVars; content masked | eVar-derived KPIs are **not** cross-suite-spliceable until the dictionary confirms shared meaning. |
| No bot filtering; device-level identity only | Own the bot heuristics; treat visits/visitors as approximate. |
| `post_page_url` ~37–48 % blank | Scope production on **`coalesce(page_url, post_page_url)`**. |
| Presence-only events | KPIs are **counts/rates**, not event values. |

The production pipeline already carries the switches to act on this — a `SCOPE_URL_MODE`
(`en_only` default vs `broad` French/business coverage) and a `SCOPE_SUITE_MODE`
(`current_only` default vs `with_legacy` union on the coalesce column) in
[`databricks/conf/settings.py`](../../databricks/conf/settings.py) and
[`databricks/src/01_bronze_ingest.py`](../../databricks/src/01_bronze_ingest.py) — **held off until
the business signs off scope**, because flipping either re-baselines every KPI.

> **On alert thresholds.** The EDA sets the *baseline shape* (DOW-aware, change-point-marked); it does
> **not** set alert magnitudes. The level-shift sensitivity figure (×1.9) used elsewhere comes from
> the separate detectability-floor analysis in the `detect/` engine, **not** from this EDA — do not
> attribute it here.

---

## 8. Open questions for business / analytics owners

**Scope & history**
1. Confirm **2026-02-01** as the migration date and whether `manugrs` history is authoritative for
   backfill. Union the legacy suite for training (≈2.5 yr, at the cost of a change-point) or start
   clean at the new suite (158 days)?
2. Should scope include **French `/ca/fr`** traffic? It is now a **two-domain** decision
   (`manulifeim.com` legacy +314 K rows, `manulife.com` new +230 K rows).

**Metric semantics**
3. Provide the **GWAM eVar dictionary** for *both* suites — content is masked, and the CoverMe
   dictionary is not authoritative. Priority: `evar107` (`<internal>` free-text — PII-safe to
   surface?) and `evar140` (sensitive hash/join-key).
4. For the **12 shared eVars**, does each capture the **same dimension** in both suites? Only these
   can be spliced.
5. Which KPIs should actually trigger alerts (enrolment funnel? campaign views? traffic? language
   mix?)?

**Dimensions & operations**
6. Is the new suite's **USA 11.5 %** real end-users or internal/test traffic to exclude?
7. Confirm the `language` mapping (45 = EN, 39 = FR).
8. Freshness SLA / daily cutoff (writes are ~3-min batches; grain of record is daily).
9. Is **weekend/holiday ≈ 40 %** expected, and should statutory holidays be modelled?

---

## 9. Assumptions & provenance

- **Every figure** is reproduced from an executed notebook cell (`SHAREABLE` blocks). New-suite run
  confirmed 2026-07-09, re-run 2026-07-12 (S4c added, manifest verified 16/16 on byte-length **and**
  sha1). Legacy run 2026-07-10, re-run 2026-07-13 (all exact figures reproduced; that notebook is an
  **earlier copy** — its manifest sha1s are redacted, integrity is byte-length only, and it lacks the
  S4c cell).
- **Sample = 5 %, seed 42** (new suite 57,832 rows; legacy ~66,418 hits); shares are computed on the
  sample, exact counts on full scans. Deep-profiling stats (S5–S11) on the legacy suite are sampled
  from a window that straddles its collapse; full-range volume is exact.
- **Adobe uses empty string, not NULL**; `date_time` is Eastern, `hit_time_gmt` epoch GMT;
  `language` values are Adobe numeric IDs; `clean_hits == hits` because no bot filtering is applied;
  eVar/prop *content* is masked (ADR-0007) so "same column live in both suites" is necessary, not
  sufficient, for "same meaning."
- **Related records:** [`12-eda-findings-analysis.md`](12-eda-findings-analysis.md) (new suite, full
  detail) · [`14-manugrs-cross-suite-analysis.md`](14-manugrs-cross-suite-analysis.md) (legacy /
  cross-suite) · [`10-data-profile-alignment.md`](10-data-profile-alignment.md) (governance) ·
  [`11-privacy-identity-governance.md`](11-privacy-identity-governance.md) (privacy). Source
  notebooks: [`eda/gwam_canada_retirement_eda.ipynb`](../../eda/gwam_canada_retirement_eda.ipynb),
  `eda/gwam_canada_retirement_eda_manugrs.ipynb` (retired 2026-07-20, at `408de5a`),
  charts [`eda/gwam_canada_retirement_charts.ipynb`](../../eda/gwam_canada_retirement_charts.ipynb).
