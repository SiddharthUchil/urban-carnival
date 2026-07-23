# 15 — GWAM Canada Retirement: Consolidated EDA Report (both suites)

> **Purpose.** A single, stakeholder-facing account of the exploratory data analysis (EDA)
> performed for the GWAM Canada-Retirement anomaly-detection build. It explains, in plain terms,
> **what the two profiling notebooks did, which widgets and columns we used, and what every EDA
> step tells us about the data** — then goes to analyst depth on the findings.
>
> **Scope.** Two report suites in one source table:
> the **shipped** suite `manulifeglobalprod` (new site, profiled in
> `eda/gwam_canada_retirement_eda.ipynb`) and the
> **legacy** suite `manugrs` (old site, profiled in `eda/gwam_canada_retirement_eda_manugrs.ipynb`
> — notebook retired 2026-07-20, recoverable at `git show 408de5a:<path>`).
>
> ⚠️ **Both `.ipynb` files were removed 2026-07-20.** `eda/` is now exactly two `.py` files
> (doc-16 D1), and the surviving profiler covers **both suites in a single run** via the
> `rsid_list` widget — the one-notebook-per-suite split this report describes no longer
> exists. Per-suite figures now come from `window_frame.filter.rsid_breakdown` within one run.
>
> ⚠️ **Superseded in part (2026-07-20):** "legacy" describes the *site*, not the suite. `manugrs`
> is still live — 320,304,305 hits through 2026-07-19, concurrent with `manulifeglobalprod`.
> See doc-16 §1 D7 / §2.
>
> ⚠️ **Revised 2026-07-22 — two changes invalidate parts of this report as written.**
> **(a) The eVar dictionary now exists.** `data/EDDL_datalayer.xlsx` (25 tabs) was added
> 2026-07-19 and parsed into [`16-e2e-production-blueprint.md`](16-e2e-production-blueprint.md) §3.
> Statements below that eVar meanings are unknowable, and that CoverMe is "the only dictionary on
> hand," are **obsolete** — see §5.4 and §8.
> **(b) The privacy regime inverted, then went full-raw.** ADR-0007 §5 was *revised, not retired*:
> business dimensions (eVars, props, events, URLs, pagenames, campaigns, referrers, search terms)
> profile **raw and in full**, and as of the **2026-07-23 full-raw revision** the direct
> device/network identifiers do too — **no shape-only carve-out remains**. Every "content is
> masked" and "identifier is shape-only" claim below is **obsolete** — see §4 and ADR-0007 §5.
> Sections rewritten for this: §2 (widgets), §3 (scope), §4 (privacy), §5.3, §5.4, §5.8, §7, §8,
> and the new **§8b (EDA exit criteria)**. Numeric findings from the profiling runs are unchanged.
>
> **Data runs.** Production `gwam_prod_catalog`. New suite confirmed 2026-07-09, re-run 2026-07-12
> (adds the S4c URL audit). Legacy suite run 2026-07-10, independently re-run 2026-07-13 (all
> figures reproduced). **Grain of record: daily.** **Privacy regime: ADR-0007 §5** (full-raw as of
> 2026-07-23 — all columns incl. identifiers). This document consolidates and does not replace the two analyses of record,
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
  batches, Eastern-time timestamps, ~0 duplicate rate — but **no person-level identity is
  populated** (`cust_visid` 100 % null, `userid` constant); analysis is device-level only.
- **We now have the GWAM eVar dictionary.** `data/EDDL_datalayer.xlsx` (25 tabs, added 2026-07-19)
  supersedes the CoverMe workbook. It **names all 12 cross-suite eVars** and resolves the two
  unknowns this report previously flagged as blocking: **eVar107 = full page URL** and
  **eVar140 = Medallia UUID** (§5.4). The workbook contains **no rsid names**, so it says what each
  variable *should* mean — not which suite implements it. That gap is now the open question (§8).
- **Three findings emerge only from combining the dictionary with the profiling runs**, and all
  three need action before build (§8b): **eVar107 is a URL field that the S4c scope audit never
  tested**; **eVar132–134 are spec'd as Member Customer IDs**, which would be person-level identity
  if populated; and under the new raw-by-default regime **eVar131 (ECID) and eVar108 (User Agent)
  print raw** because the sensitivity check matches literal column names, not eVar semantics.

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
4. Applies **ADR-0007 privacy** throughout — but note the regime **inverted in 2026-07**: business
   dimensions now print raw, and only direct device/network identifiers are shape-only. See §4.

The two notebooks were structurally identical, differing only in the widget values pointing them at
each suite. **Both were retired 2026-07-20** and replaced by a single dual-rsid profiler,
[`eda/gwam_canada_retirement_eda.py`](../../eda/gwam_canada_retirement_eda.py), which covers both
suites in one run via `rsid_list` (§2); per-suite figures come from
`window_frame.filter.rsid_breakdown`. The companion
[`eda/gwam_canada_retirement_charts.py`](../../eda/gwam_canada_retirement_charts.py) renders the
same data as interactive charts (traffic over time, day-of-week heatmap, country map, language mix,
event timeline, RRSP seasonality) — a visualization layer, not new analysis. The table above is
retained because **the figures in this report come from those historical runs**.

---

## 2. The widgets we used

> **Rewritten 2026-07-22.** The 11-widget, one-suite-per-run contract described here previously was
> replaced by commits `1fb274b` and `5e2a220`. The live profiler
> ([`eda/gwam_canada_retirement_eda.py`](../../eda/gwam_canada_retirement_eda.py)) now takes
> **14 widgets** and profiles **both suites in a single run**.

Nine widgets are tuning knobs; **five define the population**: `rsid_list`, `url_scope_mode`,
`url_scope_list`, `url_scope_exclude`, `login_host_exclude`.

| # | Widget | Default | What it controls |
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
| 10 | **`rsid_list`** | `manugrs,manulifeglobalprod` | **Report suites to keep** — comma-separated, **both by default** (doc-16 D7). |
| 11 | **`url_scope_mode`** | `broad` | `broad` = use `url_scope_list`; `en_only` = pin to the single English pattern for pipeline parity. |
| 12 | **`url_scope_list`** | `%/group-retirement%,%/group-plans%,%/regimes-collectifs%` | **Authoritative URL include patterns** (SQL `LIKE`). Add URLs here. |
| 13 | **`url_scope_exclude`** | `%adobeaemcloud.com%,%/ph/%` | URL patterns to drop (authoring host, Philippines). |
| 14 | **`login_host_exclude`** | authenticated hosts | Member-portal / auth hosts excluded by business rule (doc-16 D8). |

**Correction to the previous version of this section.** It stated there is "no `SCOPE_URL_MODE`
toggle in the notebook." That is now **false** — `url_scope_mode` is widget 11, and `en_only`
reproduces the pipeline's default scope. What remains true: the sample **seed is hard-coded (42)**,
the window is **derived** from `window_months` rather than explicit dates, and there is still **no
`SCOPE_SUITE_MODE`** in the EDA (dual-rsid is now permanent, so the toggle is unnecessary here; the
production equivalent lives in [`databricks/conf/settings.py`](../../databricks/conf/settings.py)).
Explicit `start_date` / `end_date` and geo dropdowns exist only in the *charts* companion.

---

## 3. How we scoped the data

The notebook picks the real column names defensively (`rsid` from `rsid|report_suite|reportsuite…`,
URL from `post_page_url|page_url`). **As of `5e2a220` the scope is list-based**, not a single
substring:

```
lower(trim(rsid_col)) IN rsid_list
  AND  ( url_col LIKE ANY url_scope_list )         -- or the single EN pattern if url_scope_mode=en_only
  AND  NOT ( url_col LIKE ANY url_scope_exclude )
  AND  NOT ( host      LIKE ANY login_host_exclude )
```

Any widget left empty drops that condition; a missing schema column is dropped **and flagged** in
the run metadata, so the notebook never silently profiles the wrong population. The
`login_host_exclude` clause encodes the business rule that authenticated member-portal traffic
(`portal.manulife.ca`, `id.manulife.ca`, `grsmembers.manulife.com`, `gsrs1.manulife.com`) is out of
scope — this is why suite-level `manugrs` volume (320 M hits) dwarfs the ~5.6 M in scope. Two
consequences the audit sections (S4/S4b/S4c) quantify:

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

**Privacy (ADR-0007 §5) governs how each column is printed — this regime was inverted in 2026-07:**

The old rule was *mask by default, allowlist a few technical dimensions*. It was abandoned because
it destroyed the analytical signal without protecting a person: whole sections came back as
unreadable `<masked:xxxxxxxx>` tokens, and the regex net (`guid|token|mcid|aamid|zip$|social`) swept
in business columns that were never identifiers. The current rule is the reverse:

- **Business dimensions print raw and in full** — eVars, props, events, URLs, pagenames, campaigns,
  referrers, search terms. The justification is that this feed carries **no person-level
  identifier** (`cust_visid` wholly NULL, `userid` a single constant — confirmed with the data owner
  2026-07-04).
- **Direct device/network identifiers print shape-only** (null %, cardinality, length stats). The
  set is **16 exact column names**, not a pattern: `mcvisid`, `visid_high/low`,
  `post_visid_high/low`, `cust_visid`, `post_cust_visid`, `cookies`, `post_cookies`,
  `persistent_cookie`, `ip`, `ip2`, `ipv6`, `geo_zip`, `post_zip`, `zip`, `user_agent`.
- **URL query strings are stripped** (session tokens live there); paths and hosts print in full.
- The residual scrub net is **deliberately minimal**: emails and IPv4 literals only, with a
  2,000-character truncation cap.

> ⚠️ **Gap this creates — see §8b item 3.** `is_sensitive()` is an **exact match** against those 16
> names. It has no knowledge of eVar *semantics*. The EDDL dictionary shows several live eVars carry
> exactly the content that set exists to protect — **eVar131 = Anonymous ID (ECID)** and
> **eVar108 = User Agent** are both in the live 12-eVar cross-suite set, and both now print raw.
> `user_agent` is shape-only while `evar108`, which holds the same string, is not.

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

> ⚠️ **The S4c sweep is narrower than it reads — see §8b item 1.** "All five candidate URL columns"
> means exactly `first_hit_page_url`, `page_url`, `post_page_url`, `visit_start_page_url`,
> `site_url` (`URL_CANDIDATES` in the profiler). The EDDL dictionary now identifies
> **eVar107 (+prop52) as the primary full page URL field** — `document.url` split into
> Domain / Hash / Query / Path — and **eVar107 is live in both suites**. It was never tested as a
> scope source. The claim "no retirement traffic hides beyond the coalesce" is therefore
> **unproven for eVar107/prop52**, and the same applies to **eVar194** (click href) and
> **eVar127** (download URL). Until S4c is widened, treat the coalesce as *best known*, not *proven
> complete*.

### 5.4 Schema population & custom dimensions (S5, S7)

| Signal | New suite | Legacy suite |
|---|---:|---:|
| Populated columns (of 1,198) | 197 | 272 |
| Core (≥99 %) | 158 | 186 |
| Dead | 995 | 912 |
| Live eVars (logical) | 20 (15 core) | 96 (52 core) |
| Live props (logical) | prop51–57 (5 core) | 26 (14 core) |

The legacy suite lights up **more** columns because it carries a larger live-eVar set. **Cross-suite
overlap is small: only 12 logical eVars are live in both**; **50 are legacy-only, 8 are new-only**.
That is the "different columns are populated → high missing rate on a naïve union" case the manager
anticipated — *not* a rename.

**The 12 shared eVars now have names** (EDDL `Global Data Layer_GPMSS_IT` + per-entity tabs, doc-16
§3.1). These are the only cross-suite splice candidates:

| eVar | EDDL meaning | Note for detection |
|---|---|---|
| eVar101 | Page Name (`web.webPageDetails.name`) | Slicing dimension; pairs with `pagename`. |
| eVar102 | Page Type (Non-Txn / Txn) | Funnel segmentation. |
| eVar103 | Site Type (PWS / CWS / Sign-in hub) | **PWS = public, CWS = customer**; group retirement is CWS. |
| eVar105 | Brand \| Line of Business \| Segment | Segment ∈ CA / JH / GWAM / Asia — the scope discriminator. |
| eVar106 | Country \| Region \| City | ⚠️ fine geo, delimited multi-value. |
| **eVar107** | **Full Page URL \| Domain \| Hash \| Query \| Path** | **Resolves the old `<internal>` free-text mystery** — card ~734, ≤262 chars is exactly a URL. Pairs with prop52. |
| eVar108 | User Agent (`navigator.userAgent`) | ⚠️ prints raw — see §4 and §8b item 3. |
| eVar109 | Language (pairs prop54) | Cross-check against the numeric `language` IDs (45/39). |
| eVar131 | **Anonymous ID (ECID)** | ⚠️ device identifier printing raw — §8b item 3. |
| eVar138 | User Type | Member vs sponsor vs advisor segmentation. |
| eVar162 | Search Keywords (pairs prop71) | ⚠️ conflicts with CAR applicability — §5.4 note below. |
| eVar200 | Onetrust Categories-ID | ⚠️ conflicts with the profiled cardinality — note below. |

**Two previously-blocking unknowns are resolved.** `evar107` = full page URL (above), and
**`evar140` = Medallia UUID** — a survey-platform identifier, not the "sensitive join-key" this
report previously guessed. Both were listed as *priority* asks in §8 Q3; both are now answered by
the spec.

**Two dictionary-vs-data conflicts remain open:**
- **eVar200.** EDDL says *Onetrust Categories-ID* (consent-category grouping, expected to be
  low-cardinality). Profiling measured it at **≈ visitor cardinality**, i.e. identity-like. Either
  the suite reuses the slot, or the field carries a per-visitor consent receipt. Do not use it as a
  KPI until reconciled.
- **eVar162.** EDDL says *Search Keywords*, but the `EDDL ready for CAR` tab marks **Searches as
  not CAR-applicable**. A search field that is live in both retirement suites contradicts that.

**The CoverMe dictionary is formally superseded.** Worth recording why it misled: the profile in
`new_data/data_profile_summary.json` is a profile **of the CoverMe dictionary spreadsheets
themselves** (5 files, 509 rows total — `input_root` is a local Desktop path), *not* of GWAM hit
data. Any eVar "type" read from that file describes spreadsheet cells, not production traffic.

**Critical caveat — the workbook names no rsids.** EDDL is a *planning* spec: its "Report Suite
Mapping" column is blank throughout. It establishes what each variable **should** mean; it cannot
confirm that `manugrs` and `manulifeglobalprod` implement it, or implement it identically. So the
12 shared eVars remain splice candidates **pending a per-rsid confirmation** (§8b item 2), and the
known spec-internal conflicts (Platform eVar110↔185, Domain eVar107↔121, eVar122 dual meaning,
DKPIs legacy numbering) must be flagged rather than guessed.

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
> **`new_data/event.tsv` is that lookup** and is already in the repo (e.g. `10006` = *Instance of
> eVar107*, `10065` = *Instance of eVar166*, `10068` = *Instance of eVar169*). Loading it would
> resolve the labels; that wiring was scoped previously but never implemented.

> **The eVar166 / eVar169 contradiction (doc-16 backlog #4) — narrowed.** EDDL says
> **eVar166 = Product ID** and assigns **no meaning to eVar169**; doc-16 §3.4 records an earlier
> claim that production profiling tagged both as *URL-type* in `manugrs`. On review that claim
> **has no traceable source in this repo** — the only eVar166/169 references are the
> instance-of-eVar rows in `event.tsv` above and empty cells in the CoverMe workbook profile
> (100 % null, `distinct_count: 0`; the "float" type is the default inference for an empty column,
> not evidence of content). Two corrections follow: doc-16's "**eVar169 does not exist**" is too
> strong — the Adobe slot exists and has an instance event (`10068`); what does not exist is an
> *EDDL meaning* for it. And "event169 = Video Complete" is a **different ID space** from
> instance-of-eVar169. Resolution still requires the per-rsid live-eVar census (§8b item 2), but
> nothing currently contradicts the spec.

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
information** under Law 25 / PIPEDA, so RBAC stays mandatory and a **PIA is required before
production ingestion**.

> **The dictionary changes the identity question — see §8b item 4.** This report previously deferred
> person-level stitching "pending a MarTech ask to capture a login/customer ID in a dedicated eVar."
> **EDDL shows that ask is already specified**: `eVar132 / eVar133 / eVar134` = **Primary /
> Secondary / Tertiary Member Customer ID**, with `eVar131` = Anonymous ID (ECID),
> `eVar136` = Email (hashed), and `eVar137` = Age \| Gender \| Spouse Age \| Spouse Gender \|
> #Dependents \| Smoking. None of eVar132–134 appears in the live cross-suite set, and the
> "no person-level identity" finding above rests on `cust_visid` / `userid` — **not** on these
> columns, which were never checked by name. The question is no longer *"can we ask for a customer
> ID?"* but **"are eVar132–134 populated in either suite today?"** If any is, the privacy posture
> changes materially: the feed would carry direct customer identifiers, the PIA scope widens, and
> the raw-by-default regime in §4 would need revisiting before the next EDA run emits blocks.

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
| Only 12 shared eVars; **now named** via EDDL | eVar-derived KPIs are cross-suite-spliceable **only after a per-rsid census confirms the spec holds in each suite** — the workbook names no rsids. |
| eVar107/194/127 are URL fields never audited for scope | **Widen S4c** before freezing scope; the coalesce is best-known, not proven complete. |
| eVar131 (ECID) + eVar108 (User Agent) print raw under the new regime | Extend the sensitivity check to **eVar semantics**, not just literal column names, before the next run emits blocks. |
| eVar132–134 spec'd as Member Customer IDs | **Check population before the PIA.** If live, the feed carries direct customer identifiers and the privacy posture changes. |
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

> **Updated 2026-07-22 after the EDDL dictionary landed.** Q3 is now **closed**; Q4 is **narrowed**
> from "what do these mean?" to "does the spec hold per suite?". Five questions are new (Q10–Q14),
> and three of them (Q10, Q12, Q13) are **blocking** — they change scope, privacy posture, or both.

### Closed by the dictionary

| Was | Now |
|---|---|
| **Q3** — provide the GWAM eVar dictionary; priority `evar107`, `evar140` | ✅ **Closed.** `data/EDDL_datalayer.xlsx`, 25 tabs, parsed in doc-16 §3. **eVar107 = Full Page URL**, **eVar140 = Medallia UUID**. CoverMe formally superseded (§5.4). |

### Still open — scope & history

1. Confirm **2026-02-01** as the migration date and whether `manugrs` history is authoritative for
   backfill. Union the legacy suite for training (≈2.5 yr, at the cost of a change-point) or start
   clean at the new suite (158 days)?
2. Should scope include **French `/ca/fr`** traffic? It is now a **two-domain** decision
   (`manulifeim.com` legacy +314 K rows, `manulife.com` new +230 K rows).
10. 🚩 **Blocking — does scope need to consider `eVar107`?** EDDL calls it the primary page-URL
    field, and the S4c audit never tested it (§5.3). If eVar107 carries retirement hits the
    `coalesce(page_url, post_page_url)` scope misses, the scope decision reopens. Also ask whether
    **eVar121 (Domain)** duplicates eVar107, which the spec flags as a conflict.
11. Reconcile the **unfiltered `manulifeglobalprod` first-day discrepancy**: the scoped run reports
    2026-02-01, the unfiltered inventory reports **2026-03-10**. Which is the true suite start?

### Still open — metric semantics

4. **Narrowed.** For the **12 shared eVars** (now named, §5.4), does each carry the **same content
   in both `manugrs` and `manulifeglobalprod`**? EDDL contains **no rsid mapping**, so it cannot
   answer this — it needs either the Adobe report-suite variable map per rsid, or the census in
   §8b item 2.
5. Which KPIs should actually trigger alerts (enrolment funnel? campaign views? traffic? language
   mix?)? EDDL's `EDDL ready for CAR` tab gives a starting cut: **CAR-applicable = Yes** for page
   views, errors, transactions, user IDs, LOB/segment, referrers, downloads, exits, product,
   policy IDs; **No** for searches, registrations, logins, campaign, link tracking.
12. 🚩 **Blocking — resolve the two dictionary-vs-data conflicts** in §5.4: **eVar200**
    (spec'd Onetrust Categories-ID, profiled at visitor cardinality) and **eVar162** (spec'd Search
    Keywords, but Searches are marked not-CAR-applicable). Neither can feed a KPI as-is.
13. 🚩 **Blocking — are `eVar132 / 133 / 134` (Primary / Secondary / Tertiary Member Customer ID)
    populated in either suite?** See §5.8. A "yes" makes this a feed carrying direct customer
    identifiers and widens the PIA before any production ingestion.
14. Confirm the **spec conflicts EDDL flags internally** rather than guessing: Platform
    eVar110 ↔ eVar185, Domain eVar107 ↔ eVar121, eVar122 dual meaning (Login Step vs Error
    Description), event164 dual meaning, Purchase-ID event165 ↔ event167, and the **DKPIs tab's
    legacy numbering** — is DKPIs genuinely superseded, or live for some suite?

### Still open — dimensions & operations

6. Is the new suite's **USA 11.5 %** real end-users or internal/test traffic to exclude?
7. Confirm the `language` mapping (45 = EN, 39 = FR) — and cross-check against **eVar109 /
   prop54**, which EDDL says carry language directly and may be more readable than numeric IDs.
8. Freshness SLA / daily cutoff (writes are ~3-min batches; grain of record is daily).
9. Is **weekend/holiday ≈ 40 %** expected, and should statutory holidays be modelled?
15. Confirm the **authenticated-host exclusion** (`portal.manulife.ca`, `id.manulife.ca`,
    `grsmembers.manulife.com`, `gsrs1.manulife.com`) is correct and complete. It removes the large
    majority of suite-level `manugrs` volume, so a wrong list silently changes every baseline.

---

## 8b. EDA exit criteria — what must be true before build proceeds

> **Added 2026-07-22.** The profiling runs themselves are complete and reproduced. What is *not*
> complete is the reconciliation between those runs and the dictionary that arrived afterwards.
> These five items are the gate. Items 1–4 are **repo-side work we can do without waiting on
> stakeholders**; item 5 is the stakeholder gate.

| # | Item | Why it blocks | Done when |
|---|---|---|---|
| 1 | **Widen the S4c URL audit to `evar107`/`post_evar107`, `prop52`, `evar194`, `evar127`** | The "no retirement traffic beyond the coalesce" conclusion is unproven for the field EDDL calls the *primary* page URL (§5.3). Scope is the foundation every KPI sits on. | S4c reports retirement-hit counts for those columns and either confirms zero incremental rows or quantifies them. |
| 2 | **Per-rsid live-eVar census cross-referenced against EDDL** | Resolves Q4 (same meaning per suite), doc-16 backlog #4 (eVar166/169), and the eVar200/eVar162 conflicts in one pass. The unified dual-rsid profiler already emits `live_custom_dims` per rsid — this is a join, not a new run. | A table of `rsid × eVar × EDDL meaning × populated % × cardinality`, with mismatches flagged. |
| 3 | **Extend the sensitivity check to eVar semantics** | `is_sensitive()` matches 16 literal column names, so **eVar131 (ECID)** and **eVar108 (User Agent)** print raw today (§4), and eVar136/137 would too if live. Every SHAREABLE block copied out of the workspace carries this. | `DIRECT_IDENTIFIERS` (or an EDDL-derived map) covers identifier-bearing eVars; a re-run emits no raw ECID or user-agent values. |
| 4 | **Check `evar132/133/134` population explicitly** | Determines whether person-level identity exists (§5.8). Drives the PIA scope and possibly the whole privacy regime. | Census reports populated % for all three in both suites. |
| 5 | **Stakeholder sign-off on scope** (Q1, Q2, Q10, Q15) | `SCOPE_URL_MODE` / `SCOPE_SUITE_MODE` are held deliberately — flipping either **re-baselines every KPI**, so it must happen before baselines are fit, not after. | Written confirmation of suite union, French inclusion, eVar107 handling, and the login-host list. |

**What is already sufficient and needs no further EDA:** volume/history/seasonality (§5.2), the
day-of-week and RRSP shape driving baseline design, data-quality posture (§5.7 — no bot filtering,
Eastern timestamps, ~0 duplicates), the cutover date and change-points (§6), and the event decode
convention (§5.5). Detection design can proceed against these **while items 1–4 run**, provided no
eVar-derived KPI and no frozen scope ships before item 5.

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
  `language` values are Adobe numeric IDs; `clean_hits == hits` because no bot filtering is applied.
  "Same column live in both suites" remains **necessary but not sufficient** for "same meaning" —
  the reason has changed, though: no longer because content is masked (it is not, §4), but because
  **EDDL carries no rsid mapping** and so cannot confirm either suite implements the spec (§5.4).
- **Dictionary provenance (2026-07-22).** `data/EDDL_datalayer.xlsx`, 25 tabs, verified by direct
  read: authoritative = `Global Data Layer_GPMSS_IT` + the 15 per-entity tabs; `DKPIs` is a
  conflicting legacy numbering; CAR applicability lives in `EDDL ready for CAR` /
  `EDDL for CAR_WIP`. Parsing note: the environment has **no `openpyxl`/`xlrd`** — the workbook must
  be read via stdlib `zipfile` + XML. Full transcription in doc-16 §3.
- **Claims retired in this revision**, recorded so they are not reintroduced: "the only dictionary
  on hand is CoverMe"; "every eVar's meaning is masked"; "24 sensitive columns + regex net + raw
  allowlist"; "11 widgets, `rsid_filter`/`url_filter`"; "there is no `SCOPE_URL_MODE` toggle in the
  notebook"; and the unsourced "eVar166/169 are URL-type in `manugrs`" (§5.5).
- **Related records:** [`12-eda-findings-analysis.md`](12-eda-findings-analysis.md) (new suite, full
  detail) · [`14-manugrs-cross-suite-analysis.md`](14-manugrs-cross-suite-analysis.md) (legacy /
  cross-suite) · [`10-data-profile-alignment.md`](10-data-profile-alignment.md) (governance) ·
  [`11-privacy-identity-governance.md`](11-privacy-identity-governance.md) (privacy). Source
  notebooks (all three `.ipynb` retired 2026-07-20 — recover via `git show <sha>:<path>`):
  `eda/gwam_canada_retirement_eda.ipynb`,
  `eda/gwam_canada_retirement_eda_manugrs.ipynb` (at `408de5a`),
  charts `eda/gwam_canada_retirement_charts.ipynb`. Live sources:
  [`eda/gwam_canada_retirement_eda.py`](../../eda/gwam_canada_retirement_eda.py) and
  [`eda/gwam_canada_retirement_charts.py`](../../eda/gwam_canada_retirement_charts.py).
