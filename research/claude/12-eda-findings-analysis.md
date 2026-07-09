# 12 — GWAM Canada Retirement EDA: Findings & Analysis

> **Status:** analysis of record for the exploratory profiling in
> [`gwam_canada_retirement_eda.ipynb`](../../gwam_canada_retirement_eda.ipynb)
> (paired source: [`eda/gwam_canada_retirement_eda.py`](../../eda/gwam_canada_retirement_eda.py)).
> **Data run:** production, `gwam_prod_catalog` (confirmed 2026‑07‑09).
> **Grain of record:** daily. **Privacy regime:** ADR‑0007 (shape‑only for sensitive columns).
> **Companion:** interactive charts in [`eda/gwam_canada_retirement_charts.py`](../../eda/gwam_canada_retirement_charts.py).

This document turns the notebook's raw `SHAREABLE` JSON blocks into a narrative with
**objectives, method, findings, and implications** for the anomaly‑detection build. Every
number here is reproduced from an executed notebook cell; section IDs (S1–S12) map 1:1 to the
notebook.

---

## TL;DR — the ten things we now know

1. **The data is a thin slice of a huge table.** The source holds **3.18 B** GWAM Adobe hit rows across **1,198 columns / ~357 GB**; CA Retirement is **1,151,474 rows (0.036 %)**, selected by `rsid = manulifeglobalprod` **AND** page‑URL prefix `…/ca/en/personal/group-plans/group-retirement`.
2. **CA Retirement history is short.** Rows exist **only from 2026‑02‑01 → 2026‑07‑07 — 157 consecutive days, zero gaps.** There is no earlier CA‑Retirement history in this table. This is the single biggest constraint on model choice.
3. **Weekly seasonality is strong and clean.** Autocorrelation **lag‑7 = 0.726**, lag‑28 = 0.620; day‑of‑week index runs **1.33 (Mon) → 0.47 (Sun)** — weekends are **~half** of weekdays. Peak hour ≈ **10:00 local**.
4. **Volume is moderately volatile.** Coefficient of variation **0.52**; RRSP season shows in monthly totals (Feb–Mar highest, tapering into summer).
5. **Every hit carries events.** `post_event_list` is populated on **100 %** of hits, **16–18 events/hit** (max 22). Most are *instance‑of‑eVar* presence flags (no numeric value).
6. **The real KPI candidates are few.** Non‑instance events with signal: **ev500** "Instance of clickmappage" (15.9 % of hits), **ev20** "Campaign View" (9.6 %), **ev501–504** clickmap (~3.9 % each).
7. **Instrumentation changed mid‑window.** ev501–504 begin **2026‑02‑24**; ev500 fires **only 2026‑04‑02 → mid‑May** then goes to zero. These on/off shifts must not be mistaken for anomalies.
8. **Geography and language are rich — and available in source.** `geo_country` (CAN 85 %, USA 11 %), `geo_region` (ON 47 %, AB 15 %, BC 12 %, QC 4 %), `language` (45 ≈ EN 63 %, 39 ≈ FR 30 %). *Note: geo columns are in the source table but not yet in the production pipeline bronze layer.*
9. **There is no person‑level identity.** `cust_visid`/`post_cust_visid` are **100 % null**; `userid` is constant. Visits/visitors are approximated from `mcvisid`/`visid_*`. Daily ratios: **1.36 hits/visit, 1.09 visits/visitor**.
10. **Data quality is high but under‑instrumented for lineage.** Key‑column null rate ≈ **0.125 %**; **zero** server‑side bot‑filtered hits (`exclude_hit` uniformly 0); clock offset **−4 h/−5 h** confirms EST/EDT; **no load/ingest timestamp column** exists (arrival cadence must be inferred from Delta history — ~3‑min micro‑batches).

---

## 1. Objectives & outcomes

| # | Objective (what we set out to learn) | Outcome (what we now know) |
|---|---|---|
| 1 | Fill evidence gaps: volume, history depth, load cadence, schema population. | Volume/history quantified (§3); cadence is near‑real‑time micro‑batch (§2); **197 of 1,198 columns carry data** (§5). |
| 2 | Discover the *real* metric candidates on this report suite. | `post_event_list` decoded and named via `event.tsv`; KPI shortlist identified. The CoverMe **business-KPI → custom-event** mappings in `metric-registry.yaml` don't apply to this suite, but standard event *naming* does (§6). |
| 3 | Capture time‑series shape for anomaly‑model design. | Strong weekly seasonality, moderate volatility, and concrete level‑shift/instrumentation‑shift candidates quantified (§8). |
| 4 | Produce a machine‑readable synthesis spec for synthetic data. | 120‑column schema spec emitted (S12), consumed by the synthetic generator and the `detect/` engine. |

**Net outcome:** we have enough to (a) choose a **seasonal, day‑of‑week‑aware** anomaly baseline at **daily** grain, (b) enumerate the KPI series worth alerting on, and (c) list the specific questions only the business/analytics owners can answer before the metrics are trustworthy (§9).

---

## 2. Data source, scope & load cadence (S1, S2, S4)

**Objective.** Confirm the table, define the CA‑Retirement subset unambiguously, and establish freshness.

**Method.** Unity Catalog discovery (metadata only); `DESCRIBE DETAIL` / `DESCRIBE HISTORY` (zero data scan); filter‑diagnostic counts on the unfiltered window so a wrong scope fails loudly.

**Findings.**
- **Table:** `gwam_prod_catalog.inv_typed_common.adobe_hit_data` — Delta, **16,655 files, 356.6 GB, 1,198 columns**, partitioned by `process_date`, created 2023‑06‑09, last modified 2026‑07‑08. **All columns typed `string`.**
- **Scope is URL‑bound.** In the profiling window, `rsid`‑only matches = **7,495,940**, but `url`‑only = both = **1,151,474**. The **URL prefix is the binding constraint**; `manulifeglobalprod` alone is far broader (mobile app, other suites). `manulifeglobalprod` is **not** a top‑10 report suite (top rsids: `manufingbrsmobileapp.prod` 56.9 %, `jhfswamjhreupeprod` 26.6 %, `manugrs` 7.8 %).
- **URL sparsity.** `post_page_url` is blank on **88.2 %** of window rows (and <11 % of the whole 3.18 B‑row table) — the URL filter therefore *also* excludes blank‑URL hits by construction (measured, not silent).
- **Cadence.** 70 WRITE ops in the last 100 history rows; **median inter‑arrival ≈ 3 minutes** (0.05 h), min 0.011 h, max 17.4 h; recent writes 2 K–479 K rows each. This is a **near‑real‑time micro‑batch** feed.

**Implications.** (a) The scope definition is correct but fragile — it depends on `post_page_url` being populated; any upstream change to URL capture would silently shrink the subset. (b) Freshness is sub‑hourly, so a **daily** detector has comfortable headroom, but there is **no per‑row ingest timestamp** to measure late arrival — lineage must lean on Delta history (§7).

---

## 3. Volume, coverage & seasonality (S3)

**Objective.** History depth, missing days, day‑of‑week and monthly shape, biggest day‑over‑day jumps.

**Method.** The one full‑table scan (narrow projection): daily counts of total vs CA rows over all history.

**Findings.**
- **CA Retirement exists only 2026‑02‑01 → 2026‑07‑07: 157 days present, 0 missing.** (The 13‑month profiling window is mostly empty for this subset.)
- **Day‑of‑week means (Mon→Sun), CA hits:** `9751, 9141, 9187, 8602, 7681, 3523, 3441`. Weekdays cluster ~8–10 K; **weekends collapse to ~3.5 K (~40 %).**
- **Monthly CA totals:** Feb 256,530 · Mar 311,632 · Apr 216,952 · May 152,433 · Jun 166,999 · Jul 46,928 (partial). **RRSP season (Feb–Mar) is the volume peak**, tapering into summer.
- **Top day‑over‑day jumps** flagged as level‑shift candidates (see §6/§8).

**Implications.** 157 days ≈ **22 weeks** of history. That is enough to *see* weekly seasonality but **too little to fit long seasonal models** (e.g. yearly/holiday effects) robustly — favouring **short‑memory, DOW‑aware baselines** over heavy seasonal decomposition. Monthly RRSP seasonality means a naïve month‑over‑month comparison will false‑alarm every spring/summer.

---

## 4. Schema population & custom dimensions (S5, S7)

**Objective.** Which of 1,198 columns actually carry data, and what the live custom dimensions look like.

**Findings.**
- **197 populated, 4 sparse, 997 dead** (~16 % carry data). High‑cardinality technical columns: `t_time_info` 58,473; `stats_server` 35,875; `user_agent` 1,223 (sensitive, shape‑only).
- **8 live eVars**, each populated ~99.9 %: `evar105` (card 5), `evar106` (card 4), **`evar107` (card 718, free‑text, length up to 255 — likely a path/label field)**, `evar108/109/137/138/145/200`. These feed the 8 `post_eVar` registry slots and the synthetic generator.
- **eVar *content* is masked in every profile (ADR-0007)** — no business meaning is derivable from the data. Sample tokens (`data_profiling_report.md`): `evar107` = `<internal>`, `evar108` = `<text:41>`, **`evar140` = `<hash>` (sensitive / join-key)**, the rest `<masked:…>`. The only name dictionary in `new_data/` is the **CoverMe** suite (`data_profile_summary.json` file[2] `post_eVar` = 93 rows; file[4] `post_event_list` = 156 rows) — a *different* report suite; its 8 transcribed eVar meanings live in `metric-registry.yaml` but are **not authoritative** for `manulifeglobalprod`, and the raw name strings are not in the repo.

**Implications.** The usable feature space is ~197 columns, not 1,198 — the synthesis spec correctly prunes to 120. `evar107`'s free‑text/length profile flags it as a **PII‑review** candidate before it is ever surfaced raw. `evar140` (hash, `is_sensitive`) is an identity-like slot — keep it shape-only.

---

## 5. Events & KPI candidates (S6, S8 per‑event)

**Objective.** Turn `post_event_list` into a metric shortlist.

**Findings.**
- **100 % of hits carry events**; **16–18 events/hit** (p50/p95), max 22. Composition per hit: ~17 instance‑of‑eVar flags, ~5 clickmap targets, ~1 campaign.
- **KPI‑worthy (non‑instance) events:** `ev500` "Instance of clickmappage" **15.9 %** of hits (9,177 instances in sample); `ev20` "Campaign View" **9.6 %** (5,517); `ev501–504` clickmap ~**3.9 %** each.
- **Event IDs are named by the standard Adobe `event.tsv`** (S6 hardcodes it, plus an optional `event_lookup_path` widget): 500–504 = clickmappage / clickmaplink / clickmapregion / clickmaplinkbyregion / targetsessionid; `10000+k` = Instance of eVar(101+k) → e.g. 10036 = eVar137. No *custom* events (200–1000) were observed, so nothing is unresolved.
- **Instance‑of‑eVar events (10004+) fire on 100 % of hits but `has_value_pct = 0`** — they are *presence* flags, not numeric measures.
- **Instrumentation timeline (per‑event daily series):** `ev501–504` start **2026‑02‑24** (not 02‑01); `ev500` fires **only 2026‑04‑02 → mid‑May**, then zero.

**Implications.** KPIs must be **count/rate‑based** (hits, event‑presence counts), not value‑based. The instrumentation on/off dates are **hard boundaries**: any series that starts or stops mid‑window will read as a huge anomaly to a naïve detector — these dates should be encoded as known change‑points and excluded from training/alerting windows. Event *naming* is settled via `event.tsv` — only eVar *content* semantics remain open (§9 Q3).

---

## 6. Dimensions: geography, language, technical (S9)

**Objective.** Candidate slicing dimensions with cardinality and top values.

**Findings.**

| Dimension | Card. | Top values (share) |
|---|---|---|
| `pagename` | 72 | `ca-ret:personal:overview` 57.3 %, `…enrol-now` 14.3 % |
| `page_url` (shape) | 769 | (query‑stripped shapes) |
| **`geo_country`** | 94 | **CAN 85.0 %, USA 11.4 %**, HKG 0.6 %, PHL/IND 0.5 % |
| **`geo_region`** | 322 | **ON 46.7 %, AB 15.4 %, BC 11.6 %, QC 3.7 %** |
| `geo_city` (masked) | 2,602 | shape‑only |
| **`language`** | 67 | **45 ≈ EN 63.4 %, 39 ≈ FR 30.1 %** |
| `connection_type` | 2 | 2 = 91.2 %, 4 = 8.9 % |
| `ref_type` | 6 | 6 = 60.8 %, 2 = 22.6 %, 3 = 10.5 %, 1 = 6.1 % |
| `new_visit` | 2 | 1 = 68.7 %, 0 = 31.5 % |
| `va_closer_id` | 15 | (marketing channel) |

**Implications.** Geography and language give the **global team** natural slice/drill axes (the companion charts notebook is built around them). **Caveat:** these geo columns live in the *source* table only — they are **not yet ingested into the production bronze layer** (see doc‑10 fact 9, "region" listed as a missing correlation key). Surfacing geo KPIs in the running detector requires widening `databricks/conf/bronze_columns.py`.

---

## 7. Data quality & identity (S10, S11)

**Findings.**
- **Null/blank ≈ 0.125 %** on key columns — very clean.
- **`exclude_hit × hit_source` = (0, 1) for 100 %** of the sample → **no server‑side bot filtering** is applied in this feed; `clean_hits == hits`. Bot handling, if needed, is our responsibility.
- **Clock skew p5/p50/p95 = −18000 / −14400 / −14400 s** = **−5 h/−4 h** → `date_time` is **Eastern (EST/EDT)**, `hit_time_gmt` is epoch GMT.
- **Duplicates:** on 2026‑07‑06, 18,824 rows / 18,824 distinct keys → **dup_pct 0.0** (one‑day exact check).
- **No load/ingest timestamp column** — late‑arrival can't be measured directly.
- **Identity:** `mcvisid` card 52,237 (len 38); `post_visid_high/low` ~51 K; **`cust_visid`/`post_cust_visid` 100 % null; `userid`/`username`/`user_hash`/`cookies` cardinality 1.** Daily ratios **1.36 hits/visit, 1.09 visits/visitor**.

**Implications.** Visit/visitor metrics must be built from `mcvisid`/`visid_*` (approximate distinct), never from a customer ID. The Eastern‑time offset must be applied when converting to any other geography's local time (the charts notebook does this from `hit_time_gmt`).

---

## 8. Implications for anomaly detection (synthesis)

| Signal from EDA | Design consequence |
|---|---|
| Strong weekly seasonality (lag‑7 0.73), weekend ≈ 40 % of weekday | Baseline must be **day‑of‑week aware** (e.g. trailing same‑weekday median / seasonal‑naïve), not a flat rolling mean. |
| Only 157 days of history | Prefer **short‑memory** methods; long seasonal decomposition / yearly effects are not yet fittable. |
| Instrumentation on/off shifts (ev500/501–504) | Encode 2026‑02‑24 and the ev500 Apr–May window as **known change‑points**; exclude from training. |
| Presence‑only events, no numeric values | KPIs are **counts/rates**, and coverage rules ("event fired on X % of hits") are the natural detectors. |
| RRSP monthly seasonality | Month‑over‑month deltas will false‑alarm seasonally; anchor to same‑period‑last‑year is impossible (no history) → rely on within‑window DOW baselines. |
| No bot filtering, no person ID | Own bot heuristics if required; visits/visitors are approximate by construction. |
| Eastern‑time timestamps, ~3‑min freshness | Daily grain is safe; intraday is possible but noisier and tz‑sensitive. |

These conclusions are consistent with the `detect/` engine's observed behaviour (DOW‑median univariate baseline; level‑shift being the hardest scenario). The project‑memory note *"level‑shift‑detectability‑floor"* records why level‑shift detection has an irreducible false‑positive floor under a DOW‑median baseline.

---

## 9. Open questions for business / analytics owners

> These cannot be answered from the data alone. Grouped by decision they block.

**Scope & history**
1. Is **2026‑02‑01** the true launch of CA‑Retirement instrumentation, or does earlier history live under a different `rsid`/table we should union in? (157 days limits model choice.)
2. Is the CA‑Retirement population correctly defined as `manulifeglobalprod` + that URL prefix, or are there additional pages/subdomains (e.g. app, advisor portal) that belong in scope?

**Metric semantics**
3. **eVar *content* semantics.** The event IDs themselves are already resolved by the standard Adobe `event.tsv` (20 = Campaign View; 500–504 = clickmap; `10000+k` = Instance of eVar(101+k)). What is *not* known is what each **eVar captures as a dimension** (e.g. eVar105 / eVar137) — that needs the eVar dictionary. Separately, if any **custom events (200–1000)** begin firing they *would* be suite-specific (the CoverMe business-KPI→custom-event mappings in `metric-registry.yaml` are for a different suite).
4. **The GWAM eVar dictionary.** What does each live eVar capture (`evar105-200`)? Verified `new_data/` holds no answer — values are masked (ADR-0007) and the only dictionary present is the **CoverMe** suite (93 eVar rows), whose 8 transcribed meanings in `metric-registry.yaml` are **not authoritative** here. Priority flags: **`evar107` = `<internal>` free-text** (card 718, ≤255 chars — PII-safe to surface?) and **`evar140` = a sensitive hash/join-key**.
5. Which KPIs should actually trigger alerts (enrolment funnel? campaign views? page traffic? language mix?)? The EDA lists *candidates*; the business owns the *shortlist*.

**Known changes vs anomalies**
6. Are the instrumentation on/off shifts (ev501–504 from 2026‑02‑24; ev500 only Apr→mid‑May) **planned tagging changes**, or data‑collection issues?

**Dimensions & operations**
7. Is the **USA 11 %** geo share real end‑users, or internal/advisor/test traffic that should be excluded?
8. Confirm the language‑code mapping (**45 = English, 39 = French**?) and whether other codes matter.
9. What is the **expected freshness SLA and daily cutoff** for the detector (we observe ~3‑min micro‑batches; when is a day "complete")?
10. Is weekend/holiday volume (~40 % of weekday) business‑expected, and should stat‑holidays be modelled explicitly?
11. Is exactly‑once delivery guaranteed upstream, or should the pipeline dedupe on `visid_high, visid_low, visit_num, visit_page_num`?

---

## 10. Assumptions on record

1. `date_time` is **local Eastern** (EST/EDT) time; `hit_time_gmt` is **epoch GMT**. Confirmed by the −4/−5 h clock‑skew offset.
2. CA Retirement = `rsid = manulifeglobalprod` **AND** URL contains `…/ca/en/personal/group-plans/group-retirement` (both notebook widgets).
3. The **5 % sample (seed 42, 57,672 rows)** is representative; percentages are computed on the sample, exact counts on the full subset where noted.
4. Adobe uses **empty string, not NULL**, for "no value" (all populated/blank logic uses `trim() != ''`).
5. `language` values are **Adobe numeric lookup IDs**, not ISO codes.
6. `clean_hits == hits` **because** `exclude_hit` is uniformly `0` in this feed (no bot filtering applied), not because bots were removed.
7. **Daily** is the defensible anomaly‑detection grain; intraday is possible (timestamps are fully populated) but out of current scope.
8. Geo/language columns are trustworthy for slicing even though they are not yet in the production pipeline (they are populated in the source at ~99 %+ / high coverage).
9. Event IDs are named from the standard Adobe `event.tsv` (identical across suites); the EDA notebook can load it via the `event_lookup_path` widget. Only eVar *content* meaning is suite-specific.

---

### Provenance
All figures reproduced from executed cells in `gwam_canada_retirement_eda.ipynb` (S1–S12 `SHAREABLE` blocks), production run confirmed 2026‑07‑09. Related records: [`10-data-profile-alignment.md`](10-data-profile-alignment.md), [`11-privacy-identity-governance.md`](11-privacy-identity-governance.md), [`metric-registry.yaml`](metric-registry.yaml).
