# 14 — GWAM Canada Retirement: `manugrs` legacy suite & cross-suite analysis

> **Status:** analysis of record for the `rsid = manugrs` profiling run in
> [`eda/gwam_canada_retirement_eda_manugrs.ipynb`](../../eda/gwam_canada_retirement_eda_manugrs.ipynb),
> compared against the shipped `manulifeglobalprod` run analysed in
> [`12-eda-findings-analysis.md`](12-eda-findings-analysis.md)
> (notebook [`eda/gwam_canada_retirement_eda.ipynb`](../../eda/gwam_canada_retirement_eda.ipynb)).
> **Data run:** production, `gwam_prod_catalog` (both runs 2026‑07‑10).
> **Grain of record:** daily. **Privacy regime:** ADR‑0007 (shape‑only for sensitive columns).

**Why this document exists.** The manager challenged two business‑supplied assumptions: (1) the
CA‑Retirement URL filter `post_page_url LIKE '…/ca/en/personal/group-plans/group-retirement'`, and
(2) the report suite. He asked us to profile `rsid = manugrs` — the **legacy** CA‑Retirement suite
that predates Storefront (old site `manulifeim.com/group-retirement/ca/en`) — to recover history
that exists *before* the new suite began, to quantify other capturable "retirement" traffic, and to
check whether the old and new suites populate the **same columns** (and whether same columns mean the
same thing). This document answers those four questions from the executed notebooks.

> **Provenance caveat.** The `manugrs` export was produced by an **earlier copy of the notebook**
> (before the 2026‑07‑10 `event.tsv` hard‑code and the manifest‑scrubber fix). Consequences, carried
> throughout: its `run_manifest` sha1s are all `<redacted:hexid>` (integrity is **byte‑length only**,
> not sha1), and its event IDs are **unresolved** ("resolve via event lookup"). Every figure below is
> reproduced from an executed cell; the merged extraction backing this doc is in
> `scratchpad/manugrs_cross_suite_facts.json`. **A clean re‑run should use the current notebook.**

---

## TL;DR — what the legacy suite tells us

1. **There is a clean, datable cutover.** `manugrs` ran healthy from **Jan 2024 through Jan 2026**
   (~100–520 K hits/month), then **collapsed in Feb 2026** (10,806 → 2,136 → single digits) — exactly
   when the new `manulifeglobalprod` suite **begins (2026‑02‑01)**. This is a report‑suite migration,
   not two overlapping populations. **It directly answers doc‑12 open question Q1:** earlier
   CA‑Retirement history *does* exist, under `manugrs`.
2. **The prize is history depth.** `manugrs` holds **5,568,271 rows over 2023‑12‑31 → 2026‑07‑07**.
   Splicing the pre‑Feb‑2026 legacy series to the new suite yields **~2.5 years** of CA‑Retirement
   history instead of the current **158 days** — enough to fit the seasonal/holiday models doc 12 said
   were "not yet fittable."
3. **The suites populate largely *different* columns.** Of the live custom dimensions, only **12
   logical eVars are live in both** suites; **50 are live only in `manugrs`** and **8 only in the new
   suite**. Same story for props (4 shared, 9 legacy‑only, 3 new‑only). So a naïve union would show
   **high missing rates** on most eVars — the old and new instrumentation are genuinely different tag
   sets, not a renaming.
4. **`post_page_url` is the wrong scope column here too — more so.** On the legacy suite `post_page_url`
   is **blank on 48.0 %** of rows while `page_url` is **0 %** blank; the same legacy filter matches
   **3× more rows** on `page_url` (3,996,490) than on `post_page_url` (1,322,835). The doc‑12
   recommendation — scope on **`coalesce(page_url, post_page_url)`** — is reconfirmed and strengthened.
5. **Legacy retirement traffic is more French, less US.** `manugrs` is **CAN 95.7 % / USA 3.3 %** and
   **EN 53 % / FR 40.5 %**, versus the new suite's CAN 84.8 % / USA 11.5 % and EN 63 % / FR 30 %. The
   addable legacy traffic outside the current filter (**+314,249 rows, +7.3 %**) is **essentially all
   French `/ca/fr`** paths — the French scope decision now spans **both** domains.
6. **Everything else is structurally the same table.** Same 1,198‑column source, same near‑real‑time
   ~3‑min micro‑batch cadence, same clean DQ (no bot filtering, −4/−5 h Eastern clock), same absent
   person‑level identity (`cust_visid` 100 % null, `userid` constant). The migration changed the
   *tagging*, not the plumbing.

---

## 1. The cutover (S3, both runs)

Same source table, same day‑level scan, two report suites. Monthly CA‑subset hits around the
handover:

| Month | `manugrs` (legacy) | `manulifeglobalprod` (new) |
|---|---:|---:|
| 2025‑12 | 227,951 | — (0) |
| 2026‑01 | 300,898 | — (0) |
| **2026‑02** | **10,806** | **256,530** |
| 2026‑03 | 2,136 | 311,632 |
| 2026‑04 | 13 | 216,952 |
| 2026‑05 | 11 | 152,433 |
| 2026‑06 | 5 | 166,999 |
| 2026‑07 | 3 | 54,464 (partial) |

The legacy suite's healthy history runs **2023‑12‑31 → 2026‑01** (peaks in RRSP season: 2024‑02 =
519,373, 2024‑01 = 403,928); the residual single/low‑double‑digit hits after Feb 2026 are the tail of
a decommissioned tag, **not data loss**. The new suite switches on **2026‑02‑01**. The two series are
end‑to‑end complementary.

**`manugrs` is a material report suite in its own right.** In the 13‑month profiling window it is the
**3rd‑largest rsid** (7.8 % of all GWAM hits), behind the mobile app (56.9 %) and `jhfswamjhreupeprod`
(26.6 %) — whereas `manulifeglobalprod` is not even top‑10. The business filtered on the *smaller*
suite.

---

## 2. Volume, coverage & seasonality — `manugrs` (S3)

- **5,568,271 rows, 2023‑12‑31 → 2026‑07‑07.** Of 920 calendar days in range, **821 present, 99
  "missing"** — but almost all "missing" days are **after the Feb‑2026 collapse** (a dead suite has
  no traffic), so they are expected, not gaps in a live feed. The healthy period (2024–Jan 2026) is
  dense.
- **Day‑of‑week means (Mon→Sun):** `7560, 7597, 7535, 7429, 6607, 2689, 2950`. **Weekends ≈ 37 % of
  weekdays** — the same strong weekly seasonality doc 12 found for the new suite (weekend ≈ 40 %).
- **RRSP seasonality is visible across two full seasons** (Feb 2024 and Feb 2025 are annual peaks) —
  which the new suite, at 158 days, cannot show even once.

> **Caveat on the deep‑profiling sections (S5–S11) for `manugrs`.** The 5 % sample is drawn from the
> **13‑month window (2025‑06‑01 → 2026‑07‑07)**, which straddles the suite's healthy tail *and* its
> Feb‑2026 collapse. Census/dimension/event *shares* below are representative of the suite's final
> year; **full‑range** counts (volume, monthly totals) are exact over all history.

**Implication.** If the legacy series is ingested, the combined record supports **seasonal‑naïve /
holiday‑aware** baselines that 158 days alone cannot — but **2026‑02‑01 must be encoded as a
known change‑point** (suite migration), and pre/post series are only comparable on **suite‑agnostic
metrics** (hit counts, geography), not on eVar‑derived KPIs (§4).

---

## 3. Do old and new populate the same columns? (S5, S7) — the manager's mapping question

**Schema population is broadly similar in shape but the *live custom dimensions differ*.** The legacy
suite lights up **272 of 1,198** columns (186 core ≥99 %); the new suite **197** (158 core). The extra
legacy columns are mostly its larger live‑eVar set.

**Live eVars (logical, `evar` ∪ `post_evar` collapsed):**

| Bucket | Count | eVars |
|---|---:|---|
| **Live in BOTH** | **12** | evar101, 102, 103, 105, 106, 107, 108, 109, 131, 138, 162, 200 |
| **New‑suite only** | 8 | evar104, 121, 137, 140, 144, 145, 193, 194 |
| **`manugrs` only** | 50 | evar2–99 range (2,3,4,5,6,8,14,15,17,18,19,21,33,37,42,52,53,54,59,65,66,74,75,76,78,99), 110, 111, 118, 147–186 range, 222, 223 |

**Live props (logical):** shared **prop51, 52, 54, 55**; legacy‑only **prop3, 4, 5, 8, 14, 15, 16, 26,
28**; new‑only **prop53, 56, 57**.

**Non‑eVar columns populated on `manugrs` but not surfaced on the new suite's top‑120 census view:**
`currency`, `domain`, `geo_city`, `geo_country`, `geo_region`, `va_closer_detail`, `va_finder_detail`.
(Geo is populated on *both* suites — see §5; it simply ranks inside vs outside each run's top‑120
listing.)

**Verdict for the manager's question — "are the same columns populated for old and new, and do they
mean the same thing?"**

- **Different columns are populated.** Only ~12 eVars overlap; ~50 are legacy‑only. **A naïve union of
  the two suites would therefore show high missing rates** on the union of eVar columns — exactly the
  symptom the manager anticipated. This is the "different columns populated → high missing rate" case,
  not the "same columns, different meaning" case.
- **Even the 12 shared eVars can't be assumed identical in meaning.** All eVar *content* is masked
  under ADR‑0007, so we cannot confirm semantics from the data. The shared 12 are the **only splice
  candidates**, and only *after* the business eVar dictionary confirms each captures the same thing in
  both suites.
- **Practical consequence:** the safe cross‑suite metrics are the **suite‑agnostic** ones — hit/visit
  counts, geography, language, `pagename` — not eVar‑derived dimensions.

`pagename` itself is renamed across the migration (further evidence the suites are distinct
instrumentation): legacy pages use the **`crt-public:`** namespace (`crt-public:home` 40.5 %,
`crt-public:enroll-now` 8.9 %, `crt-public:financial-stress-survey` 7.8 %, `mim-gr:home` 1.6 %), while
the new suite uses **`ca-ret:personal:`** (`ca-ret:personal:overview` 57.5 %, `…enrol-now` 13.9 %).

---

## 4. URL columns & scope (S4b, both runs) — the manager's filter challenge

The manager questioned whether `post_page_url` and the English‑only substring are the right scope. The
legacy suite makes the case even more strongly than the new one:

| Signal (rsid‑only window) | `manugrs` (legacy) | `manulifeglobalprod` (new) |
|---|---:|---:|
| `post_page_url` blank % | **47.99 %** | 36.98 % |
| `page_url` blank % | 0.0 % | 0.013 % |
| Current filter matched on `page_url` | 3,996,490 | 2,573,024 |
| Current filter matched on `post_page_url` | 1,322,835 | 1,165,431 |
| Rows matched **only** via `page_url` | 2,673,658 | 1,407,600 |
| Retirement‑related rows (excl. noise) | 4,310,739 | 3,202,778 |
| **Addable retirement rows outside current filter** | **+314,249 (+7.3 %)** | **+629,754 (+19.7 %)** |
| Recommended scope column | `coalesce(page_url, post_page_url)` | `coalesce(page_url, post_page_url)` |

- **`post_page_url` misses ~2/3 of legacy retirement traffic.** Scoping on it would capture only
  1.32 M of the 4.0 M rows the same filter finds on `page_url`. **`coalesce(page_url, post_page_url)`
  is the correct scope column** — doc‑12's conclusion, reconfirmed on independent data.
- **The addable legacy traffic is French.** The +314,249 rows outside the current English filter are
  **essentially all `manulifeim.com/group-retirement/ca/fr`** paths (enroll‑now, explore‑plans,
  support, plan‑ahead, …). Combined with the new suite's +229,928 French `/ca/fr/particuliers/…`
  rows, **the French‑scope decision now spans two domains** (`manulifeim.com` legacy + `manulife.com`
  new) and is the single largest scope question for the business.
- **Columns still un‑audited.** Neither run has profiled `first_hit_page_url`, `visit_start_page_url`,
  or `site_url`, nor scanned `pagename` for "retirement". The manager explicitly listed these. That gap
  is closed by the new **S4c** section added to the EDA notebook (see
  [`eda/gwam_canada_retirement_eda.py`](../../eda/gwam_canada_retirement_eda.py)); it audits all five
  URL columns + `pagename` and does a window‑wide, rsid‑agnostic "retirement" sweep to size **how much
  retirement traffic exists beyond the two known suites**. It runs on the next Databricks execution.

---

## 5. Everything structurally unchanged (S2, S6, S9, S10, S11)

Confirming the migration changed tagging, not infrastructure — these match the new suite:

- **Same table & cadence:** 1,198‑column Delta, ~357 GB, partitioned by `process_date`, **~3‑min
  micro‑batch** writes (median inter‑arrival 0.05 h).
- **Every hit carries events:** `post_event_list` populated on **100 %** of hits. Legacy hits carry
  **more** events (p50/p95 = **32/37**, max 41) than the new suite (16/18, max 22) — consistent with
  the legacy suite's larger live‑eVar set (more instance‑of‑eVar presence flags per hit). **Event IDs
  are unresolved in this export** (old notebook); a current‑notebook re‑run will name them via
  `event.tsv`.
- **Geography & language richer toward Canada/French:** `geo_country` **CAN 95.7 %, USA 3.3 %**;
  `geo_region` **ON 51.6 %, AB 16.2 %, BC 13.6 %, QC 4.5 %**; `language` **45 (EN) 53 %, 39 (FR)
  40.5 %**. (The new suite's higher USA 11.5 % share is worth a business check — legacy US share was
  only 3.3 %.)
- **Data quality high:** key‑column null/blank ≤ **0.002 %**; `exclude_hit × hit_source` = (0,1) for
  100 % → **no server‑side bot filtering**; clock offset **−4/−5 h** (Eastern); one‑day dup check
  **0 %**.
- **No person‑level identity:** `cust_visid`/`post_cust_visid` **100 % null**; `userid` constant;
  visitors approximated from `mcvisid` (card 62,357 in‑window).

---

## 6. Implications for the anomaly‑detection build

| Signal from the legacy suite | Design consequence |
|---|---|
| Clean, datable cutover at 2026‑02‑01 | Encode **2026‑02‑01 as a hard change‑point**; never compare eVar KPIs across it. |
| +~2.5 yr history recoverable under `manugrs` | **Suite‑agnostic** KPIs (hits, visits, geography, language) become spliceable → seasonal/holiday models become fittable, lifting doc‑12's 158‑day ceiling. |
| Only 12 shared eVars; 50 legacy‑only | eVar‑derived KPIs are **not** cross‑suite splice‑able until the dictionary confirms shared semantics; treat old/new eVars as separate series. |
| `pagename` namespace renamed (`crt-public:` → `ca-ret:personal:`) | Map page taxonomies explicitly before any cross‑suite page‑level metric. |
| Legacy traffic more French, addable `/ca/fr` | French scope is a two‑domain decision; sizing lands in S4c. |
| Same cadence / DQ / identity as new suite | The ingestion, dedupe, bot, and identity assumptions in doc 12 carry over unchanged. |

To make the history splice real, the pipeline now carries a **`SCOPE_SUITE_MODE`** toggle
(`current_only` default; `with_legacy` unions `manugrs` on `coalesce(page_url, post_page_url)`) in
[`databricks/conf/settings.py`](../../databricks/conf/settings.py) and
[`databricks/src/01_bronze_ingest.py`](../../databricks/src/01_bronze_ingest.py) — **off until the
business signs off scope**, because flipping it re‑baselines every KPI.

---

## 7. Open questions for business / analytics owners

**Migration & history**
1. Confirm **`manugrs` → `manulifeglobalprod` migration date** (data says 2026‑02‑01). Is the legacy
   suite formally decommissioned, and is its pre‑2026‑02 history authoritative for backfill?
2. Should CA‑Retirement history **union the legacy suite** for model training, or start clean at the
   new suite? (Trades 158 days for ~2.5 years, at the cost of a change‑point.)

**Column mapping** (the manager's core question)
3. For the **12 shared eVars** (evar101–109, 131, 138, 162, 200), does each capture the **same
   dimension** in both suites? Only these can be spliced, and only if confirmed.
4. We need the **eVar dictionaries for *both* suites** — the 50 legacy‑only and 8 new‑only eVars are
   otherwise un‑mappable (content is masked under ADR‑0007).

**Scope**
5. Should the CA‑Retirement scope include **French `/ca/fr`** traffic? It is now a **two‑domain**
   decision: `manulifeim.com` (legacy, +314 K rows) *and* `manulife.com` (new, +230 K rows).
6. The new suite carries **USA 11.5 %** vs the legacy 3.3 %. Real end‑users, or advisor/internal/test
   traffic to exclude?

**Freshness / handover** — carry over doc‑12 Q9–Q11 unchanged.

---

## 8. Assumptions on record

1. The `manugrs` export was run from an **earlier notebook copy**: manifest sha1s are redacted
   (byte‑length integrity only) and event IDs are unresolved. A clean re‑run uses the current notebook.
2. `manugrs` deep‑profiling stats (S5–S11) are sampled from the **2025‑06 → 2026‑07** window, which
   includes the suite's collapse; full‑range volume figures are exact.
3. CA‑Retirement legacy scope = `rsid = manugrs` **AND** URL contains
   `manulifeim.com/group-retirement/ca/en` (notebook widgets). The recommended production scope column
   is `coalesce(page_url, post_page_url)`, not `post_page_url` alone.
4. eVar/prop *content* is masked (ADR‑0007); "same column live in both suites" is a **necessary, not
   sufficient**, condition for "same meaning."
5. All other doc‑12 assumptions (Eastern time, empty‑string‑not‑NULL, `language` numeric IDs,
   `clean_hits == hits` because no bot filtering, daily grain) hold for `manugrs` too.

---

### Provenance
All figures reproduced from executed cells in `eda/gwam_canada_retirement_eda_manugrs.ipynb`
(S1–S12 `SHAREABLE` blocks) and cross‑referenced against
`eda/gwam_canada_retirement_eda.ipynb`, both production runs of 2026‑07‑10. Merged extraction:
`scratchpad/manugrs_cross_suite_facts.json`. **Integrity of the `manugrs` export is byte‑length only**
(sha1 redacted by the pre‑fix scrubber). Related records:
[`12-eda-findings-analysis.md`](12-eda-findings-analysis.md),
[`10-data-profile-alignment.md`](10-data-profile-alignment.md),
[`11-privacy-identity-governance.md`](11-privacy-identity-governance.md).
