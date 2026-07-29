# 19 — GWAM Canada Retirement: Multi-Channel Alerting Readiness & SME Gap Assessment

> **Purpose.** The business SME has sent a **four-channel alerting scope** for Canada Retirement
> (Public Website / Web Member / Mobile / ManulifeID) with a six-metric applicability matrix. This
> document (a) records that table as the scope spec of record, (b) maps **every cell** to what
> actually exists in this repo, (c) lists the **engineering gates** that are ours to close, and
> (d) states exactly **what we need from business / SME** — with the blockers called out. Companion
> to [16 — E2E Production Blueprint](16-e2e-production-blueprint.md) (standing decisions),
> [15 — Consolidated EDA Report](15-consolidated-eda-report.md) (the GWAM EDA of record), and the
> CoverMe precedent [17](17-coverme-eda-readiness.md) / [18](18-coverme-sme-questions.md). The
> send-ready questionnaire is [20 — GWAM SME Questions](20-gwam-sme-questions.md).

**Status:** Written 2026-07-28, **pre-probe.** Unlike doc 17 — which was written after the CoverMe
EDA had run — this assessment is written *before* `gwam_channel_discovery` executes. Cells marked ⏳
are answerable from data and are waiting on that run (**G1**). Revise this document with the results
before treating any ⏳ as settled.

---

## 0. Context — why this assessment exists

The GWAM pipeline in this repo monitors **one** report suite, `manulifeglobalprod`, scoped by **URL
`LIKE` patterns** ([settings.py:19-60](../../databricks/conf/settings.py)). Everything downstream —
bronze filter, silver conform, the 35 gold series, the detector thresholds — sits on that one
definition of "Canada Retirement."

The SME table describes a different product: four channels, four report suites, scoped by **segment
fields** rather than URLs, with two metric families (**Errors**, **Sign-in**) that have never been
built. It also asks for exactly the traffic that standing decision **D8** removes.

This is the same situation CoverMe was in on 2026-07-27, and the same treatment applies: write down
what we were given, verify every cell, answer from data what we can, and send the business a short
list of genuine decisions rather than a long list of things we could have looked up ourselves.

### ⭐ Headline: three of the four report suites do not exist anywhere in this repo

`GRS+`, `GBRS Mobile App - Production`, and `manucustomer.prod` appear in **zero** files — not in
`databricks/conf/`, not in any doc, not in any test. Only `Manulife Global Prod` maps to something we
run (`manulifeglobalprod`).

There is one strong lead. The unfiltered rsid census over
`gwam_prod_catalog.inv_typed_common.adobe_hit_data` puts **`manufingbrsmobileapp.prod` at 56.9% of
all rows** — the single largest report suite in our source table, and almost certainly the SME's
"GBRS Mobile App - Production" ([12 §](12-eda-findings-analysis.md); it also appears as the
`grs-mobile` tile at [gmai-pulse-concept.html:549](../../frontend/gmai-pulse-concept.html), which is
concept UI, not governed config). If that identification holds, **the majority of our source table is
Canada-Retirement-relevant traffic we have never touched.**

That is the optimistic reading. The pessimistic one is equally important: `manucustomer.prod` and any
GRS+ suite do **not** appear in the top-10 rsid census at all, and an rsid absent from this table is
not proof the suite doesn't exist — it may sit in a different Adobe instance. The SME's own table
notes Mobile lives under a **different Adobe login company** ("Manulife Financial"). Whether that is a
naming detail or a separate data feed we lack access to is the difference between a two-week build
and a data-acquisition project.

---

## 1. The SME table as received

Recorded verbatim. Also encoded in [metric-registry.yaml](metric-registry.yaml)
`meta.gwam_sme_inputs` and in the probe's `SME_CHANNELS` constant, so the emitted payloads can be read
straight against it.

| | Public Website | Web Member | Mobile | ManulifeID | Notes |
|---|---|---|---|---|---|
| **Instance** | Manulife | Manulife | Manulife **Financial** | Manulife | |
| **Report Suite** | Manulife Global Prod | GRS+ | GBRS Mobile App - Production | manucustomer.prod | |
| **Segment** | Brand (evar105) = `ca-retirement :  : GWAM` | Platfrom - v185 = MPS Member | Canada Retirement App Pages v2 | *(blank)* | "Not sure how to seperate Retirement from other ManulifeID signins" |
| Page Views (count) | 1 | 1 | 1 | 1 | Ideally non-marketing |
| Visits (count) | 1 | 1 | 1 | 1 | Ideally non-marketing |
| Visitors (count) | 1 | 1 | 1 | 1 | Ideally non-marketing |
| Errors (count) | 0 | 1 | 1 | 1 | |
| Sign in % rate completion | 0 | 0 | 0 | 1 | |
| Sign in Error (count) | 0 | 0 | 0 | 1 | |

**Reading assumption (unconfirmed — [20](20-gwam-sme-questions.md) Q4):** `1` = in scope, `0` = not in
scope, giving **17 (metric × channel) pairs**. The alternative reading — `1` as a priority rank — would
change the deliverable, so it is asked rather than assumed.

**Two things the table is silent on** and which no query can recover: alert thresholds/severity per
channel, and whether the four channels alert independently or roll up to one Canada-Retirement number.

---

## 2. What each cell maps to today

### 2.1 Report suites

| Channel | SME label | Repo reality | Verdict |
|---|---|---|---|
| Public Website | Manulife Global Prod | `manulifeglobalprod` — the one suite the pipeline ingests ([settings.py:19](../../databricks/conf/settings.py)) | ✅ Known |
| Mobile | GBRS Mobile App - Production | `manufingbrsmobileapp.prod`, **56.9%** of the source table, entirely unmodelled. Name never confirmed against the SME's label. | 🟡 Probable (⏳ **C1**) |
| Web Member | GRS+ | No matching rsid in config, docs, or the top-10 census. | 🔴 Unidentified (⏳ **C1**) |
| ManulifeID | manucustomer.prod | Not in config; not in the top-10 census. Presence in our source table unverified. | 🔴 Unidentified (⏳ **C1**) |

### 2.2 Scope model — the re-baseline problem

The SME scopes by segment; we scope by URL. The good news is that the segment value is
*self-consistent with our own dictionary*: eVar105 is documented as `Brand | Line of Business |
Segment`, "delimited multi-value; Segment ∈ CA / JH / GWAM / Asia — **the scope discriminator**"
([16 §3](16-e2e-production-blueprint.md), [15 §](15-consolidated-eda-report.md)). The SME's
`ca-retirement :  : GWAM` splits on `" : "` into exactly `["ca-retirement", "", "GWAM"]` — Brand,
blank Line of Business, Segment. That is the documented triple, not a coincidence.

If it holds, segment-scope is **better** than what we ship: it is language-agnostic, so it would close
the French-traffic gap doc 16 calls *"the single largest scope gap"* ([16 §2](16-e2e-production-blueprint.md))
without needing FR URL patterns at all.

Two hard constraints on acting on it:

- **Any scope change is a re-baseline event.** Stated identically at [settings.py:25-31](../../databricks/conf/settings.py)
  and [:88-90](../../databricks/conf/settings.py): flipping scope re-baselines every downstream KPI,
  detector threshold, and injected-anomaly calibration; done under `mode=incremental` it also writes a
  step change mid-series that the detector reads as a level-shift anomaly. Any flip must be a full
  `mode=backfill` with gold truncated. **C3** measures the magnitude (segment-only vs url-only rows)
  so this is a sized decision, not a leap.
- **A URL-based scope cannot express the Mobile channel at all.** Every scope predicate in the repo is
  a SQL `LIKE` on a URL column ([01_bronze_ingest.py:62-101](../../databricks/src/01_bronze_ingest.py));
  a hit with no page URL cannot enter scope. App hits carry no page URL. **C2** confirms this per suite.

### 2.3 ⚠️ The D8 conflict — the single biggest blocker

**D8** ([16 §1](16-e2e-production-blueprint.md)) is a business rule dated **2026-07-20**: *"Individual-login
traffic is out of anomaly scope."* It is encoded as `SCOPE_LOGIN_HOST_EXCLUDE`
([settings.py:62-77](../../databricks/conf/settings.py)) and subtracted from `suite_scope` in **every**
URL and suite mode ([01_bronze_ingest.py:90-97](../../databricks/src/01_bronze_ingest.py)). It hard-excludes
`id.manulife.ca`, `portal.manulife.ca`, `grsmembers.manulife.com`, `gsrs1.manulife.com`,
`viproom.manulife.com`, `portail.manuvie.ca`. Doc-16 §3's EDDL prioritization reinforces it:
**Logins = CAR-applicable No**.

The new table requires precisely that traffic. The **ManulifeID** channel *is* the sign-in system, and
"Web Member / MPS Member" is the authenticated member portal. Two business rules from the same
stakeholder group now point in opposite directions.

**This is not ours to reconcile, and D8 is not silently reversed here.** It stays in force until the
authority that issued it rules otherwise ([20](20-gwam-sme-questions.md) Q1). Note the stakes: doc-16 §2
records that ~94% of the `manugrs` suite is D8 login traffic, so this ruling moves an order of
magnitude more data than any other question on the list.

### 2.4 Metrics

| Metric | Exists? | Detail |
|---|---|---|
| Page Views | 🟡 Approximately | We compute `hits_total` = row count ([gold_lib.py:149-154](../../databricks/src/gold_lib.py)) and surface it as page views. Adobe's *page views* are narrower than *hits*. Which the SME means changes the number ([20](20-gwam-sme-questions.md) Q6). |
| Visits | ✅ | `visits_total` = distinct `concat(post_visid_high, post_visid_low, visit_num)`. |
| Visitors | ✅ | `visitors_total` = distinct `mcvisid`. |
| Errors | 🔴 Nothing | No error metric anywhere. eVar181-184 (Error Code / Description / Type / Category) are labeled only in [gwam_canada_retirement_eda.py:169-179](../../eda/gwam_canada_retirement_eda.py); `event173` is listed in [16 §3](16-e2e-production-blueprint.md). `RULE_DIMS` ([registry.py:108](../../detect/registry.py)) has no error dimension. ⏳ **C5** |
| Sign in % rate completion | 🟡 Engine ready, inputs missing | `gold_lib.build_kpis_spark` already has a **`ratio`** kind that divides one metric by a sibling ([gold_lib.py:168-175](../../databricks/src/gold_lib.py)) — exactly how CoverMe's funnel works ([cm_registry.py:151-162](../../detect/cm_registry.py)). But GWAM's `SeriesSpec` ([registry.py:68-83](../../detect/registry.py)) is `count \| rate \| share` only, with no numerator/denominator (**G2**), and nothing marks a sign-in attempt vs success (⏳ **C6**). |
| Sign in Error | 🔴 Nothing | eVar122 "Login Step" / eVar135 "Login Method" labeled in the EDA only. ⏳ **C6** |

Compounding all of it: **GWAM's 23 tracked event ids are unresolved** ([registry.py:18-22](../../detect/registry.py)),
and the registry's own `meta.report_suite_caveat` ([metric-registry.yaml:57-61](metric-registry.yaml))
warns that CoverMe's ids do not transfer to GWAM. The three new suites' event spaces are wholly
unknown (⏳ **C7**).

### 2.5 "Ideally non-marketing"

Undefined operationally; no marketing discriminator is implemented for GWAM. **C10** profiles the
candidates (campaign / ref_type / referrer / channel) so [20](20-gwam-sme-questions.md) Q5 can offer
concrete options instead of an open question.

---

## 3. Engineering gates (ours to close — no SME needed)

Numbered **G1–G6**. Deliberately *not* the `E1–E4` series: that namespace belongs to CoverMe in
[17 §3](17-coverme-eda-readiness.md) and reusing it would collide across products.

| # | Gap | Evidence | Fix | Impact if unfixed |
|---|---|---|---|---|
| **G1** | **The discovery probe has not been run.** Ten of the questions in §2 are answerable from data, not from the SME. | [`eda/gwam_channel_discovery.py`](../../eda/gwam_channel_discovery.py) written 2026-07-28, never executed. | Run it on Databricks; paste the SHAREABLE blocks back; fold results into §2 and §4. **Check `run_manifest.skipped == {}`** — a silently skipped section is how CoverMe's S6 went missing (doc-17 E1). | Every ⏳ above stays open, and we ask the SME questions we could have answered ourselves. |
| **G2** | **GWAM's `SeriesSpec` cannot express a ratio or carry governance.** The Spark engine supports `ratio`; the GWAM Python spec does not. | [registry.py:68-83](../../detect/registry.py) — `kind` is `count \| rate \| share`; no `numerator`/`denominator`, no `status`/`direction`/`owner`. `CmSeriesSpec` ([cm_registry.py:106-134](../../detect/cm_registry.py)) has all of them. | Port the missing fields from `CmSeriesSpec`. `gold_lib` needs **no** change — [gold_lib.py:168-175](../../databricks/src/gold_lib.py) already resolves ratios by sibling `metric_id`. | "Sign in % rate completion" cannot be declared at all, and GWAM metrics stay ungoverned (no owner/status). |
| **G3** | **No error or sign-in columns reach bronze/silver.** | [bronze_columns.py](../../databricks/conf/bronze_columns.py) — `DETECTOR_COLUMNS` / `SILVER_COLUMNS` carry no eVar181-184, eVar122, eVar135. | Add them once **C5/C6** confirm which are populated. | Errors and Sign-in Errors are unbuildable regardless of any SME ruling. |
| **G4** | **Scope has no channel dimension.** `SCOPE_RSID` is a single string and the predicate is one rsid AND a URL match. | [settings.py:19](../../databricks/conf/settings.py), [01_bronze_ingest.py:62-101](../../databricks/src/01_bronze_ingest.py). | Per-channel scope config (rsid + its own segment predicate), and a `channel` column carried to gold so metrics break down by it. **Blocked on the D8 ruling** — do not build until §4 item 1 lands. | A four-channel product cannot be expressed. Note this is also the change that re-baselines everything (§2.2). |
| **G5** | **GWAM has no registry pin or drift test.** CoverMe has both. | [test_registry_yaml.py](../../tests/test_registry_yaml.py) covers only the three CoverMe sheets; `detect/registry.py` has no `REGISTRY_VERSION`. | Pin GWAM's binding to the YAML and add a drift guard, mirroring `test_series_governance_matches_yaml`. Seeded 2026-07-28 by `test_gwam_channel_seed_counts`. | GWAM metric definitions can drift from the governed registry silently — the exact failure the CoverMe test was written to prevent. |
| **G6** | **No test covers the scope constants.** | Nothing asserts `SCOPE_RSID`, `SCOPE_URL_MODE`, `SCOPE_SUITE_MODE`, or `SCOPE_LOGIN_HOST_EXCLUDE`. CoverMe's equivalent predicate *is* tested ([test_cm_silver.py](../../tests/test_cm_silver.py) `test_scope_expr_include_minus_exclude`). | Add scope-predicate unit tests before touching scope. | The single highest-consequence config in the repo (§2.2 re-baseline) is unguarded — precisely the wrong thing to change untested. |

---

## 4. What we need from Business / SME (the clarity gaps)

Ranked by how hard each blocks the build. The send-ready version is [20 — GWAM SME Questions](20-gwam-sme-questions.md);
this table is the technical agenda behind it.

| # | What we need clarified | Why it blocks | Our current assumption | Resolving artifact |
|---|---|---|---|---|
| **1** | 🚩 **Does the four-channel scope supersede the 2026-07-20 login-exclusion rule (D8)?** Two channels require the traffic D8 removes. | Scope is the denominator of every metric. D8 is enforced in *every* mode and moves ~94% of one suite. | D8 stands until explicitly reversed. **We have not changed it.** | Written ruling from the authority that issued D8 → doc 20 Q1 |
| **2** | 🚩 **The actual report-suite IDs for "GRS+" and "manucustomer.prod".** | Three of four channels cannot be located in our source table. | `manufingbrsmobileapp.prod` = the Mobile suite (probable, ⏳ C1); the other two unknown. | The rsid strings, or Adobe admin access → doc 20 Q2 |
| **3** | 🚩 **Is `ca-retirement :  : GWAM` the literal eVar105 value, and does segment-scope replace our URL filter?** | Switching the scope model re-baselines every KPI and threshold; it must happen before baselines are fit, not after. | It is the documented `Brand \| LoB \| Segment` triple, `" : "`-delimited. ⏳ C3 sizes the change. | Confirmation + sign-off on the re-baseline → doc 20 Q3 |
| **4** | **Does `1`/`0` mean in/out of scope?** And do the four channels alert independently? | Determines whether the deliverable is 17 metrics or a ranked subset. | 1 = in scope; channels alert independently. | One-line confirmation → doc 20 Q4 |
| **5** | **What defines "marketing" traffic?** | Three of six metrics carry the "ideally non-marketing" qualifier. | Campaign-tagged is the likely definition; ⏳ C10 bounds the options. | A rule we can encode → doc 20 Q5 |
| **6** | **"Page Views" — Adobe page views, or all hits?** | We currently report hits. The two differ materially. | Adobe page views. | Confirmation → doc 20 Q6 |
| **7** | **Which field is the error of record, and is "count" errors or affected visits?** | No error metric exists; we would be guessing at both field and grain. | eVar181-184; count = error hits. ⏳ C5 | Field + grain → doc 20 Q7 |
| **8** | **Numerator and denominator of "Sign in % rate completion", and over what unit.** | A ratio without a defined denominator is not a metric. | Successful sign-ins ÷ sign-in attempts, at visit level. ⏳ C6 | Definition → doc 20 Q8 |
| **9** | **How to separate Retirement from other ManulifeID sign-ins** — the SME's own open item. | Without a discriminator the ManulifeID channel cannot be scoped to Canada Retirement. | None known. ⏳ C9 profiles every candidate; if none isolates it, this is a **tagging change**, not something solvable downstream. | A field + value, or acceptance that it needs new tagging → doc 20 Q9 |
| **10** | **Is the "Manulife Financial" Adobe instance the same feed?** | If Mobile is a separate feed we lack access to, this is data acquisition, not modelling. | Same feed — `manufingbrsmobileapp.prod` is visible in our catalog. ⏳ C1 | Confirmation from Adobe admin → doc 20 Q10 |
| **11** | **The definition of the "Canada Retirement App Pages v2" segment.** | It is an Adobe segment name; we cannot implement a name. | Translatable to a pagename prefix. ⏳ C8 | The segment definition → doc 20 Q11 |
| **12** | Alert thresholds / severity per channel, and a named owner per metric. | Not blocking — detection can be built with defaults and tuned. | Reuse the existing severity ladder (`warn 3.5 / minor 5.0 / major 8.0 / critical 12.0`). | Non-blocking attachment → doc 20 Q12 |

---

## 5. Gap to the full build (roadmap, post-SME)

**A. Close what data can close — now.** Run **G1**. Fold the results into §2 and §4; every ⏳ either
becomes a fact or moves to §4 with a stated reason it could not be settled from data. This is the only
step that needs nothing from anyone else.

**B. Get the three blocking rulings.** Items 1–3 in §4. Until item 1 lands, **G4 must not be built** —
a channel-aware scope that assumes the wrong answer on D8 is worse than no scope change, because it
re-baselines everything in the wrong direction.

**C. Engineering, in dependency order.** G2 (SeriesSpec ratio + governance) and G6 (scope tests) are
safe to do immediately — neither depends on a ruling. G3 follows C5/C6. G4 follows item 1. G5 follows
the GWAM registry entries being promoted past `candidate`.

**D. Then the pipeline.** Per-channel bronze scope → `channel` carried through silver → gold series per
(metric × channel) → detector wiring. A full `mode=backfill` with gold truncated, per §2.2. Sequenced
after A–C; nothing here is startable today.

**E. Doc hygiene.** [README.md](README.md)'s index and its "#1 blocker" blockquote are stale (tracked
since [17 §5](17-coverme-eda-readiness.md) D). Index rows for 17–20 are added in this pass; the stale
blocker text and the Synapse-era claims in docs 01/02/03/10/11 remain open.

---

## 6. Readiness verdict

| Area | Status |
|---|---|
| SME scope table received & recorded | ✅ **Complete** — §1, and `meta.gwam_sme_inputs` in the registry |
| Report suites identified | 🔴 **1 of 4 confirmed** — two unlocated, one probable pending C1 |
| Discovery probe | 🟡 **Written, not run** (G1) — [`gwam_channel_discovery.py`](../../eda/gwam_channel_discovery.py) |
| Scope model (URL → segment) | 🔴 **Blocked** — on SME item 3 *and* C3 sizing; re-baseline event either way |
| D8 / login-traffic conflict | 🔴 **Blocked on SME** — item 1, the biggest single question on the list |
| Page Views / Visits / Visitors | ✅ **Engine ready** — exist today; only the page-views definition is open (item 6) |
| Errors | 🔴 **Not implemented** — field unknown (C5), columns not in bronze (G3) |
| Sign-in completion rate | 🟡 **Engine ready, inputs unknown** — `ratio` kind exists; `SeriesSpec` can't express it (G2); no attempt/success marker known (C6) |
| Sign-in errors | 🔴 **Not implemented** |
| Mobile-app ingestion | 🔴 **Net-new** — pipeline is URL-scoped end to end; app hits carry no URL (C2, G4) |
| ManulifeID retirement split | 🔴 **Open — SME's own flagged unknown** (item 9, C9) |
| Governance (GWAM registry pin + drift test) | 🟡 **Seeded** — 17 candidate entries at v0.4.0; binding pin still open (G5) |

**Bottom line.** We have a clear, recorded scope ask and a precise map of what it collides with — but
GWAM is **not** in the position CoverMe reached on 2026-07-27. CoverMe's blockers were labels on data
we already had; **GWAM's are the data itself**: three of four report suites are unlocated, and the two
sign-in metrics require reversing a standing business rule. The critical path is therefore *not* our
engineering — it is (1) running the probe, which is a day's work and closes ten questions, and (2)
three business rulings, of which the D8 conflict is by far the largest. Nothing in the pipeline should
change until those land; the pieces that are safe to build in the meantime are G2 and G6.

---

## 7. Verification / how to confirm this is done

1. **Probe runs clean.** `run_manifest.skipped == {}` and all 11 sections present. A non-empty
   `skipped` map means the run is *not* complete coverage, regardless of how much output appeared.
2. **Every ⏳ in §2 and §4 is resolved** — either flipped to a fact with the emitting section cited
   (e.g. "C3: 41.2% of `manulifeglobalprod` carries `ca-retirement`"), or moved to §4 with the reason
   it could not be settled from data.
3. **D8 is still in force in code.** `git diff databricks/conf/settings.py` is empty; `SCOPE_LOGIN_HOST_EXCLUDE`
   is unchanged. This document flags the conflict; it does not resolve it.
4. **Registry seeded, tests green.** `pytest tests/ -q` passes, including `test_gwam_channel_seed_counts`
   (17 candidate entries) and the untouched `test_gold_parity` (35 GWAM series, Spark↔pandas).
5. **Doc 20 reads as an email.** No repo jargon, no unexplained eVar numbers, blockers first.
