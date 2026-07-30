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

> ### ↺ REVISION 2026-07-29 (later the same day) — the scope narrowed to ONE channel
>
> **The SME (Abhisekh) has ruled: "Currently we are only going with *Public Website* in scope."**
> Everything below was written to assess four channels. It is kept — the per-suite evidence is what
> makes a future re-widening cheap — but read it knowing that **three of the four channels are now
> `deferred`**, and that this ruling changes the *shape* of the readiness verdict, not just its
> scale. Specifically:
>
> - **§2.3's D8 conflict is DISSOLVED, not resolved.** Both channels that needed login traffic left
>   scope. D8 was never adjudicated and stands unchanged; the conflict returns verbatim if scope
>   re-widens. Calling it "the single biggest blocker" is no longer accurate.
> - **§2.2's argument for the segment scope model collapses.** Its load-bearing claim was that *the
>   other three channels cannot be expressed by URL at all*. With only the public website in scope,
>   segment-vs-URL is a straight +1,436 / −60,594 row trade — see the ↺ note in that section.
> - **The `manucustomer.prod` access request is MOOT** — the programme's longest-lead item retires.
> - **§3's gates re-rank:** G4 (no channel dimension) and G3 (error columns) go moot; **G2 becomes
>   critical**, because the SME's new anomaly signals are per-visit *ratios* and `SeriesSpec` cannot
>   express one.
> - **Two questions were answered and two are new.** Q5 ("what is marketing?") is answered — see the
>   new **§2.5.1**. Q3 is partly answered and spawns **Q3b** (the `wealth-ca` / `pvt-wealth` brand
>   variants). Q6 (page views vs hits) is **escalated**, since the new signals divide by that
>   numerator. ↺ **Q3b was answered 2026-07-30 — both variants ruled OUT of Canada Retirement, which
>   confirms the predicate this document had held, so no re-baseline follows. Q6 is now the only open
>   SME answer on the critical path.**
> - **Three new anomaly signals arrived** — see the new **§1.1**.
>
> Registry effect: [metric-registry.yaml](metric-registry.yaml) **v0.5.0** — 5 `candidate`
> public-website entries (3 traffic + 2 new signal seeds), 14 `deferred`.

**Status:** Written 2026-07-28 pre-probe; **revised 2026-07-29 with the probe results — G1 is closed**;
**re-scoped to a single channel later on 2026-07-29 (banner above).**
`gwam_channel_discovery` ran on Databricks (`generated_at` 2026-07-29T02:00:54, window
2026-04-29 → 2026-07-28, 90 days, table `gwam_prod_catalog.inv_typed_common.adobe_hit_data`,
1,198 columns). **11 SHAREABLE sections emitted, `run_manifest.skipped == {}`, `complete: true`** —
export tracked at [`gwam_channel_discovery.html`](../../gwam_channel_discovery.html). Every ⏳ marker
below has been replaced by its result, cited as **C1–C10**. Findings that *contradicted* the
pre-probe text are called out inline as **↺ corrected**, not silently overwritten.

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

### ⭐ Headline: ↺ **three of the four report suites are in our data after all — one is not**

The pre-probe version of this section said all three unknown suites "do not exist anywhere in this
repo." That was true of the *repo* and false of the *data*. **C1**'s unfiltered full-history census
over `gwam_prod_catalog.inv_typed_common.adobe_hit_data` (3.25B rows, **16 rsids**) locates three of
the four channels:

| SME label | rsid | Rows (full history) | Share | Days of history |
|---|---|---|---|---|
| Manulife Global Prod | `manulifeglobalprod` | 9,108,890 | 0.28% | **138** (from 2026-03-10) |
| GBRS Mobile App - Production | `manufingbrsmobileapp.prod` | 2,239,037,706 | **68.9%** | 881 |
| GRS+ | **`manugrs`** | 322,394,428 | 9.92% | 883 |
| manucustomer.prod | **— absent —** | 0 | — | — |

Three consequences, in order of how much they change the plan:

1. **`manugrs` is the Web Member suite.** Not a guess: **C4** shows `post_evar185 = 'MPS Member'`
   covers **54.14%** of `manugrs` and **100% of its populated eVar185** — the SME's Web Member
   predicate, verbatim, on a suite we already knew about but had only ever considered as a
   cross-suite curiosity ([14](14-manugrs-cross-suite-analysis.md)). The rsid *string* still wants
   SME ratification ([20](20-gwam-sme-questions.md) Q2), but the identification is data-confirmed.
2. **↺ Mobile is 68.9% of the table, not 56.9%.** The pre-probe figure came from a partial census;
   the corrected full-history share is higher. It remains entirely unmodelled — **the majority of our
   source table is traffic we have never touched.**
3. **`manucustomer.prod` is genuinely not in our feed** — **C9** found **0 rows** in the window and
   it is absent from all 16 rsids. This is no longer a modelling question. It is a **data-access
   request**, and it is the longest-lead item on the list, so it should be started now rather than
   at build time.
   > **↺ 2026-07-29 (single-channel ruling): withdraw this request.** ManulifeID is out of scope,
   > so the longest-lead item on the programme retires without ever being started. Keep the finding:
   > if scope re-widens, the access request is still the first thing to file.

> **↺ 2026-07-29 — this headline is now historical.** Locating three of the four suites was the
> right answer to the question being asked on 2026-07-28. Under the single-channel ruling the
> operative line is the *last* paragraph of this section, not the first: **`manulifeglobalprod`'s
> 138 days of history is the whole baseline story**, because it is the only suite in scope. The
> 68.9% mobile mass and the 322M-row `manugrs` suite are no longer "traffic we have never touched"
> in a way that matters to this programme — they are simply out of scope.

The `manulifeglobalprod` history is the quiet risk: **138 days**, first day 2026-03-10 (the
discrepancy [12 §](12-eda-findings-analysis.md) already flagged). It clears the ≥90-day baseline gate,
but with far less margin than the other two suites, and any scope change consumes that margin.

One caveat the probe cannot lift: an rsid absent from this table is still not proof the suite doesn't
exist — it may sit in a different Adobe instance. The SME's table puts Mobile under a **different
Adobe login company** ("Manulife Financial"), yet `manufingbrsmobileapp.prod` is present in *this*
feed, which is suggestive but not conclusive ([20](20-gwam-sme-questions.md) Q10).

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

> **↺ 2026-07-29 — only the Public Website column survives.** The 17-pair reading collapses to
> **three live pairs**: Page Views, Visits, Visitors on the public website. Errors and both sign-in
> metrics leave scope entirely with their channels, which also makes Q4 moot for them — there is no
> longer a question about whether four channels alert independently. The table stays as received.

**Two things the table is silent on** and which no query can recover: alert thresholds/severity per
channel, and whether the four channels alert independently or roll up to one Canada-Retirement number.

### 1.1 ↺ New — three anomaly signals the SME suggested (2026-07-29)

Alongside the scope ruling he named three things worth watching. These are **not** in the matrix
above; they arrived as observations about what tends to indicate a problem on this site, and they are
the first metric suggestions we have received that came from operational instinct rather than a
report-suite inventory. All three are seeded in
[metric-registry.yaml](metric-registry.yaml) `gwam_channel_metrics`.

| SME wording | What it means for us | Status |
|---|---|---|
| "Unique ECID - unique visitor" | **Already covered.** `gwam_pw_visitors` resolves to `countDistinct(mcvisid)` ([gold_lib.py:94](../../databricks/src/gold_lib.py)) and `mcvisid` *is* the ECID. ✅ The grain divergence we flagged — the EDA notebooks count the `post_visid` pair instead ([gwam_canada_retirement_eda.py:1320](../../eda/gwam_canada_retirement_eda.py)) — is **measured and negligible** (C12, below). | ✅ **settled** |
| "Page view per visit (if page view < 1 could be anomaly)" | New metric `gwam_pw_pv_per_visit`. Note what the test really detects: a daily ratio can only fall **below 1** if visits exist containing **no page view at all**, so this is a zero-page-view-visit detector. ⚠ **C12 says the literal test never fires** — see below. | 🟡 blocked on **Q6** only |
| "If all pages are consistently at 2 (sometimes an indicator of duplication) especially when we see consistently 2" | New metric `gwam_pw_pv_per_visit_dup2` — the share of visits with **exactly** 2 page views. His concern is **consistency**, not level: an unusually *stable* point mass. No current scorer (robust-z, level-shift, ECOD, rules) expresses "unusually stable", so this needs a new rule kind, not a threshold. C12 gives it a stability baseline. | 🟡 blocked on **Q6** + new detector kind |

**↺ 2026-07-30 — C12 has run, and it changes two of these three rows.** 88 days, rsid
`manulifeglobalprod`, measured on the Adobe page-view basis (`pv_basis` =
`try_cast(post_page_event as int) == 0`).

- **The grain question is closed.** ECID and the visid-pair agree on 74 of 88 days; on the 14 that
  differ the gap is at most **15 visitors** (max 0.068%, mean 0.0039%), and ECID never *exceeds* the
  pair — the expected direction, since a pair can split one ECID but cannot merge two. There is no
  reconciliation work and no re-baseline risk: either grain answers his signal.
- **The "< 1" test is inert as specified.** `pv_per_visit` = **1.3219** (segment 1.3429), daily min
  **1.2236**, max 1.5571 — on **zero of 88 days** did it approach 1.0, let alone fall below. The
  floor sits 22% above the threshold. The reason is dilution: 78.8% of visits have exactly one page
  view, which swamps the zero-page-view visits in the mean. The quantity that *is* detectable is
  `share_pv_eq_0` = **3.25%** (segment 2.38%). We should alert on that share, not on a ratio
  crossing 1 — put to the SME as Q6 in [20](20-gwam-sme-questions.md).
- **dup2 now has a stability baseline.** `share_pv_eq_2` = **11.67%** suite / 12.66% segment. Since
  the signal is about steadiness, the dispersion is the number that matters: daily range
  0.0768–0.1350, sd 0.0140, **cv 0.122** (segment cv 0.099). That is the normal wobble a
  "suspiciously steady" detector has to fire *below*. Full mix: 0pv 3.3% / 1pv 78.8% / 2pv 11.7% /
  3–5pv 5.2% / 6+ 1.1%.

**G2 is closed** (2026-07-30) — `SeriesSpec` now carries `numerator`/`denominator` plus
`status`/`direction`/`owner`, `kpis.py` resolves `kind=ratio` in a second pass matching `gold_lib`,
and [test_gold_parity.py](../../tests/test_gold_parity.py) pins pandas/Spark agreement including the
zero-denominator day. Both metrics are now *declarable*; neither is *declared*, because both divide
by "page views" and **Q6 — page views or hits? — is the one remaining blocker**, and it is an SME
answer rather than engineering. C12 also priced Q6: on this scope the two bases give **2.885**
(hits/visit, derived) versus **1.343** (Adobe page views), so the "consistently 2" signal is
meaningful under one basis and meaningless under the other.

---

## 2. What each cell maps to today

### 2.1 Report suites

| Channel | SME label | Data reality (C1/C2/C4) | Verdict |
|---|---|---|---|
| Public Website | Manulife Global Prod | `manulifeglobalprod` — the one suite the pipeline ingests ([settings.py:19](../../databricks/conf/settings.py)). 9.1M rows, **138 days** of history. `verdict=web` (URL rate 1.000). | ✅ **Confirmed** |
| Mobile | GBRS Mobile App - Production | `manufingbrsmobileapp.prod`, **68.9%** of the source table (↺ was 56.9%), 881 days, entirely unmodelled. `verdict=app_or_mixed` — **URL rate 0.000**. | ✅ **Present** — SME to ratify the name (Q2/Q10) |
| Web Member | GRS+ | **`manugrs`** — 322M rows, 883 days. `post_evar185='MPS Member'` = 54.14% of the suite, **100% of its populated eVar185**. `verdict=web` (URL rate 1.000). | ✅ **Data-confirmed** — SME to ratify the rsid string (Q2) |
| ManulifeID | manucustomer.prod | **0 rows** (C9). Absent from all 16 rsids in the census. | 🔴 **Not in our feed** → access request (Q2) |

**The `MPS Member` tag is not suite-unique.** It also covers 18.16% of `manufingbrsmobileapp.prod`
(≈38M hits), which is consistent with the mobile app hosting the same MPS retirement platform. Every
channel predicate must therefore be **suite AND segment**, never segment alone.

### 2.2 Scope model — the re-baseline problem

The SME scopes by segment; we scope by URL. **C3 confirms the SME's value is real**:
`ca-retirement :  : GWAM` is a **literal** `post_evar105` value with **776,860 hits** in the window.
A second Canada-Retirement form also exists — `Manulife : GWAM : group-plans:ca-retirement`
(526,357) — so any predicate must be a `contains`-style match on *ca-retirement* + *GWAM*, not
string equality against the one value the SME quoted.

**↺ Corrected — the delimiter is `":"`, not `" : "`.** The pre-probe text asserted `" : "` from the
doc's `Brand | LoB | Segment` shorthand. C3 measured all six candidates instead of assuming:

| Delimiter | avg parts | % splitting into exactly 3 |
|---|---|---|
| `":"` | 2.380 | **37.62%** |
| `" : "` | 1.037 | 1.84% |
| `" \| "`, `"\|"`, `" - "`, `","` | 1.000 | 0.00% |

The documented triple still holds — `ca-retirement :  : GWAM` on `":"` yields
`["ca-retirement ", "  ", " GWAM"]`, i.e. Brand / blank LoB / Segment once trimmed — but any parser
we write must split on the bare colon and trim, or it will silently produce one part instead of three.

**↺ Corrected — segment scope is *narrower* than URL scope on the Public Website, not wider.** The
pre-probe text claimed segment scope "would close the French-traffic gap." C3's sizing on
`manulifeglobalprod` over the window says otherwise:

| | Rows |
|---|---|
| Window rows | 6,148,797 |
| Today's URL scope (broad) | 1,418,435 |
| Segment scope (`ca-retirement` + `GWAM`) | 1,304,325 |
| In both | 1,302,889 |
| **Segment only — traffic we would GAIN** | **1,436** |
| **URL only — traffic we would LOSE** | **60,594** |

The two models agree on **~96%** of the traffic. Switching would gain 1,436 rows and lose 60,594.
Two caveats before reading that as an argument against switching: `URL_SCOPE_BROAD` already includes
`%/regimes-collectifs%`, so French is *not* the gap here that [16 §2](16-e2e-production-blueprint.md)
described; and the probe's URL predicate deliberately does **not** apply D8's
`SCOPE_LOGIN_HOST_EXCLUDE`, so the shipped scope is somewhat smaller than the 1,418,435 shown.

> **↺ corrected (2026-07-29 audit):** probe C3 had a null-guard bug — the URL coalesce lacked the
> trailing `F.lit("")` fallback and `like_any` lacked its NULL guard, so rows with NULL `page_url`
> AND NULL `post_page_url` (the mobile-app-hit shape) evaluated `in_url_scope = NULL` and fell out
> of the `segment_only` bucket. **The +1,436 GAIN figure is an undercount (suspect).** The −60,594
> and in-both figures are unaffected, so the ~96% overlap may shift slightly downward. The code is
> fixed; the figures above stand pending a probe re-run on Databricks.

**So the case for segment scope changes shape.** It is *not* "segment scope is bigger and fixes
French." It is: on the Public Website the two are near-equivalent, so the **re-baseline cost there is
low** — and the real reason to adopt the segment model is that **the other three channels cannot be
expressed by URL at all**. Adopting it is a decision about the other three channels, priced on the
first.

> **↺ 2026-07-29 — that argument is now void, and this is the most consequential single change the
> ruling makes.** The segment model's justification was *entirely* the other three channels; the
> public-website numbers were only ever the price tag. With those channels deferred, there is no
> remaining case built on coverage — segment-vs-URL is now a bare trade: **gain 1,436 rows
> (undercounted, pending the C3 re-run), lose 60,594**, for a full `mode=backfill` with gold
> truncated. On those numbers the honest recommendation is **stay on URL scope** unless the SME
> wants the eVar105 brand tag as the *definition* of Canada Retirement for governance reasons
> rather than coverage ones — which is exactly what [20](20-gwam-sme-questions.md) Q3 asks. Note
> the two facts that used to be secondary and are now the whole reason to consider switching: the
> brand tag is the SME's own definition, and it is stable against URL restructuring.
>
> One thing the ruling does **not** change: the re-run is still worth doing. The null-guard bug it
> fixes affected NULL-URL rows, which are app-shaped — so on a web-only suite the correction should
> be small. "Should be" is the reason to measure rather than assume, and the re-run now rides along
> with the new C11/C12 sections anyway.

Two further findings constrain how far the segment model generalises:

- **`manugrs` has eVar105 populated at 0.0001%** — effectively unpopulated (a handful of rows out of
  23.5M in the window). Web Member scope cannot come from eVar105; it must come from eVar185 (C4).
  The segment field is *per-suite*, not a universal key.
- **`manufingbrsmobileapp.prod` is 93.6% populated on eVar105, but its dominant value is
  `GWAM:US Retirement:Mobile` (92.5M) — US, not Canada.** eVar105 on the mobile suite selects the
  wrong country; the Canada-Retirement subset there has to come from pagename (C8).

Two hard constraints on acting on any of it:

- **Any scope change is a re-baseline event.** Stated identically at [settings.py:25-31](../../databricks/conf/settings.py)
  and [:88-90](../../databricks/conf/settings.py): flipping scope re-baselines every downstream KPI,
  detector threshold, and injected-anomaly calibration; done under `mode=incremental` it also writes a
  step change mid-series that the detector reads as a level-shift anomaly. Any flip must be a full
  `mode=backfill` with gold truncated. C3 has now **sized** it: on the Public Website the delta is
  ~4%, which makes this a cheap cutover rather than the leap it looked like pre-probe.
- **A URL-based scope cannot express the Mobile channel at all — now proven.** Every scope predicate
  in the repo is a SQL `LIKE` on a URL column ([01_bronze_ingest.py:62-101](../../databricks/src/01_bronze_ingest.py));
  a hit with no page URL cannot enter scope. **C2 measured `manufingbrsmobileapp.prod`'s coalesced URL
  rate at exactly 0.000** (`verdict=app_or_mixed`). Note also that **`mobileappid` is 0.000 populated
  on every suite**, so the obvious app discriminator is unusable — pagename is the only handle (C8).

### 2.3 ⚠️ The D8 conflict — ↺ **DISSOLVED 2026-07-29** (was: the single biggest blocker)

> **↺ 2026-07-29.** This section described the programme's largest open question. It is no longer
> open, and it is important to be precise about *why*: **nobody ruled on D8.** The single-channel
> ruling removed both channels that needed login traffic — ManulifeID *is* the sign-in system and Web
> Member *is* the authenticated portal — so the two business rules stopped pointing in opposite
> directions. D8 stands exactly as written, `SCOPE_LOGIN_HOST_EXCLUDE` keeps subtracting its six
> hosts, and **the conflict returns verbatim the moment scope re-widens to a signed-in channel.**
> Read the rest of this section as the analysis to reach for if that happens, not as a live blocker.
> The ~94% figure below is why it would still be worth an escalation then.

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

**What the probe changes about this conflict — it narrows, but does not shrink.** With
`manucustomer.prod` absent from our feed (C9), the ManulifeID half of the collision is **moot until
access is granted**: we could not build those four metrics today even if D8 were reversed tomorrow.
The *live* half is therefore **Web Member**, and C1/C4 have now put a number on it —
`manugrs` is 322M rows, and the D8 hosts include `grsmembers.manulife.com`, which C8 shows is exactly
where its retirement-planner pages live (`.../passport/Jsp/RetirementGoals/*.jsp`, ~330k hits across
ten pages in the window).

C8 also shows the ManulifeID sign-in flow is **partially observable inside `manugrs`** without any new
access: `mfid:sign-in` (1,160,058 hits) and `grs:id-flow:member:account-selection` (2,312,927). That is
a genuine option for the sign-in metrics — and it is *precisely* the traffic D8 excludes, so it is
gated on the same ruling. It does not create a workaround; it raises the cost of answering "no."

### 2.4 Metrics

> **↺ 2026-07-29 — only the first three rows are still in scope.** Errors and both sign-in metrics
> left scope with their channels, so the analysis below them is preserved as evidence rather than as
> work. Concretely: **Q7 and Q8 are withdrawn, not answered**, along with the follow-up query and the
> `event154/155/156` lead. **G3 goes moot** (Errors was already `0` for the public website in the SME
> matrix). What survives and *hardens* is the Page Views row — see §1.1: two new anomaly signals
> divide by that numerator, so Q6 is now blocking.

| Metric | Exists? | Detail |
|---|---|---|
| Page Views | 🟡 Approximately | We compute `hits_total` = row count ([gold_lib.py:149-154](../../databricks/src/gold_lib.py)) and surface it as page views. Adobe's *page views* are narrower than *hits*. Which the SME means changes the number ([20](20-gwam-sme-questions.md) Q6). ↺ **2026-07-29: this ambiguity is now load-bearing** — the pv-per-visit signals are ratios over it. ↺ **2026-07-30: C12 priced it** — 2.885 hits/visit (derived) vs 1.343 Adobe page views/visit on this scope. |
| Visits | ✅ | `visits_total` = distinct `concat(post_visid_high, post_visid_low, visit_num)`. |
| Visitors | ✅ | `visitors_total` = distinct `mcvisid`. ↺ **2026-07-29: the SME named "unique ECID" as a signal, which is exactly this** — `mcvisid` is the ECID. But the EDA notebooks count the `post_visid` pair instead, so the two layers can disagree; ↺ **2026-07-30 C12 measured the gap and it is negligible** — 14 of 88 days differ, by at most 15 visitors (0.068%), ECID never above the pair (§1.1). |
| Errors | 🟢 **Buildable** (C5) | ↺ was "🔴 Nothing". The error eVars are **populated at scale on the channels that need them**: on `manugrs` eVar181 **52.0%** (12.2M rows) / eVar182 **69.9%** (16.4M) / eVar184 **61.3%** (14.4M); on mobile eVar184 **17.9%** (**37.6M rows** — the largest error footprint anywhere). ⚠ **eVar183 is effectively absent from the Canada channels** — 0.00% on `manugrs`, 0.14% on mobile — so it is a John Hancock field, not ours. Remaining work is **G3** + Q7 (field of record, and errors-vs-affected-visits). See the attribution caveat below before quoting any example value. |
| Sign in % rate completion | 🔴 **Not buildable from the assumed fields** (C6) | ↺ was "🟡 engine ready, inputs missing" — the inputs are now known to be *absent*. **eVar122 and eVar135 are 0% populated on `manugrs` and on mobile.** eVar122's entire footprint is John Hancock (2.77% of `jhfswamjhreupeprod`), where its values **duplicate eVar182 exactly** — so eVar122 carries error descriptions there, not ordered login steps. eVar135 is an auth-method enum (`email` 23.7M / `mfa` 145k / `biometrics` 1k / `username/password` 17), not an attempt/success marker. **The remaining path is pagename-based** (C8): `mfid:sign-in` → `grs:id-flow:member:account-selection` on `manugrs`, or `CIAM Sign In` (9.27M) on mobile. That needs an SME definition (Q8). ↺ **2026-07-30: G2 no longer blocks declaring the ratio** — but this row stays 🔴 because the channel it belongs to is deferred and Q8 is withdrawn. |
| Sign in Error | 🟡 **Likely buildable, not yet proven** (C5/C6) | ↺ was "🔴 Nothing". Sign-in failure strings are present in the window (`Username & Password, Invalid, Attempt` 1,054,850 · `Your password is required.` 800,213 · `Username & Password, Invalid, Locked` 462,428) and eVar181 carries `N/A_CAS_INVALID_CREDENTIALS` / `N/A_CAS_USER_LOCKED`. **But none of these can be attributed to a Canada channel from this probe** — see the caveat below. Same G3 + Q7 dependency as Errors, plus one confirming query. |

⚠ **Attribution caveat — C5's value lists are cross-suite, its population rates are not.** `top_values`
is computed over the whole window across all 16 rsids; only `per_rsid_population` is per-suite. So a
value's *presence* in the list is not evidence it occurs on a GWAM Canada channel. Capacity
arithmetic (value count vs a suite's populated-row count) resolves only a few cases:

- **Forced onto `manugrs`:** eVar181 `"N/A"` at **8,750,383** exceeds every other suite's eVar181
  capacity combined (223,328). Worth noting on its own terms — **~72% of `manugrs`'s populated
  eVar181 is the literal string `"N/A"`**, so Error *Code* is mostly a non-value there. That is an
  argument for eVar182 (Description) as the field of record in Q7.
- **Forced OFF the mobile suite:** `ServerFetchFailure_BottomSheet_RetirementPlannerDataFailedtoLoad`
  (2,102,470) cannot be a Canada mobile error — that suite has only **295,898** populated eVar183
  rows in total. It belongs to `jhfswamjhreupeprod` (6,965,243 capacity). *An earlier draft of this
  document attributed it to the app; that was wrong.*
- **Unresolvable:** the sign-in failure strings above fit inside either `manugrs` (16.4M) or
  `jhfswamjhreupeprod` (2.86M). Note they are **exactly** eVar122's top values with identical counts,
  and eVar122 is John-Hancock-only (2,860,959 of its 2,862,198 populated rows) — so the parsimonious
  reading is that they are *John Hancock's*, not ours.

**Consequence: a small follow-up query is needed before Q7 can be answered** — per-rsid top values for
eVar181/182/184 on `manugrs` and `manufingbrsmobileapp.prod`. The probe emitted global value lists by
design (to bound payload size); this is the one place that bound costs us an answer. Worth folding
into the next revision of `gwam_channel_discovery.py`.

**↺ Corrected — `event173` is not the GWAM error event.** [16 §3](16-e2e-production-blueprint.md)
lists it as the error event; C5 shows it fires on **John Hancock / investments** suites (99.95% of
`jhfsjhinvestments`, 72.4% of `jhfsmanulifeinvestmentmgt2.0prod`) and effectively **zero on
`manugrs` (0.000001)** and zero on mobile. Errors on the GWAM Canada channels are an **eVar** signal,
not an event signal.

Compounding all of it: **GWAM's 23 tracked event ids are unresolved** ([registry.py:18-22](../../detect/registry.py)),
and the registry's own `meta.report_suite_caveat` ([metric-registry.yaml:57-61](metric-registry.yaml))
warns that CoverMe's ids do not transfer to GWAM. **C7 mapped the id spaces but could not label
them** — this feed carries no event dictionary:

| Suite | Top ids (window) |
|---|---|
| `manugrs` | `152` 23.5M · `151` 20.4M · `107` 19.8M · `500`/`501`/`502`/`503` 11.5M each · `132` 7.4M · `121` 5.2M · `122` 4.9M |
| `manufingbrsmobileapp.prod` | `10030`/`108` 210.0M · `10004` 143.4M · `10000` 99.3M · `112` 95.0M · `151` 83.2M |
| `manulifeglobalprod` | `10004`/`10044`/`10005`/`10099` 5.5M · `20` 2.9M · `502` 1.0M |

Ids in `100-199` and `10000-10099` are Adobe **Instance-of-eVar** events (the same formula CoverMe's
`decode_event()` uses); the `500`-series and `20xxx` are custom and need the EDDL workbook or the SME.
`152` firing on 100% of `manugrs` rows suggests a page-view-equivalent — a candidate answer to Q6,
but not one to assert without a label. Note also the tight co-firing blocks: `500`/`501`/`502`/`503`
at *exactly* 11,472,195 each on `manugrs`, and `501`-`504` at exactly 1,033,283 each on
`manulifeglobalprod` — one instrumented action emitting a fixed set.

**A cheap lead for Q8 that the probe could not close.** [16 §3](16-e2e-production-blueprint.md)
line 117 documents **event154/155/156 = Login Start / Complete / Error** — which is precisely the
numerator/denominator pair "Sign in % rate completion" needs. **None of `154`, `155`, `156` (nor the
registration trio `157`-`159`) appears in the top-25 events of any GWAM Canada suite.** The probe
capped at `TOP_N=25` to avoid Databricks' stdout truncation, so this bounds rather than excludes
them: on `manugrs` they fire fewer than ~3.0M times in the window, on mobile fewer than ~16.9M, on
`manulifeglobalprod` fewer than ~951k. **A targeted count of those three ids is a single cheap query
and would settle Q8 outright** — it should run before we accept the pagename-funnel fallback.

### 2.5 "Ideally non-marketing"

Undefined operationally; no marketing discriminator is implemented for GWAM. **C10 bounds the
options, and the answer is largely structural:**

| Suite | `post_campaign` | `ref_type` | `post_referrer` | `post_channel` |
|---|---|---|---|---|
| `manulifeglobalprod` | **57.03%** | 100% | 58.56% | 68.07% |
| `manugrs` | 0.51% | 100% | 10.36% | 56.34% |
| `manufingbrsmobileapp.prod` | 0.02% | 100% | 0.00% | 37.68% |

**"Non-marketing" is only a meaningful filter on the Public Website.** Campaign tagging is
effectively absent from the authenticated member portal and the app — those channels are
non-marketing *by construction*, being post-login experiences. So Q5 reduces to a single decision
about one channel, not four. `ref_type` is 100% populated everywhere but is a coded enum (`8` 312M,
`6` 46.7M, `2` 5.3M) with no dictionary in this feed — usable as a fallback only if the SME can
supply the code meanings.

> **↺ 2026-07-29 — this section predicted its own resolution correctly.** It reduced Q5 to "a single
> decision about one channel"; the ruling then removed the other three channels, and the SME answered
> the decision. See §2.5.1.

#### 2.5.1 ↺ **ANSWERED — marketing = the `CID` query-string parameter** (Abhisekh, 2026-07-29)

Verbatim: *"We are capturing **CID campaign identifier — query string**. Campaign ID. It is the
standard query string parameter appended to marketing URLs."*

That is a clear business answer, and it does **not** immediately give us an implementable rule. Two
gaps sit between the answer and a shipped filter, and it is worth being explicit that neither is a
business question:

**(1) Is `post_campaign` the same thing as `cid=`?** ↺ **ANSWERED 2026-07-30 by probe C11 — and the
answer is no.** Our C10 candidate is Adobe's `post_campaign` column (**57.03%** populated on
`manulifeglobalprod`), which is Adobe's *tracking code* — normally populated *from* a query parameter
like `cid`, but that mapping is a report-suite configuration we have not seen. C11 tested it
directly, built to expect asymmetry rather than equality: Adobe **persists** a campaign value across
the whole visit, while `cid=` appears only on the landing hit, so `campaign_only >> cid_only` is the
healthy result. The two figures to read were `cid_only` ≈ 0 and a high `agreement_when_both`:

| scope | rows | `cid_rows` | `cid_only` | `campaign_only` | `agreement_when_both` |
|---|---|---|---|---|---|
| suite (`manulifeglobalprod`) | 6,264,094 | 2,806,754 | 17,275 (0.62%) | 777,524 | **0.7621** |
| segment scope (ca-retirement) | 1,298,417 | 186,188 | 7,739 (4.16%) | 179,411 | **0.5365** |

**One test passes and the other fails.** `cid_only` is small, so the rule's *direction* is confirmed —
there is almost no CID traffic the column misses entirely, and `campaign_only >> cid_only` is exactly
the persistence asymmetry we predicted. But agreement is only **76%** suite-wide and **54%** on the
segment scope we would actually ship: when both are present they disagree about half the time in the
scope that matters. `post_campaign` is therefore **not** a usable substitute for the SME's rule, and
must not be silently swapped in for it. Note the segment scope is the *worse* of the two — the
opposite of what a "narrower scope is cleaner" intuition would suggest.

Counts only were emitted; no URLs or query-string values leave the probe, and the ADR-0007 privacy
grep over this section is clean (§7).

**(2) We strip query strings by policy.** This is the harder one. The pipeline projects
`post_page_url` and the EDA notebooks strip `?`-onward explicitly, on the stated grounds that session
tokens live in query strings ([15 §](15-consolidated-eda-report.md), and
[ADR-0007 §](adr/adr-0007-identity-privacy-layer.md)). The SME's rule lives in exactly the substring
our privacy posture discards. So implementing it means **either** extracting `cid` at ingest and
keeping only that (not the raw query string), **or** relying on `post_campaign` as the proxy if C11
vindicates it. ↺ **2026-07-30: C11 did not vindicate it, so the second option is gone.** The
zero-cost path is closed and the only remaining route is extracting `cid` at ingest — which needs an
**ADR-0007 amendment**. That decision is tracked in [16](16-e2e-production-blueprint.md)'s backlog and
is now the *sole* blocker on marketing exclusion.

Until one of those lands, the three public-website metrics count **all** traffic, marketing included —
which is a documented deviation from the SME's "ideally non-marketing" qualifier, not an oversight.
Worth noting the qualifier's own hedge: "ideally" suggests he would accept unfiltered counts as a
starting point, which is what we will ship first.

### 2.6 "Canada Retirement App Pages v2" — reconstructible from pagename (C8)

The Mobile channel's segment is an Adobe segment *name*, which we cannot implement. C8 profiled
`post_pagename` on the mobile suite and found it partitions cleanly by **product-line prefix**:

| Prefix | Meaning | Examples (window) |
|---|---|---|
| `GB ` | **Group Benefits** — not retirement | GB Home 10.2M · GB Recent Claims 4.9M · GB Coverage 4.9M · GB Submit A Claim 3.4M |
| **`MPS `** | **the retirement platform** — matches eVar185 `MPS Member` | MPS Account Balances 8.1M · MPS Transac/Contrib History 1.5M |
| `MM ` | Manulife Mobile shell | MM Select Account 5.1M |
| `CIAM Sign In` | the sign-in flow | 9.3M |

So the candidate predicate is **`rsid = manufingbrsmobileapp.prod` AND `post_pagename LIKE 'MPS %'`**,
optionally intersected with `post_evar185 = 'MPS Member'` (18.16% of the suite). That needs
confirmation against the real Adobe segment definition (Q11) — a prefix reconstruction can miss pages
the segment includes.

⚠ **Do not misread the probe's `retirement_like` output here.** It returned a single row for this
suite (`GR Retirement Redefined`, 741 hits) because it searched for the substring *retire* — and
**the Canada retirement pages are named `MPS`, not "Retirement."** The near-empty result is an
artifact of the search term, not evidence of absent retirement traffic.

---

## 3. Engineering gates (ours to close — no SME needed)

Numbered **G1–G6**. Deliberately *not* the `E1–E4` series: that namespace belongs to CoverMe in
[17 §3](17-coverme-eda-readiness.md) and reusing it would collide across products.

> **↺ 2026-07-29 — the gates re-rank under the single-channel ruling.** The ordering below was written
> when four channels were in scope; that inverted two of the priorities.
>
> | Gate | Was | Now |
> |---|---|---|
> | **G2** SeriesSpec ratio | nice-to-have; only sign-in completion needed it | ✅ **CLOSED 2026-07-30** — was 🚩 CRITICAL (both new anomaly signals are per-visit ratios and could not be *declared* without it). Ported; the signals are now blocked on Q6 alone. |
> | **G3** error columns | actionable, cheapest of three | **MOOT** — Errors was already `0` for the public website in the SME matrix, and the channels that needed it are deferred. Do not widen bronze for eVar181/182/184. |
> | **G4** channel dimension | blocked on the D8 ruling | **MOOT** — one channel needs no channel dimension. This also means the re-baseline risk it carried disappears. |
> | **G1, G5, G6** | — | unchanged. G6 (scope tests) matters *more*, since scope is what the SME just changed. |

| # | Gap | Evidence | Fix | Impact if unfixed |
|---|---|---|---|---|
| **G1** | ✅ **CLOSED 2026-07-29; extended run landed 2026-07-30.** The discovery probe has been run twice. | [`eda/gwam_channel_discovery.py`](../../eda/gwam_channel_discovery.py) executed on Databricks; export [`gwam_channel_discovery.html`](../../gwam_channel_discovery.html). Current run: `generated_at` **2026-07-30T08:28:42**, **12 sections** (13 SHAREABLE blocks), **`skipped == {}`**, `complete: true`, and **12/12 payloads match the manifest bytes+sha1** per [`scripts/decode_databricks_export.py`](../../scripts/decode_databricks_export.py). Supersedes the 2026-07-29T02:00:54 / 11-section run. | Done — the C1–C10 results are folded into §0, §2.1-2.6 and §4; the C3 re-run, C11 and C12 into §1.1, §2.5.1 and §7. Three pre-probe claims were **corrected**, not just filled in (suite count, delimiter, segment-vs-URL direction). | — |
| **G2** | ✅ **CLOSED 2026-07-30.** GWAM's `SeriesSpec` could not express a ratio or carry governance; the Spark engine supported `ratio` but the GWAM Python spec did not. | Was [registry.py:68-83](../../detect/registry.py) — `kind` was `count \| rate \| share`, with no `numerator`/`denominator` and no `status`/`direction`/`owner`, while `CmSeriesSpec` ([cm_registry.py:118-145](../../detect/cm_registry.py)) had all of them. | Done. The five fields are ported from `CmSeriesSpec`; [kpis.py](../../detect/kpis.py) gained the two-pass ratio arm it was missing (its `else` raised `ValueError`, so a declared ratio would have built in Spark and crashed in pandas); [test_gold_parity.py](../../tests/test_gold_parity.py) pins pandas/Spark agreement including a zero-denominator day. `gold_lib` needed **no** change, as predicted — [gold_lib.py:168-175](../../databricks/src/gold_lib.py) already resolved ratios by sibling `metric_id`. | — (was: "Sign in % rate completion" undeclarable, and GWAM metrics ungoverned). |
| **G3** | **No error or sign-in columns reach bronze/silver.** Now **actionable** — C5/C6 determined which columns are worth carrying. | [bronze_columns.py](../../databricks/conf/bronze_columns.py) — `DETECTOR_COLUMNS` / `SILVER_COLUMNS` carry no eVar181-184, eVar122, eVar135. | Add **`post_evar181`, `post_evar182`, `post_evar184`** — the three populated at scale on the Canada channels (12.2M / 16.4M / 14.4M rows on `manugrs`; 37.6M on mobile for eVar184). **Do NOT add `post_evar183`** (0.00% on `manugrs`, 0.14% on mobile — a John Hancock field) **or eVar122/eVar135** (0% on both Canada suites). Carrying any of those three costs bronze width for near-guaranteed nulls. | Errors and Sign-in Errors are unbuildable regardless of any SME ruling. |
| **G4** | **Scope has no channel dimension.** `SCOPE_RSID` is a single string and the predicate is one rsid AND a URL match. | [settings.py:19](../../databricks/conf/settings.py), [01_bronze_ingest.py:62-101](../../databricks/src/01_bronze_ingest.py). | Per-channel scope config (rsid + its own segment predicate), and a `channel` column carried to gold so metrics break down by it. **Blocked on the D8 ruling** — do not build until §4 item 1 lands. | A four-channel product cannot be expressed. Note this is also the change that re-baselines everything (§2.2). |
| **G5** | **GWAM has no registry pin or drift test.** CoverMe has both. | [test_registry_yaml.py](../../tests/test_registry_yaml.py) covers only the three CoverMe sheets; `detect/registry.py` has no `REGISTRY_VERSION`. | Pin GWAM's binding to the YAML and add a drift guard, mirroring `test_series_governance_matches_yaml`. Seeded 2026-07-28 by `test_gwam_channel_seed_counts`. | GWAM metric definitions can drift from the governed registry silently — the exact failure the CoverMe test was written to prevent. |
| **G6** | **No test covers the scope constants.** | Nothing asserts `SCOPE_RSID`, `SCOPE_URL_MODE`, `SCOPE_SUITE_MODE`, or `SCOPE_LOGIN_HOST_EXCLUDE`. CoverMe's equivalent predicate *is* tested ([test_cm_silver.py](../../tests/test_cm_silver.py) `test_scope_expr_include_minus_exclude`). | Add scope-predicate unit tests before touching scope. | The single highest-consequence config in the repo (§2.2 re-baseline) is unguarded — precisely the wrong thing to change untested. |

---

## 4. What we need from Business / SME (the clarity gaps)

Ranked by how hard each blocks the build. The send-ready version is [20 — GWAM SME Questions](20-gwam-sme-questions.md);
this table is the technical agenda behind it.

> **↺ 2026-07-29 — most of this agenda cleared in one message.** Of the twelve items, **seven are
> withdrawn** because the channels they concerned left scope (items 1, 2a, 7, 8, 9, 10, 11), **one is
> answered** (item 5 — marketing = CID, §2.5.1), **one is partly answered and spawns a new question**
> (item 3's brand-tag half → **new item 13 / Q3b**), and **one is escalated** (item 6, page views —
> now blocking, §1.1). Items 2b, 4 and 12 shrink to the public website. What remains genuinely open is
> a short list: **Q3b, Q3 sign-off, Q6, Q12**. ↺ **2026-07-30: Q3b is answered (both brand variants
> ruled OUT), leaving Q3 sign-off, Q6, Q12.**
>
> | # | New status | Why |
> |---|---|---|
> | 1 (D8) | ⬜ **Withdrawn** | Both signed-in channels deferred — the conflict dissolved without a ruling (§2.3) |
> | 2a (`manucustomer.prod` access) | ⬜ **Withdrawn** | ManulifeID out of scope; longest-lead item retires |
> | 2b (ratify rsids) | 🟡 Shrunk | Only `manulifeglobalprod` matters now, and it was never in doubt |
> | 3 (segment scope) | 🚩 **Open, re-priced** | Now a bare +1,436 / −60,594 trade; the coverage argument is gone (§2.2) |
> | **13 / Q3b** (NEW) | ✅ **CLOSED 2026-07-30** — was 🚩 Open | Are `wealth-ca` / `pvt-wealth` inside Canada Retirement? **Both ruled OUT** (Abhisekh, 2026-07-30). Probe C3 had sized them (+250,355 / +9,690, zero overlap); the ruling **confirms the HELD parts-match predicate**, so nothing re-baselines and the +19.3% swing is retired |
> | 4 (1/0 reading) | 🟡 Shrunk | Moot for the deferred metrics; no multi-channel roll-up question left |
> | 5 (marketing) | ✅ **Answered** | CID query parameter — §2.5.1. What remains is mechanical (C11 + the ADR-0007 question) |
> | 6 (page views) | 🚩 **Escalated** | Was a labelling question; now the denominator of two new signals (§1.1) |
> | 7, 8 (errors, sign-in ratio) | ⬜ **Withdrawn** | Those metrics left scope with their channels |
> | 9, 10, 11 (ManulifeID split, instance, App Pages v2) | ⬜ **Withdrawn** | Mobile and ManulifeID deferred |
> | 12 (thresholds, owners) | 🟡 Shrunk | Five metrics on one channel instead of 17 on four |

| # | What we need clarified | Why it blocks | Our current assumption | Resolving artifact |
|---|---|---|---|---|
| **1** | 🚩 **Does the four-channel scope supersede the 2026-07-20 login-exclusion rule (D8)?** Two channels require the traffic D8 removes. | Scope is the denominator of every metric. D8 is enforced in *every* mode and moves ~94% of one suite. | D8 stands until explicitly reversed. **We have not changed it.** | Written ruling from the authority that issued D8 → doc 20 Q1 |
| **2a** | 🚩 **Access to `manucustomer.prod`.** ↺ Escalated from "what is the ID" to "we do not have this data." | The ManulifeID channel — 4 of the 17 metric×channel pairs — cannot be built at all. | **C9: 0 rows.** Absent from all 16 rsids. Not a naming problem. | A data-access request, started **now** — longest lead time on the list → doc 20 Q2 |
| **2b** | **Ratify `manugrs` = GRS+ and `manufingbrsmobileapp.prod` = GBRS Mobile.** ↺ Downgraded from blocking. | Only the rsid *strings* are unconfirmed; the identifications are data-backed. | **C1/C4: `manugrs` carries the SME's own `MPS Member` predicate at 100% of its populated eVar185.** Mobile is 68.9% of the table. | One-line confirmation → doc 20 Q2 |
| **3** | 🚩 **Does segment-scope replace our URL filter?** ↺ The *value* question is answered; only the decision remains. | Switching the scope model re-baselines every KPI and threshold; it must happen before baselines are fit, not after. | **C3: `ca-retirement :  : GWAM` is a literal value (776,860 hits); delimiter is `":"` not `" : "`.** Sizing on the Public Website: **+1,436 / −60,594 rows, ~96% overlap** — a *cheap* cutover, and one justified by the other three channels rather than by this one. | Sign-off on the re-baseline → doc 20 Q3 |
| **4** | **Does `1`/`0` mean in/out of scope?** And do the four channels alert independently? | Determines whether the deliverable is 17 metrics or a ranked subset. | 1 = in scope; channels alert independently. | One-line confirmation → doc 20 Q4 |
| **5** | **What defines "marketing" traffic?** ↺ Narrowed to one channel. | Three of six metrics carry the "ideally non-marketing" qualifier. | **C10: campaign tagging is 57% on the Public Website but ~0% on Web Member (0.51%) and Mobile (0.02%)** — those two are non-marketing by construction. The question only bites on one channel. | A rule for the Public Website → doc 20 Q5 |
| **6** | **"Page Views" — Adobe page views, or all hits?** | We currently report hits. The two differ materially. | Adobe page views. C7 notes event `152` fires on ~100% of `manugrs` rows and may be the page-view event, but the id is unlabeled. | Confirmation → doc 20 Q6 |
| **7** | **Which field is the error of record, and is "count" errors or affected visits?** ↺ Now a menu, not an open question. | An error metric is buildable; we would still be guessing at field and grain. | **C5: eVar181 52% / eVar182 70% / eVar184 61% populated on `manugrs`, eVar184 17.9% on mobile. eVar183 and `event173` are BOTH ruled out** (John Hancock fields). Leaning eVar182 — eVar181 is ~72% the literal string `"N/A"`. Needs one follow-up query for per-suite values. | Field + grain → doc 20 Q7 |
| **8** | **Numerator and denominator of "Sign in % rate completion", and over what unit.** ↺ The assumed inputs are gone. | A ratio without a defined denominator is not a metric. | **C6: eVar122 and eVar135 are 0% populated on the Canada suites.** Remaining path is the C8 pagename funnel (`mfid:sign-in` → `account-selection`, or `CIAM Sign In`). | Definition, chosen from the pagename options → doc 20 Q8 |
| **9** | **How to separate Retirement from other ManulifeID sign-ins** — the SME's own open item. ↺ Answered, negatively. | Without a discriminator the ManulifeID channel cannot be scoped to Canada Retirement. | **C9: the suite has 0 rows in our feed — no field could be profiled.** This is not solvable downstream; it needs access first, then likely a tagging change. | Access, then a field + value → doc 20 Q9 |
| **10** | **Is the "Manulife Financial" Adobe instance the same feed?** | If Mobile is a separate feed we lack access to, this is data acquisition, not modelling. | **C1: `manufingbrsmobileapp.prod` is present in our feed with 881 days of history** — strongly suggests same feed, but the instance label is an Adobe-admin fact no query can settle. | Confirmation from Adobe admin → doc 20 Q10 |
| **11** | **The definition of the "Canada Retirement App Pages v2" segment.** ↺ Reconstructed; needs confirming. | It is an Adobe segment name; we cannot implement a name. | **C8: `post_pagename LIKE 'MPS %'`** isolates the retirement platform from `GB ` (Group Benefits) and `MM `. | Confirm the reconstruction, or export the real definition → doc 20 Q11 |
| **12** | Alert thresholds / severity per channel, and a named owner per metric. | Not blocking — detection can be built with defaults and tuned. | Reuse the existing severity ladder (`warn 3.5 / minor 5.0 / major 8.0 / critical 12.0`). | Non-blocking attachment → doc 20 Q12 |

---

## 5. Gap to the full build (roadmap, post-SME)

> **↺ 2026-07-29 — re-sequenced.** The roadmap below is written around four channels and a blocking
> ruling; neither applies. The current sequence is short:
>
> 1. **Run the extended probe** (C3 re-run + brand-variant sizing + C11 + C12). One Databricks run,
>    13 sections. This is the only thing gating the two new metrics' thresholds.
> 2. **G2** — port `numerator`/`denominator` + governance fields from `CmSeriesSpec` to GWAM's
>    `SeriesSpec`. Now the critical path, not a tidy-up.
> 3. **G6** — scope-predicate tests. Do this *before* touching scope, which is exactly what item 3
>    would do.
> 4. **Decide item 3** (URL vs segment scope) on the re-run numbers, then a single
>    `mode=backfill` if it flips.
> 5. **Marketing exclusion** — only after C11, and only via the ADR-0007 decision (§2.5.1).
>
> Dropped from the sequence entirely: G3, G4, per-channel bronze scope, mobile-app ingestion, the
> access request. Step D below is no longer "nothing here is startable today" — steps 1–3 are all
> startable now.

> **↺ 2026-07-30 — steps 1, 2 and 5 have resolved.** Against the sequence above:
>
> 1. ✅ **Done.** The probe ran 2026-07-30T08:28:42 — 13 blocks / 12 sections, verified. It did gate
>    the thresholds, and it also **falsified** two assumptions (§7 item 6).
> 2. ✅ **Done.** G2 is closed — the fields are ported, plus the `kpis.py` ratio arm that the original
>    scoping missed and parity coverage for it.
> 3. ⬜ **G6 unchanged** and now the *only* engineering item left. Still do it before touching scope.
> 4. ⬜ **Item 3 (URL vs segment) has its numbers**: segment scope *loses* 64,079 rows and gains 1,352
>    — a net ~4.5% loss. That is the decision input; the decision itself is still Q3.
> 5. 🔴 **Changed shape.** "Only after C11" is satisfied, but C11 **rejected** `post_campaign`, so the
>    ADR-0007 amendment is no longer one of two options — it is the only one.
>
> Net: the sequence is now **G6, then two SME answers (Q3b, Q6), then the ADR-0007 decision.** Nothing
> engineering-side is blocked on anyone. ↺ **2026-07-30: Q3b is answered, so the sequence is G6, then
> Q6, then the ADR-0007 decision.**

**A. Close what data can close — ✅ done 2026-07-29.** G1 ran clean; §0 and §2.1-2.6 now carry results
rather than ⏳ markers, and three pre-probe claims were corrected. What remains in §4 is there because
**no query can settle it**, not because we haven't looked.

**B. Get the rulings, and start the access request.** Items 1, 2a, 3 in §4. Until item 1 lands,
**G4 must not be built** — a channel-aware scope that assumes the wrong answer on D8 is worse than no
scope change, because it re-baselines everything in the wrong direction. Item **2a is now the longest
pole**: `manucustomer.prod` access is procurement-shaped, not engineering-shaped, and nothing about
the ManulifeID channel can start until it lands.

**C. Engineering, in dependency order.** ↺ **G2 (SeriesSpec ratio + governance) is DONE, 2026-07-30.**
G6 (scope tests) remains safe to do immediately — it depends on no ruling. **G3 is now also unblocked**: C5/C6 determined
exactly which columns to carry (eVar181/182/184 in; **eVar183 OUT** — John Hancock field per the G3
gate above and §2.4; eVar122/135 out — ↺ corrected 2026-07-29, this line previously listed eVar183
as in), and it is the cheapest of the three. G4 follows item 1. G5 follows the GWAM registry entries being promoted past `candidate`.

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
| Discovery probe | ✅ **Run clean 2026-07-29** (G1 closed) — 11 sections, `skipped == {}` |
| Report suites identified | 🟢 **3 of 4 located** — `manulifeglobalprod`, `manugrs` (data-confirmed), `manufingbrsmobileapp.prod`; `manucustomer.prod` **absent from our feed** |
| Scope model (URL → segment) | 🟡 **Sized, awaiting sign-off** — SME item 3; ~96% overlap on the Public Website (+1,436 / −60,594; the +1,436 is an undercount — see the §2.1 C3 correction, re-run pending), so a cheap cutover; still a full re-baseline |
| D8 / login-traffic conflict | 🔴 **Blocked on SME** — item 1, still the biggest single question; now narrowed to the **Web Member** channel |
| Page Views / Visits / Visitors | ✅ **Engine ready** — exist today; only the page-views definition is open (item 6) |
| Errors | 🟢 **Buildable** — eVar181/182/184 at 12.2M/16.4M/14.4M rows on `manugrs`, eVar184 at 37.6M on mobile; needs G3 + field-of-record ruling (item 7) + one per-suite value query |
| Sign-in errors | 🟡 **Likely buildable** — the failure strings exist but cannot be attributed to a Canada channel from this probe; same G3 + item 7 |
| Sign-in completion rate | 🔴 **Assumed inputs are empty** — eVar122/135 0% on the Canada suites (C6); pagename funnel is the remaining path, needs item 8 + G2 |
| Mobile-app ingestion | 🔴 **Net-new, and proven necessary** — mobile URL rate is exactly 0.000; `mobileappid` unusable; pagename `MPS %` is the handle (C2/C8, G4) |
| ManulifeID channel | 🔴 **Blocked on data access** — suite has 0 rows in our feed (item 2a); the retirement split (item 9) cannot even be attempted until then |
| Governance (GWAM registry pin + drift test) | 🟡 **Seeded** — 17 candidate entries at v0.4.0; binding pin still open (G5) |

**Bottom line.** ↺ The pre-probe verdict said "GWAM's blockers are the data itself." **That was too
pessimistic by three-quarters.** Three of the four suites are in our feed with 880+ days of history on
the two large ones; Errors and Sign-in Errors turned out to be *buildable* rather than absent; and the
scope cutover is ~4% on the Public Website rather than the leap it looked like. What survives is
sharper and smaller: **one business ruling (D8), one access request (`manucustomer.prod`), and one
definition we cannot infer (sign-in completion).** The critical path is no longer discovery — it is
those three, plus G2/G3/G6, which are all now unblocked and independent of any ruling.

> ### ↺ Revised bottom line (2026-07-29, post-ruling)
>
> **All three of those named blockers are gone** — and none of them was solved. The D8 ruling was
> never made, the access request retires unfiled, and sign-in completion left scope. That is worth
> stating plainly rather than reading as progress: **scope narrowing removed the questions instead of
> answering them**, and each one returns intact if the scope re-widens.
>
> What the programme actually looks like now:
>
> | Area | Status |
> |---|---|
> | Scope definition | ✅ **Settled** — `manulifeglobalprod` + eVar105 parts-match (`ca-retirement` + `gwam`), public website only |
> | Page Views / Visits / Visitors | ✅ **Engine ready today** — the three metrics exist and run |
> | Marketing exclusion | 🟡 **Defined, not implementable** — CID rule known; needs C11 + an ADR-0007 decision (§2.5.1) |
> | Brand-variant scope (Q3b) | 🚩 **Open** — `wealth-ca` / `pvt-wealth` unclassified; predicate held meanwhile |
> | Page-view numerator (Q6) | 🚩 **Open and now blocking** — two new signals divide by it |
> | New anomaly signals | 🔴 **Blocked on G2** — `SeriesSpec` cannot declare a ratio |
> | Baseline history | 🟡 **138 days** — clears the 90-day gate with the least margin of any suite, and it is now the *only* suite |
> | Governance | 🟡 v0.5.0 — 5 candidate / 14 deferred; pin still open (G5) |
>
> **The honest summary: this product is closer to shippable than it has ever been.** Three of its five
> metrics work today against a settled scope. The remaining work is one engineering gate (G2), one
> probe run, and two SME answers — none of which is procurement-shaped or cross-team. The thing to
> watch is no longer coverage but **baseline thinness**: 138 days on the only suite in scope, and any
> scope flip consumes that margin.

> ### ↺ Revised bottom line (2026-07-30, post-extended-probe)
>
> The probe run and the engineering gate are both done. **Everything still open is an SME answer** —
> there is no engineering work left on the critical path, which is a first for this programme.
>
> | Area | Status |
> |---|---|
> | Scope definition | ✅ **Settled** — unchanged |
> | Page Views / Visits / Visitors | ✅ **Engine ready today** — unchanged |
> | Marketing exclusion | 🔴 **Harder than it looked** — was 🟡. C11 **rejected** `post_campaign` as the proxy (agreement 0.762 suite / 0.537 segment), so the zero-cost path is gone and an **ADR-0007 amendment is the only route** (§2.5.1) |
> | Brand-variant scope (Q3b) | ✅ **ANSWERED 2026-07-30** — was 🚩. The sizing (`wealth-ca` +250,355 / **+19.3%**, `pvt-wealth` +9,690, **zero overlap**) went to the SME, who ruled **both OUT of Canada Retirement**. That **confirms the predicate we held**, so nothing re-baselines; the cost is that ~250k records/90 days are now unwatched by explicit decision (§7 item 6) |
> | Page-view numerator (Q6) | 🚩 **Open, now priced** — the two bases give 2.885 vs 1.343 on this scope; the "consistently 2" signal is meaningful under one and not the other |
> | New anomaly signals | 🟡 **Blocked on Q6 only** — was 🔴 blocked on G2. ⚠ C12 found the SME's literal "< 1" test **never fires** (88-day floor 1.2236); `share_pv_eq_0` = 3.25% is the detectable quantity (§1.1) |
> | Baseline history | 🟡 **138 days** — unchanged |
> | Governance | 🟡 v0.6.0 — 5 candidate / 14 deferred; pin still open (G5) |
>
> **G2 is closed**, so the "one engineering gate, one probe run, two SME answers" of 2026-07-29 is now
> **two SME answers** (Q3b, Q6) plus one architectural decision (the ADR-0007 amendment) — ↺ **and
> Q3b came back the same day, leaving exactly one SME answer (Q6) and the ADR-0007 decision**. The probe
> also did something better than fill blanks: it **falsified two working assumptions** — that
> `post_campaign` could stand in for CID, and that the SME's "< 1" test would fire at all. Both would
> have shipped as silent no-ops. Baseline thinness remains the thing to watch.

---

## 7. Verification / how to confirm this is done

1. ✅ **Probe ran clean.** `run_manifest.skipped == {}`, `complete: true`, all 11 sections present and
   parsing as JSON. A non-empty `skipped` map would have meant the run is *not* complete coverage,
   regardless of how much output appeared.
2. ✅ **Every ⏳ in §2 and §4 is resolved** — flipped to a fact with the emitting section cited, or
   left in §4 with the reason no query could settle it (items 1, 2a, 4, 6, 10, 12).

   **To re-verify any figure in this document from the export** — the notebook lives in one
   base64 line, so never read it with line-oriented shell tools:
   `__DATABRICKS_NOTEBOOK_MODEL = '<b64>'` → base64-decode → URI-decode → `JSON.parse` → walk
   `commands[].results` harvesting string leaves → strip ANSI → match
   `===== BEGIN/END SHAREABLE: {id} =====` (concatenating any `----- part i of n -----` chunks).
3. **D8 is still in force in code.** `git diff databricks/conf/settings.py` is empty; `SCOPE_LOGIN_HOST_EXCLUDE`
   is unchanged. This document flags the conflict; it does not resolve it.
4. **Registry seeded, tests green.** `pytest tests/ -q` passes, including `test_gwam_channel_seed_counts`
   (↺ **19 entries at v0.6.0** — 5 candidate + 14 deferred, partition asserted by
   `test_gwam_status_partition`) and `test_gold_parity` (35 GWAM series, Spark↔pandas; ↺ extended
   2026-07-30 with the G2 ratio cases — no GWAM ratio is *declared*, so the 35-series parity is
   unchanged).
5. **Doc 20 reads as an email.** No repo jargon, no unexplained eVar numbers, blockers first.

### ↺ Added 2026-07-29 — verifying the single-channel revision — ✅ **ALL PASSED 2026-07-30**

6. **The extended probe run.** ✅ **Ran 2026-07-30T08:28:42.** [`gwam_channel_discovery.py`](../../eda/gwam_channel_discovery.py)
   produced **13 `BEGIN SHAREABLE` blocks** — which the manifest reports as **`n_sections: 12`**,
   because `c_run_manifest` counts `RESULTS` before emitting itself (the 11-block run above reported
   `n_sections: 10` the same way; asserting `n_sections == 13` reads as a vanished section when
   nothing is wrong) — plus `skipped == {}`, `complete: true`, and **12/12 payloads matching the
   manifest bytes+sha1**. Reproduce with:
   `python scripts/decode_databricks_export.py gwam_channel_discovery.html --expect-sections 12 --expect-blocks 13`.
   What each section said:
   - `evar105_census.brand_variant_sizing` → **Q3b sized.** `wealth-ca` 250,355 rows, `pvt-wealth`
     9,690, against ca-retirement's 1,298,417. `overlap_with_ca_retirement` is **exactly 0** on both,
     the expected result — they are additive (+19.3% / +0.7%), not already inside our predicate, so
     Q3b is live rather than moot. Both are present in the data, correcting the earlier assumption
     that they were unseen.
     ↺ **Q3b ANSWERED 2026-07-30 (Abhisekh): neither is part of Canada Retirement.** The sizing did its
     job — it turned a "we don't recognise these" into a priced either/or, and the SME ruled both
     **out**. The predicate stays exactly as held (`ca-retirement` + `gwam` parts-match), so this probe
     section closes a question **without triggering the re-baseline it was measuring the cost of**.
   - `evar105_census.scope_sizing_on_pipeline_rsid` → segment 1,298,417 vs URL-broad 1,415,399;
     `segment_only` **1,352** (the GAIN) and `url_only` **64,079** (the LOSS). The null-guard fix
     moved the gain little, as predicted for a web-only suite — switching to segment scope is a net
     **loss** of ~4.5% of traffic, which is the whole segment-vs-URL trade now that the other three
     channels are deferred (Q3).
   - `cid_vs_campaign` → `cid_only` small but `agreement_when_both` only **0.762 / 0.537**.
     `post_campaign` is **rejected** as the marketing proxy; the rule needs the raw query string and
     therefore an ADR-0007 amendment (§2.5.1).
   - `visit_shape` → `share_pv_eq_0` **3.25%**, `share_pv_eq_2` **11.67%** (daily cv 0.122). The
     "consistently 2" pattern is present but as an 11.7% point mass, not a dominant one; and the
     `pv_per_visit` floor of 1.2236 over 88 days means the SME's literal "< 1" test never fires (§1.1).
   - `visit_shape.visitor_grain` → ECID and visid-pair diverge on 14 of 88 days, by at most 15
     visitors (0.068%). Negligible; the grain question is settled (§1.1).
7. **The probe's new sections respect the privacy posture.** ✅ **Verified clean.** `cid_vs_campaign`
   emits counts only; it reads raw URLs transiently but nothing raw reaches the shareable block.
   `python scripts/decode_databricks_export.py gwam_channel_discovery.html --grep http --grep-sections cid_vs_campaign`
   returns `grep: 'http' clean across 1 section(s)` and exits zero.
8. **No pipeline logic moved.** ↺ **Amended 2026-07-30.** The 2026-07-29 pass moved no code at all.
   The 2026-07-30 pass closes G2, so `detect/registry.py` (five new `SeriesSpec` fields) and
   `detect/kpis.py` (the ratio arm) *did* change — but both are additive and inert: no GWAM series
   declares `kind=ratio`, every new field is defaulted, and `test_gold_parity`'s 35-series comparison
   is byte-identical before and after. `git diff databricks/` is still empty; bronze/silver/gold and
   the GWAM detector remain untouched by design.

---

## Revision log

| Date | Change |
|---|---|
| 2026-07-28 | Written pre-probe. Four-channel scope recorded as the spec of record; G1–G6 defined; 12-item SME agenda. |
| 2026-07-29 (early) | Probe C1–C10 run clean → **G1 closed**. Three pre-probe claims corrected: three of four suites located (not zero), delimiter is `":"` not `" : "`, and segment scope is *narrower* than URL scope (so it closes no French gap). Errors/Sign-in Errors reclassified from "nothing" to buildable. |
| 2026-07-29 (audit) | C3 null-guard bug found — `segment_only` (+1,436) is an undercount for NULL-URL rows; code fixed, figures flagged pending re-run. |
| **2026-07-29 (later, SME ruling)** | **Scope narrowed to the Public Website channel only.** D8 conflict dissolved (not resolved); `manucustomer.prod` access request moot; segment-scope justification collapsed; G3/G4 moot and **G2 promoted to critical**; Q5 answered (marketing = CID) with two new implementation gaps; **new §1.1** (three SME anomaly signals) and **new §2.5.1** (the CID rule); **new Q3b** (`wealth-ca` / `pvt-wealth`); Q6 escalated to blocking. Registry → **v0.5.0** (5 candidate / 14 deferred + 2 new signal seeds); probe gains C3 variant sizing, **C11** and **C12**. |
| **2026-07-30 (extended probe + G2)** | **The extended probe ran** (`generated_at` 2026-07-30T08:28:42, 12 sections, `complete: true`, 12/12 payloads verified against the manifest, privacy grep clean) and its results are folded in. **Three findings change the plan:** C11 **rejected** `post_campaign` as the CID proxy (agreement 0.762 suite / **0.537** segment), leaving an ADR-0007 amendment as the only route to marketing exclusion; C12 showed the SME's literal "page views per visit **< 1**" test **never fires** (88-day floor 1.2236, so the signal is inert as specified — `share_pv_eq_0` = 3.25% is the detectable quantity); and C12's `visitor_grain` **settled** the ECID-vs-visid-pair divergence as negligible (≤15 visitors/day, 0.068%). C3 **sized** Q3b — `wealth-ca` and `pvt-wealth` are additive with **zero** overlap. **G2 CLOSED**: `SeriesSpec` gains `numerator`/`denominator` + governance, `kpis.py` gains the ratio arm it was missing, `test_gold_parity` gains ratio + zero-denominator cases. Registry → **v0.6.0** (no entry added, removed, or promoted — evidence replacing expectation). Both signals now blocked on **Q6 alone**. |
