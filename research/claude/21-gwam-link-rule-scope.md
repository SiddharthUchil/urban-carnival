# 21 — GWAM Canada Retirement: the link-rule alerting scope
## SME spec received 2026-08-04 · scope redefinition · probe design · open questions

**Status:** Governance record for a **scope change**, not a new metric family.
**Decisions:** [16 §1](16-e2e-production-blueprint.md) **D12** (ingest scope → `broad`), **D13** (metric scope → the 16 link rules), **D14** (`post_evar194` must never enter a URL coalesce), and the **D8 amendment**.
**Registry:** `metric-registry.yaml` **v0.8.0** — 5 `scope_predicate` strings rewritten, zero entries seeded.
**Probe:** `eda/gwam_channel_discovery.py` `C13`–`C18`. **Not yet run.**
**Companion docs:** [19](19-gwam-channel-readiness.md) (channel readiness, gates), [20](20-gwam-sme-questions.md) (SME questionnaire — Part 5 carries Q13–Q19 raised here).

---

## 0. What changed, in one paragraph

The Business SME sent 8 named link-click rules, each specified in English **and** French as an (`evar193` Link Name, `evar194` Link Href) pair, plus `evar101` Page Name as a dimension, against the already-ruled Public Website channel (`manulifeglobalprod`) and the same three metrics as before (Page Views / Visits / Visitors). These rules were ratified as the **scope** for those site-wide metrics — a visit is in scope if it contains ≥1 rule-matching link click. That is a scope change, so it is the [D10](16-e2e-production-blueprint.md) event: one clean re-baseline, full `mode=backfill` with gold truncated. Nothing is built yet. This document records what arrived, what we know about it before running anything, and what the probe has to settle first.

**The single most important thing in this document:** we do not yet know whether enough qualified visits exist per day to detect on. All 42 gold series would rebuild on that population. Probe section **C17** answers it, and if the answer is small the scope as specified goes back to the SME rather than into a pipeline.

---

## 1. The spec as received

Reproduced verbatim, including the mojibake, because a future reader must be able to tell what arrived from what we corrected.

**Header block**

| Field | Value |
|---|---|
| Alerts for Anomalies | Public Website |
| Instance | Manulife |
| Report Suite | Manulife Global Prod |
| Page Views (count) | 1 |
| Visits (count) | 1 |
| Visitors (count) | 1 |
| evar101 | Page Name |
| evar193 | Link Name |
| evar194 | Link HREF URL |

`Manulife Global Prod` resolves to rsid **`manulifeglobalprod`**, which the pipeline already ships (`settings.SCOPE_RSID`) — so the suite and channel are unchanged from [D11](16-e2e-production-blueprint.md). The `1`s are the same in-scope marks as the 2026-07-28 channel table; what they mean literally is still [doc 20 Q4](20-gwam-sme-questions.md), unanswered and now largely moot.

**ENGLISH**

| RULES | evar193 | evar194 |
|---|---|---|
| Sign in - Member | Sign in | `https://id.manulife.ca/?ui_locales=en-CA&goto=https://portal.manulife.ca/apps/groupretirement/portal/member/handlelogin?ui_locales=en-CA` |
| Sign in - Sponsor | Sign in | `https://id.manulife.ca/sponsor?ui_locales=en-CA&goto=https://portal.manulife.ca/apps/groupretirement/portal/sponsor/handlelogin?ui_locales=en-C` |
| Sign in - Advisor | Sign in | `https://id.manulife.ca/advisor?ui_locales=en-CA&goto=https://portal.manulife.ca/apps/groupretirement/portal/advisor/handlelogin?ui_locales=en-CA` |
| Sign up to join | set one up to join | `https://id.manulife.ca/register?ui_locales=en-CA&goto=https://portal.manulife.ca/apps/groupretirement/portal/enrolment/handlelogin?ui_locales=en-CA` |
| Sign in to join | Sign in to join | `https://id.manulife.ca/?ui_locales=en-CA&goto=https://portal.manulife.ca/apps/groupretirement/portal/enrolment/handlelogin?ui_locales=en-CA` |
| Apple App Download - Link Click | *(blank)* | `https://apps.apple.com/ca/app/manulife-mobile/id1214009312` |
| Android App Download - Link Click | *(blank)* | `https://play.google.com/store/apps/details?id=ca.manulife.MobileGBRS&hl=en` |
| Find an Advisor | Get started | `https://www.manulife.ca/page/groupsavings-talk-to-an-advisor.html?` |

**FRENCH**

| RULES | evar193 | evar194 |
|---|---|---|
| Sign in - Member | Ouvrir une session | `https://id.manulife.ca/?ui_locales=fr-CA` |
| Sign in - Sponsor | Ouvrir une session | `https://id.manulife.ca/sponsor/?ui_locales=fr-CA&goto=https%3A%2F%2Fportal.manulife.ca%2Fapps%2Fgroupretirement%2Fportal%2Fsponsor%2Fhandlelogin%3Fui_locales%3Den-CA` |
| Sign in - Advisor | Ouvrir une session | `https://id.manulife.ca/advisor/?ui_locales=fr-CA&goto=https%3A%2F%2Fportal.manulife.ca%2Fapps%2Fgroupretirement%2Fportal%2Fadvisor%2Fhandlelogin%3Fui_locales%3Den-CA` |
| Sign up to join | `crÃ©ez-en un pour vous inscrire` | `https://id.manulife.ca/register?ui_locales=fr-CA&goto=https://portal.manulife.ca/apps/groupretirement/portal/enrolment/handlelogin?ui_locales=fr-CA` |
| Sign in to join | `Ouvrir une session pour adhÃ©rer` | `https://id.manulife.ca/?ui_locales=fr-CA&goto=https://portal.manulife.ca/apps/groupretirement/portal/enrolment/handlelogin?ui_locales=fr-CA` |
| Apple App Download - Link Click | *(blank)* | `https://apps.apple.com/ca/app/manulife-mobile/id1214009312?l=fr` |
| Android App Download - Link Click | *(blank)* | `https://play.google.com/store/apps/details?id=ca.manulife.MobileGBRS&hl=fr` |
| Find an Advisor | Lancez-vous | `https://www.manuvie.ca/page/solutionsepargne-parler-a-un-conseiller.html?cid=CA-FR_ML_RE_IR_RetirementWebsite_PRLandingPage_PlanRight________&utm_source=RetirementWebsite&utm_medium=IR&utm_campaign=_PRLandingPage&utm_content=_&utm_term=ML_RE_CA-FR_PlanRight_____`

**Corrections we applied, and why they are corrections and not edits**

| As received | Corrected to | Basis |
|---|---|---|
| `crÃ©ez-en un pour vous inscrire` | `créez-en un pour vous inscrire` | Classic UTF-8-read-as-Latin-1 mojibake (`Ã©` → `é`). |
| `Ouvrir une session pour adhÃ©rer` | `Ouvrir une session pour adhérer` | Same. |

⚠ **Both forms are carried in code and both are matched.** The SME's own file was mangled in transit, which means the tag on the live site may be mangled too. Probe **C14** reports which encoding the data actually holds (`name_encoding`) rather than us guessing. This is not pedantry: if the site stores the mojibake and we match only the corrected form, two rules silently return zero.

The rule **names** are language-independent — "Sign in - Member" appears in both tables — so the 8 rules are one set with two match specifications, not 16 unrelated rules. Whether they should *alert* as 8 series or 16 is **Q13**.

---

## 2. The rule table, measured

Everything below was computed from the exact strings above and is pinned by `tests/test_link_rules.py`. The authoritative machine-readable copy is `SME_LINK_RULES` in `eda/gwam_channel_discovery.py` — the URLs are deliberately **not** duplicated into the registry or into code twice.

| rule_id | lang | len | path-only | token | notes |
|---|---|---:|---|---|---|
| `signin_member` | en | 136 | `id.manulife.ca/` | `member/handlelogin` | |
| `signin_member` | fr | 40 | `id.manulife.ca/` | `id.manulife.ca/?ui_locales=fr-ca` + **not** `goto=` | no `goto` at all; strict prefix of `signin_join/fr` |
| `signin_sponsor` | en | **143** | `id.manulife.ca/sponsor` | `/sponsor` | **one char short of its twin — Q15** |
| `signin_sponsor` | fr | 165 | `id.manulife.ca/sponsor/` | `/sponsor` | percent-encoded; inner `ui_locales%3Den-CA` — **Q16** |
| `signin_advisor` | en | 144 | `id.manulife.ca/advisor` | `/advisor` | |
| `signin_advisor` | fr | 165 | `id.manulife.ca/advisor/` | `/advisor` | percent-encoded; inner `ui_locales%3Den-CA` — **Q16** |
| `signup_join` | en | 147 | `id.manulife.ca/register` | `/register` | |
| `signup_join` | fr | 147 | `id.manulife.ca/register` | `/register` | mojibake link name |
| `signin_join` | en | 139 | `id.manulife.ca/` | `enrolment/handlelogin` + **not** `/register` | |
| `signin_join` | fr | 139 | `id.manulife.ca/` | `enrolment/handlelogin` + **not** `/register` | mojibake link name |
| `app_apple` | en | 58 | `.../id1214009312` | `apps.apple.com/ca/app/manulife-mobile` | blank name; strict prefix of the FR one |
| `app_apple` | fr | 63 | `.../id1214009312` | same + `?l=fr` | blank name — **Q18** |
| `app_android` | en | 74 | `play.google.com/store/apps/details` | `play.google.com/store/apps/details` + `hl=en` | blank name — **Q18** |
| `app_android` | fr | 74 | `play.google.com/store/apps/details` | same + `hl=fr` | blank name — **Q18** |
| `find_advisor` | en | 66 | `manulife.ca/page/groupsavings-...` | `groupsavings-talk-to-an-advisor` | bare trailing `?` |
| `find_advisor` | fr | **262** | `manuvie.ca/page/solutionsepargne-...` | `solutionsepargne-parler-a-un-conseiller` | **over the 255 eVar cap**; carries `cid=` — **Q17** |

### 2.1 Query-stripping does not blur these rules — it merges them

This is the finding with the widest blast radius, because the repo already contains code that would do it. `gwam_canada_retirement_eda.py` S4b (`:835-837`, `:874-875`) and S4c (`:973-975`) apply a **hard-coded, non-optional** `^([^?#]*)` to URL columns. Point that at `evar194` and:

- **4 rules collapse into 1 bucket:** `signin_member` and `signin_join`, in **both** languages, are all just `https://id.manulife.ca/`.
- **3 more lose their EN/FR split:** `signup_join`, `app_apple`, `app_android`.
- 16 hrefs → **10 distinct paths**.
- `signin_sponsor` and `signin_advisor` survive **only on a trailing slash** (`/sponsor` EN vs `/sponsor/` FR). A trailing-slash normalizer — the most innocuous-looking URL cleanup there is — would collapse those four into two. No such normalizer exists in the repo today. Keep it that way.

Path-only matching is not a *weaker* match here. It is a **wrong** one, and it fails silently: every rule still returns a plausible non-zero count. Probe **C14** therefore reports `m_path` alongside `m_token` with a collision histogram, so the merge is visible as data rather than trusted as prose.

### 2.2 The truncation cliff

`signin_member/en` and `signin_join/en` are byte-identical for their **first 101 characters** and diverge at 102 (`member/` vs `enrolment/`). Adobe props cap at 100 characters, eVars at 255.

| cap | distinct of 16 |
|---:|---:|
| 100 | **15** |
| 128 | 16 |
| 255 | 16 |

So a `max(char_length(post_evar194))` of exactly **100** means those two rules are **one value in the data** and no matching strategy recovers them — that is an SME conversation, not an engineering fix. A max of exactly **255** means `find_advisor/fr` (262 chars) is stored truncated and can never match exactly; its token sits at the front of the URL, which is the whole reason the token strategy exists rather than exact matching. **C13 reports this number first.**

### 2.3 Two hrefs are strict prefixes of others

`signin_member/fr` ⊂ `signin_join/fr`, and `app_apple/en` ⊂ `app_apple/fr`. Naive `startswith` over-claims on both. This is why C14 reports `m_prefix` as a **diagnostic** rather than using it as the match, and why `signin_member/fr` carries an explicit anti-token (`goto=`).

### 2.4 The matching contract

```
match(rule, hit) :=  token ∈ decoded(href)
                 ∧  anti_token ∉ decoded(href)        (where present)
                 ∧  lang_token ∈ raw(href)            (where present)
                 ∧  lang_anti_token ∉ raw(href)       (where present)
```

`decoded` applies only the four percent sequences that actually occur (`%3a %2f %3f %3d`) — **not** a general decoder; Spark has no `unquote` and none exists in this repo. C13 reports `pct_encoded` row counts so a reader can see how much of the real data that shortcut covers.

⚠ **Language is read off the RAW href, not the decoded one.** `signin_sponsor/fr` and `signin_advisor/fr` carry an inner `ui_locales%3Den-CA`; decoding first turns `%3D` into `=` and reads those French rules as English. Verified by `test_language_marker_reads_the_raw_href_not_the_decoded_one`.

This contract resolves all 16 rules uniquely against the SME's own strings. It is verified, not assumed — and C14 re-tests it against what the feed actually stores, which can differ (truncated, re-encoded, redirected).

### 2.5 The D8 collision

Five of the eight rules — every sign-in and sign-up rule — target `id.manulife.ca` or `portal.manulife.ca`. **Both are on `SCOPE_LOGIN_HOST_EXCLUDE`**, the D8 login-host list that is subtracted from scope in every mode.

They survive today because that list is matched against `coalesce(page_url, post_page_url)` — the **page** the hit happened on — not against a link target. The distinction is real and defensible: D8 removes hits *whose own page* is a login host (someone already inside the authenticated experience); these rules count a click *on the public site* whose destination happens to be login (intent to sign in, measured before the user leaves). **D8 has been amended in place to say so**, because on its face the new scope reads as violating it.

But the distinction is now **load-bearing rather than incidental**, which is why it is also promoted to **D14**: `post_evar194` must never enter a URL coalesce or the login-host filter. Doc 16 §4.5 proposes a shared `_gwam_common.py` helper that does exactly the kind of coalesce that would swallow it — the prohibition has to exist **before** that helper is written. Probe **C15** measures the gap (`excl_by_page_url` ≈0% vs `excl_by_href` ≈100% for those five rules) so the cost of the mistake is a number.

---

## 3. What the scope now means

### 3.1 Two layers

| Layer | Where | Predicate | Status |
|---|---|---|---|
| **Ingest** | `01_bronze_ingest.py` | `rsid = manulifeglobalprod` ∧ page-URL patterns (`broad`, minus group-plans) ∧ ¬login-host | **D12 — landed** (config only) |
| **Metric** | silver → gold | visit contains ≥1 rule-matching `post_evar194` | **D13 — ruled, not built** |

Keeping the URL filter at ingest is deliberate: the rule set stays re-editable without a re-ingest, and full page context survives so Page Views remain computable under either Q6 basis.

### 3.2 Qualified-visit grain

A visit is in scope if it contains **≥1** rule-matching link click. Then:

- **Visits** = distinct qualified visits
- **Visitors** = distinct visitors across qualified visits (`mcvisid`, gold's grain)
- **Page Views** = page views **within** qualified visits — *all* of them, not just the clicks

That last point is what keeps Page Views coherent. Under the alternative reading (count only rule-matching hits) "page views" would be incoherent under the `adobe_pv` basis, since a link-click hit carries no page-view marker — 8 of the 24 requested metrics would not exist. Under qualified-visit grain both Q6 bases are meaningful, so **Q6 reverts to a plain "which number do you want"** rather than an existential question. It still moves every page-view series (C12 measured 2.885 vs 1.343).

### 3.3 What it would take to build

Not yet built — see §4. When it is:

1. `conf/bronze_columns.py` — add `post_evar101/193/194` to `DETECTOR_COLUMNS`, `REQUIRED_SOURCE_COLUMNS`, `SILVER_COLUMNS`. **Requires a bronze backfill.**
2. `02_silver_conform.py` — compute `visit_qualified` as a semi-join on the visit key.
3. `03_gold_kpis.py` — `.where(F.col("visit_qualified"))` before `build_kpis_spark`.

Step 3 is chosen specifically so that **`gold_lib.py` and `detect/kpis.py` need no changes at all** — which means the pandas↔Spark parity assertion at `04_detect.py:68` holds for free, instead of becoming a third place to keep in sync. Per-rule click series are a separate question, gated on Q13.

Then: full `mode=backfill` with gold truncated, and recalibration of detector thresholds, injected-anomaly scenarios, and the ≤3% FP ceiling in `tests/test_detect.py`.

---

## 4. The probe: C13–C18

`eda/gwam_channel_discovery.py`, appended after C12. A clean run prints **19** `BEGIN SHAREABLE` blocks and reports `n_sections: 18`.

| Section | Settles |
|---|---|
| **C13** `link_evar_census` | Do `evar101/193/194` carry data here, at what rate and cardinality? Is the stored href raw or already stripped/encoded? **`len_max` — the truncation cliff (§2.2).** |
| **C14** `link_rule_match` | Per rule × language: hits, days, daily volume distribution, under five matching strategies side by side, with a collision histogram. Which link-name encoding the data holds. |
| **C15** `link_page_context` | Which pages host these clicks; `{en_only, broad, broad_narrow, neither}` per rule; `group_plans_only` (the price of D12's narrowing); **the D8 collision, quantified**. |
| **C16** `link_language_split` | Four independent language derivations + their pairwise agreement. Whether a native locale column exists that would retire all four. |
| **C17** `qualified_visit_scope` | **THE GATE.** Qualified-visit population vs today's URL-scoped population, their **overlap**, per-rule contribution, page views under **both** Q6 bases, and the visid-cardinality check. |
| **C18** `evar105_vs_rules` | Does D11's brand tag still add anything on top of the rules, or is it redundant? (**Q19**) |

**Run + verify** (manual Databricks UI hand-off; no CLI or credentials in this repo):

```
python scripts/decode_databricks_export.py gwam_channel_discovery.html --expect-sections 18
python scripts/decode_databricks_export.py gwam_channel_discovery.html --grep "goto=http"
```

Pass = 19 blocks · `n_sections: 18` · `skipped == {}` · `complete: true` · 18/18 sha1 match · privacy grep clean.

⚠ **The 2026-07-30 export no longer matches this source.** C11's emitted note was corrected (it claimed the pipeline strips query strings — see §6), changing that section's sha1, and six sections were added. Do not verify the old HTML against the new file and conclude the harness is broken.

**Privacy posture.** C11 established "counts only, never a URL" for query strings. C13/C14 must show hrefs, so the rule is narrower: **a URL the SME gave us is echoed verbatim; a URL we discovered has its parameter values elided** (`?a=<v>&b=<v>`), keeping scheme/host/path and parameter keys. That is enough to answer "is it raw / is it encoded / which params" and emits no session token. Gated mechanically by the `--grep "goto=http"` check above.

---

## 5. Reading the results

**Read C17 first.** If `verdict.median_daily_qualified_visits` is small, nothing else matters yet — the scope as specified cannot carry daily anomaly detection and that goes back to the SME. For orientation: the pre-flip `en_only` population was ~1.15M hits over 157 days ≈ **7.3k hits/day total**, and individual link clicks on marketing pages are a small fraction of that. `visit_overlap.share_of_url_scope_retained` says how much of today's 42 series survives.

**Then C13's `len_max`.** 100 → rules 1 and 5 are unrecoverable. 255 → `find_advisor/fr` is truncated.

**Then C14's `collision_hist.token`.** It should show only `n=1`. Anything else means the feed stores something the spec did not anticipate — read `unmatched_href_top` before trusting any count.

**Then C15's `neither` column.** Rule hits firing on pages no scope variant covers mean the ingest widening does not reach them and another scope question is open. `settings.py` already records `epargnemanuvie.ca` as uncovered.

**Then C17's `per_rule_qualified_visits`.** A rule contributing near-zero visits adds governance weight and no signal — worth telling the SME before it becomes an alert nobody can action.

---

## 6. Correction carried by this change

Several places in the repo asserted that **"the pipeline strips query strings by policy (ADR-0007)"**. That is **false of the code** and was corrected in this pass — it mattered here because query strings are the only discriminator for 7 of the 16 rules.

- Bronze writes `post_page_url` **verbatim**; nothing in bronze, silver or gold strips a query string.
- ADR-0007 governs **identity pseudonymization**, not URL truncation.
- The stripping is in the **EDA notebooks** only (`gwam_canada_retirement_eda.py` S4b/S4c hard-coded; S9 behind an opt-in widget defaulting to `false`).
- The real constraint is that **`SILVER_COLUMNS` drops `post_page_url`**, so any URL-derived field must be computed at bronze or silver.

Consequence for the CID/marketing rule ([19 §2.5.1](19-gwam-channel-readiness.md), doc 16 backlog #16): a production CID rule is a `regexp_extract` over a column we already carry — a **design decision about where the parse lives, not an ADR amendment**. Backlog #16 is retagged, not resolved; implementing a CID exclusion remains out of scope here.

Corrected in: `eda/gwam_channel_discovery.py` (C0 comment, C11 markdown, C11 emitted payload), `metric-registry.yaml` (two notes), [19 §2.5.1](19-gwam-channel-readiness.md).

---

## 7. Open questions → doc 20 Part 5

| Q | Question | Blocks |
|---|---|---|
| **Q13** | EN and FR — one alert per rule, or two? | Whether the per-rule family is 8 series or 16, and therefore registry seeding. At C17's volumes the split may make several rules undetectable. |
| **Q14** | Re-ask **Q6** — Adobe page views or every hit? | Every page-view series. Now a plain choice rather than an existential one (§3.2), but it moves 2.885 → 1.343. |
| **Q15** | `signin_sponsor/en` arrived at 143 chars ending `en-C`, one short of its structural twin. Confirm the full value. | Exact matching for that rule. Token matching is unaffected. |
| **Q16** | `signin_sponsor/fr` and `signin_advisor/fr` carry `ui_locales=en-CA` **inside** an `fr-CA` sign-in URL. Site bug or intended? | If a bug, the alert watches a broken link. |
| **Q17** | `find_advisor/fr` carries `cid=CA-FR_ML_RE_IR_...`. Under the SME's own Q5 rule ("marketing = carries CID") this link **is** marketing, but the channel table says "ideally non-marketing". | Two of the SME's own rulings point opposite ways on one rule. |
| **Q18** | `app_apple` / `app_android` have a **blank** Link Name in both languages. Confirm the href alone is the identifier — and that `?l=fr` / `&hl=fr` are two tracked links rather than one button with a browser-appended locale. | Whether those two rules can be language-split at all. |
| **Q19** | Does the `evar105 = 'ca-retirement : : GWAM'` brand tag still apply **on top of** the link rules, or do the rules replace it? | If both apply they AND together and shrink the population further, on top of whatever C17 reports. C18 sizes it. |

---

## 8. Revision log

- **2026-08-04** — created. Records the SME link-rule spec received the same day, the scope redefinition (D12/D13/D14 + the D8 amendment), the measured collision analysis, the C13–C18 probe design, and Q13–Q19. Registry → v0.8.0. Ingest scope flipped `en_only` → `broad` with `%/group-plans%` deliberately excluded. **Probe not yet run** — every figure in §2 is derived from the spec strings, not from data; §5 says what to read first when it does.
