# 21 — GWAM Canada Retirement: the link-rule alerting scope
## SME spec received 2026-08-04 · scope redefinition · probe design · open questions

**Status:** Governance record for a **scope change**, not a new metric family.
**Decisions:** [16 §1](16-e2e-production-blueprint.md) **D12** (ingest scope → `broad`), **D13** (metric scope → the 16 link rules), **D14** (`post_evar194` must never enter a URL coalesce), and the **D8 amendment**.
**Registry:** `metric-registry.yaml` **v0.8.1** — probe evidence attached; still zero entries seeded.
**Probe:** `eda/gwam_channel_discovery.py` `C13`–`C18`. ✅ **RUN 2026-08-05** (`generated_at 2026-08-05T01:29:56`, window 2026-05-06 → 2026-08-04, 18/18 sections verified). **C17 cleared** — §5 carries the readings.
**Companion docs:** [19](19-gwam-channel-readiness.md) (channel readiness, gates), [20](20-gwam-sme-questions.md) (SME questionnaire — Part 5 carries Q13–Q21 raised here).

---

## 0. What changed, in one paragraph

The Business SME sent 8 named link-click rules, each specified in English **and** French as an (`evar193` Link Name, `evar194` Link Href) pair, plus `evar101` Page Name as a dimension, against the already-ruled Public Website channel (`manulifeglobalprod`) and the same three metrics as before (Page Views / Visits / Visitors). These rules were ratified as the **scope** for those site-wide metrics — a visit is in scope if it contains ≥1 rule-matching link click. That is a scope change, so it is the [D10](16-e2e-production-blueprint.md) event: one clean re-baseline, full `mode=backfill` with gold truncated. Nothing is built yet. This document records what arrived, what we know about it before running anything, and what the probe has to settle first.

**The single most important thing in this document — ✅ ANSWERED 2026-08-05.** The question was whether enough qualified visits exist per day to detect on, since all 42 gold series rebuild on that population. **They do:** C17 measured a **median 1,599 qualified visits/day** (min 368, max 3,180) over 88 days, with **zero days below 100 and zero days at zero**. The scope as specified can carry daily anomaly detection, so it proceeds to build rather than back to the SME.

⚠ **The second number is the one to carry into planning:** only **25.0%** of today's URL-scoped visits are qualified (121,303 of 448,306 under the live `broad_narrow` scope). That is not a gate failure — the gate was volume, and volume passed — but it is a **75% population drop** and therefore a real re-baseline, accepted as the **phase-1** scope. Re-widening is a later-phase decision, not a correction of this one.

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

✅ **ANSWERED 2026-08-05 — the mangling was in transit only, not on the site.** C14 `name_encoding` returns `mojibake: 0` for both affected rules, against `true_accents` of **3,190** (`signup_join/fr`) and **5,687** (`signin_join/fr`). The live tag carries proper UTF-8; only the spreadsheet was mangled. The mojibake alternates stay in `SME_LINK_RULES` because they cost nothing and removing them would be an unforced narrowing — but nothing depends on them, and the "two rules silently return zero" risk is closed.

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
| `signup_join` | fr | 147 | `id.manulife.ca/register` | `/register` | ~~mojibake link name~~ — spreadsheet only; feed is clean (C14) |
| `signin_join` | en | 139 | `id.manulife.ca/` | `enrolment/handlelogin` + **not** `/register` | |
| `signin_join` | fr | 139 | `id.manulife.ca/` | `enrolment/handlelogin` + **not** `/register` | ~~mojibake link name~~ — spreadsheet only; feed is clean (C14) |
| `app_apple` | en | 58 | `.../id1214009312` | `apps.apple.com/ca/app/manulife-mobile` | blank name; strict prefix of the FR one |
| `app_apple` | fr | 63 | `.../id1214009312` | same + `?l=fr` | blank name — **Q18** |
| `app_android` | en | 74 | `play.google.com/store/apps/details` | `play.google.com/store/apps/details` + `hl=en` | blank name — **Q18** |
| `app_android` | fr | 74 | `play.google.com/store/apps/details` | same + `hl=fr` | blank name — **Q18** |
| `find_advisor` | en | 66 | `manulife.ca/page/groupsavings-...` | `groupsavings-talk-to-an-advisor` | bare trailing `?` |
| `find_advisor` | fr | **262** | `manuvie.ca/page/solutionsepargne-...` | `solutionsepargne-parler-a-un-conseiller` | **over the 255 eVar cap**; carries `cid=` — **Q17** |

### 2.0 ✅ What the feed actually returned (C14/C17, 2026-08-05)

The table above is derived from the spec strings. This one is measured, over 88 days on
`manulifeglobalprod`. `token` is the working match; `qual. visits` is each rule's contribution to
the qualified population (C17 `per_rule_qualified_visits`); `p50` and `zero days` are C14 `volume`.

| rule_id | lang | exact | token | qual. visits | p50/day | zero days | verdict |
|---|---|---:|---:|---:|---:|---:|---|
| `signin_member` | en | 80,286 | **85,799** | 58,490 | 1,062 | 0 | 🟢 carries the scope |
| `signin_member` | fr | 30,706 | **30,706** | 14,754 | 289 | 0 | 🟢 — but see **Q20** (42% fire off-scope) |
| `signin_sponsor` | en | **0** | **5,847** | 3,826 | 79 | 0 | 🟢 exact=0 confirms **Q15** |
| `signin_sponsor` | fr | 851 | **1,910** | 1,021 | 23 | 8 | 🟡 thin |
| `signin_advisor` | en | 1,639 | **2,398** | 1,291 | 22 | 2 | 🟡 thin |
| `signin_advisor` | fr | 111 | **203** | 105 | 1 | 43 | 🔴 undetectable daily |
| `signup_join` | en | 35,143 | **35,216** | 15,617 | 469 | 0 | 🟢 |
| `signup_join` | fr | 3,231 | **3,241** | 1,358 | 40 | 0 | 🟢 |
| `signin_join` | en | 66,811 | **67,000** | 27,051 | 927 | 0 | 🟢 |
| `signin_join` | fr | 6,275 | **6,275** | 2,465 | 75 | 0 | 🟢 |
| `app_apple` | en | 4,636 | **4,636** | 2,359 | 37 | 25 | 🔴 undetectable daily — **Q21** |
| `app_apple` | fr | 259 | **259** | 149 | 0 | 46 | 🔴 undetectable daily |
| `app_android` | en | 2,163 | **2,167** | 1,116 | 15 | 29 | 🔴 undetectable daily — **Q21** |
| `app_android` | fr | 272 | **272** | 144 | 2 | 39 | 🔴 undetectable daily |
| `find_advisor` | en | **0** | **2,793** | 1,617 | 35 | 0 | 🟢 exact=0 — the bare trailing `?` |
| `find_advisor` | fr | **0** | **0**→361 | 190 | 3 | 23 | 🔴 truncated (§2.2) **and** thin — **Q17** |

**Three things this table settles.**

1. **The token strategy was necessary, not defensive.** Three rules return `exact = 0` and would
   have reported a clean, plausible, wrong zero under exact matching: `signin_sponsor/en` (the Q15
   missing character), `find_advisor/en` (the bare trailing `?`), and `find_advisor/fr` (truncated at
   the 255-char eVar cap — `prefix` is also 0; only the front-loaded token recovers its 361 hits).
2. **Six of sixteen rules cannot support daily detection** — the four app-download rules,
   `signin_advisor/fr`, and `find_advisor/fr`. Between 23 and 46 of 88 days are outright zero. This
   is the concrete input to **Q13**: a 16-series split would ship six series that alert on noise.
3. **The scope rests on four rules.** `signin_member/en`, `signin_join/en`, `signup_join/en` and
   `signin_member/fr` contribute **115,912 of the 131,553 rule-visit pairs (88.1%)**. ⚠ Read that as
   a share of pairs, **not** of visits: the 16 per-rule counts sum to 131,553 against **121,303
   distinct** qualified visits, so ~8% of qualified visits match more than one rule and the column
   does not partition. The remaining twelve rules carry governance weight far out of proportion to
   signal.

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

✅ **MEASURED 2026-08-05: `len_max = 255` exactly.** The eVar cap, not the prop cap — the better of the two branches.

- `signin_member/en` and `signin_join/en` are **separable**; the unrecoverable-collision scenario did not happen and needs no SME conversation.
- `find_advisor/fr` **is** stored truncated, and the prediction held precisely: C14 returns `exact = 0` **and** `prefix = 0` for it, with the token strategy recovering all **361** hits. This is the single clearest justification in the run for choosing token matching over exact.
- Full C13 shape: `post_evar194` non-blank **27.84%**, `apx_distinct` 6,878, lengths `min 14 / p50 50 / p95 139 / p99 173 / max 255`.

⚠ **The rules are `manulifeglobalprod`-only.** C13's per-rsid census shows `evar194` at **0.0%** on `manugrs` and `jhfswamjhreupeprod`, and 0.98% on the mobile suite. That is fine under D11's single-channel scope, but it means the link-rule definition **does not travel** if scope re-widens to the deferred channels — those channels would need a different scope predicate, not this one.

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

✅ **RE-TESTED 2026-08-05 — the contract holds, with one measured caveat.** C14's collision
histograms came back:

| strategy | 0 rules | 1 rule | 2 rules | 4 rules |
|---|---:|---:|---:|---:|
| `path` | 1,103,400 | 19,875 | 45,732 | **633,666** |
| `token` | 1,553,686 | **248,891** | **96** | 0 |

`path`'s 633,666 four-way collisions are §2.1's predicted merge, confirmed at scale — it is not a
weaker match, it is a wrong one, and it over-claims by more than 2.5× on the four merged rules.

⚠ **`token` is not perfectly unique: 96 hits match two rules.** §5 previously asserted it "should
show only `n=1`"; that claim is now corrected to a measured one. At **0.039%** of the 248,891
matched hits this changes no count materially and no rule's verdict, but it is real and should not
be rounded away — it means the feed stores at least one href the spec did not anticipate. It is the
one loose thread the run leaves in the matching contract, and it is small enough to carry rather
than block.

### 2.5 The D8 collision

Five of the eight rules — every sign-in and sign-up rule — target `id.manulife.ca` or `portal.manulife.ca`. **Both are on `SCOPE_LOGIN_HOST_EXCLUDE`**, the D8 login-host list that is subtracted from scope in every mode.

They survive today because that list is matched against `coalesce(page_url, post_page_url)` — the **page** the hit happened on — not against a link target. The distinction is real and defensible: D8 removes hits *whose own page* is a login host (someone already inside the authenticated experience); these rules count a click *on the public site* whose destination happens to be login (intent to sign in, measured before the user leaves). **D8 has been amended in place to say so**, because on its face the new scope reads as violating it.

But the distinction is now **load-bearing rather than incidental**, which is why it is also promoted to **D14**: `post_evar194` must never enter a URL coalesce or the login-host filter. Doc 16 §4.5 proposes a shared `_gwam_common.py` helper that does exactly the kind of coalesce that would swallow it — the prohibition has to exist **before** that helper is written. Probe **C15** measures the gap (`excl_by_page_url` ≈0% vs `excl_by_href` ≈100% for those five rules) so the cost of the mistake is a number.

✅ **MEASURED 2026-08-05 — D14 is no longer a prediction. The number is 95.8%.**

C15 returns, for all ten sign-in/sign-up rule×language combinations, `excl_by_page_url = 0` and
`excl_by_href = matched` — i.e. **exactly 0%** are excluded by what the pipeline does today, and
**exactly 100%** would be excluded if `post_evar194` ever joined the URL coalesce. Summed:

> **238,595 of 248,987 rule-matching hits (95.8%) would be silently deleted** — and with them the
> four rules that carry 88% of the qualified population. Gold would still build, every series would
> still populate, and the run would look clean.

The two app-download and two `find_advisor` rules are unaffected (`excl_by_href = 0`) because their
hrefs point at `apps.apple.com` / `play.google.com` / `manulife.ca`, none of which are login hosts.
So the failure would not even be uniform — it would delete the sign-in family and leave the noisy
rules standing, which is the worst possible shape for it to fail in.

This is the strongest empirical result in the run. D14 is now a measured prohibition, pinned by
`tests/test_link_rules.py::test_the_d8_login_exclusion_would_delete_five_of_the_eight_rules`.

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

That last point is what keeps Page Views coherent. Under the alternative reading (count only rule-matching hits) "page views" would be incoherent under the `adobe_pv` basis, since a link-click hit carries no page-view marker — 8 of the 24 requested metrics would not exist. Under qualified-visit grain both Q6 bases are meaningful, so **Q6 reverts to a plain "which number do you want"** rather than an existential question. It still moves every page-view series — ↺ **re-measured on the qualified population 2026-08-05: 4.31 vs 1.65 pv/visit** (C17; the earlier 2.885 / 1.343 from C12 was the URL scope and does not carry over). `adobe_pv_available: true` on this suite, so both branches are computable today.

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
python scripts/decode_databricks_export.py gwam_channel_discovery.html --grep "goto=http" \
    --grep-sections rsid_census,web_vs_app,evar105_census,platform_census,error_fields,\
signin_fields,event_census,pagename_census,manulifeid_split,marketing_fields,cid_vs_campaign,\
visit_shape,link_page_context,link_language_split,qualified_visit_scope,evar105_vs_rules
```

Pass = 19 blocks · `n_sections: 18` · `skipped == {}` · `complete: true` · 18/18 sha1 match · privacy grep clean.

✅ **BOTH PASSED 2026-08-05** (exit 0). `generated_at 2026-08-05T01:29:56`, table
`gwam_prod_catalog.inv_typed_common.adobe_hit_data`, window `2026-05-06 → 2026-08-04`,
`18/18 payloads match the manifest bytes+sha1`. Source parity independently confirmed: all **41**
cells of the export are byte-identical to `eda/gwam_channel_discovery.py` after normalizing the
`# MAGIC` / `# COMMAND` framing, so the export was produced from the committed source and not a
drifted workspace copy.

⚠ **`--grep-sections` is not decoration — without it this gate can never pass.** The original form
above ran the grep over **all** sections and therefore failed (exit 1) on `link_evar_census`, which
is not a leak: C13 and C14 exist to show hrefs, and the SME's own rule URLs contain `goto=http`.
Verified mechanically rather than argued — every one of the **7** distinct `goto=http` strings in
`link_evar_census` is a member of `SME_LINK_RULES`, and **0** discovered-but-unelided URLs were
found. `link_evar_census` and `link_rule_match` are therefore excluded from the pattern; every other
section still gets it.

⚠ **The 2026-07-30 export no longer matches this source.** C11's emitted note was corrected (it claimed the pipeline strips query strings — see §6), changing that section's sha1, and six sections were added. Do not verify the old HTML against the new file and conclude the harness is broken.

**Privacy posture.** C11 established "counts only, never a URL" for query strings. C13/C14 must show hrefs, so the rule is narrower: **a URL the SME gave us is echoed verbatim; a URL we discovered has its parameter values elided** (`?a=<v>&b=<v>`), keeping scheme/host/path and parameter keys. That is enough to answer "is it raw / is it encoded / which params" and emits no session token. Confirmed working in the run — e.g. the discovered `https://id.manulife.ca/sponsor?ui_locales=<v>&goto=<v>` (5,662 hits) is elided while the SME's own sponsor URL is echoed whole.

⚠ **The mechanical gate is the SME-allowlist check, not the grep.** A regex cannot express "unelided
*unless* the SME gave it to us"; that is why the grep needs the section exclusions and why the
allowlist check above is the assertion that actually carries the privacy contract. If C13/C14 are
ever re-run, re-run that check — the grep alone will not catch a new discovered URL that leaks
parameters, because those two sections are outside its scope by construction.

---

## 5. The results, read in that order (2026-08-05)

The reading order below is the one this section specified **before** the run. It is preserved so the
findings can be checked against the questions they were meant to answer, rather than against a
narrative written after the fact.

### 5.1 C17 — the gate: **CLEARED on volume, expensive on population**

| | qualified | `en_only` | `broad_narrow` (live) |
|---|---:|---:|---:|
| visits / day (median) | **1,599** | 5,006 | 5,799 |
| hits / day (median) | 6,886 | 14,281 | 16,320 |
| visitors / day (median) | 1,466 | 4,595 | 5,262 |

`verdict`: median **1,599**, min **368**, max **3,180**, `days_below_100: 0`, `days_at_zero: 0`,
over **88** days. **The gate passes.** There is no day in the window where the qualified population
could not support a daily test, and the minimum is comfortably above the level where robust-z and
level-shift lose power.

`visit_overlap` against the live `broad_narrow` scope: **121,303 qualified** vs **448,306
url-scoped**, **112,108 in both**, **9,195 qualified-only**, `share_of_url_scope_retained = 0.250`.

> **The re-baseline is the real cost, and it is large: the population drops ~75%.** That is not a
> gate failure — the gate was volume, and volume passed — but every one of the 42 gold series is
> rebuilt on a quarter of today's traffic, and no historical threshold survives. Accepted as the
> **phase-1** scope per the 2026-08-05 ruling; re-widening is a later-phase decision.
>
> Note the 9,195 **qualified-only** visits: the new scope is not a subset of the old one. It admits
> traffic today's URL filter misses, so this is a genuine re-scope, not a narrowing.

✅ **`identity_cardinality` closes a stated risk.** `02_silver_conform.py:70-73` warns that
`post_visid_high/low` can collapse to ≤1 on this suite, which would make every visit count an
artefact of `visit_num` alone. Measured: `post_visid_high` **1,688,658** distinct, `post_visid_low`
**1,659,683**, `visit_key` **3,445,407**. The warning does not fire; these counts are real.

⚠ **88 days, not 91.** `2026-06-26`, `2026-07-11` and `2026-07-21` are absent from the window — the
same feed-gap class already recorded for CoverMe. Immaterial to the gate (the medians are computed
over present days) but it must not be read as three zero-traffic days by anything downstream.

### 5.2 C13 — `len_max` = **255** → the good branch

The eVar cap, not the prop cap. `signin_member/en` and `signin_join/en` stay separable;
`find_advisor/fr` is truncated and unmatchable by exact or prefix. Full detail in §2.2.

### 5.3 C14 — `collision_hist.token` shows `n=2` at **96 hits**

Not the clean `n=1`-only result this section predicted. 0.039% of matched hits; material to nothing,
but the prediction was wrong and is corrected in §2.4 rather than quietly restated.

### 5.4 C15 — the `neither` column is where the surprise is

Most rules sit near zero. **`signin_member/fr` does not: 12,872 of 30,706 (41.9%)** of French member
sign-in clicks fire on pages no scope variant covers. C15's `top_host_paths` and
`top_page_names_on_matched` identify them as **Manulife Wealth FR** pages
(`/ca/fr/particuliers/patrimoine/patrimoine-manuvie/apercu` 4,484 and `/entrer` 4,259;
`manulife:ca:fr:personal:wealth:manulife-wealth:*` 6,003) — the `wealth-ca` brand that **doc 20 Q3b
ruled OUT of Canada Retirement on 2026-07-30**.

Two of our own rulings therefore disagree: D13 says these clicks are in scope because they match a
rule; Q3b says the pages they fire on are not Canada Retirement. → **Q20**.

Also measured here, and unrelated: `group_plans_only` — the traffic D12's dropped `%/group-plans%`
umbrella would have admitted — is **73.6%** of `app_apple/en` (3,413 of 4,636) and **67.1%** of
`app_android/en` (1,454 of 2,167), because the app-download buttons live on
`/ca/en/personal/group-plans/resources/mobile`. D12 was decided without this number. → **Q21**.

### 5.5 C17 — `per_rule_qualified_visits`: twelve of sixteen are rounding error

Four rules carry 88.1% of the rule-visit pairs; six rules cannot sustain a daily series at all. Full
table and the caveat about double-counting in §2.0. This is the input to **Q13**.

### 5.6 C16 and C18 — the two that were not on the reading list

**C16 (language).** No native locale column exists to retire the four derivations
(`native_language_candidates` shows nothing usable). Of the four, `href_param` and `link_name` agree
**99.98%** and `href_param`/`page_url` **99.56%**; `href_host` is the outlier at **82.8%**, exactly
as designed — it reads every `id.manulife.ca` URL as English. **`href_param` is confirmed as the
field of record**, and the deliberate choice to read the *raw* href first-match is vindicated: it is
what keeps `signin_sponsor/fr` and `signin_advisor/fr` French despite their inner `ui_locales%3Den-CA`.
⚠ `href_param` returns `unknown` for 100% of `app_apple/en` and `find_advisor/en` (those URLs carry
no locale parameter) — for the app and find-advisor families, language must come from `page_url`.
This is the data half of **Q18**.

**C18 (Q19).** The brand tag is **not** redundant on top of the rules. 21,465 rule-matching hits are
untagged (8.6%); at visit grain, ANDing the two costs **8,957 of 121,303 qualified visits (7.4%)**.
The disagreement is concentrated, not diffuse: `tagged_share` is 0.37 / 0.31 for Apple EN/FR, 0.43 /
0.60 for Android EN/FR, and 0.567 for `signin_member/fr` — while every other rule sits above 0.92.
So "keep both" would mostly delete the app-download family and the French member rule. → **Q19**, now
priced.

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

↺ **2026-08-05 — every question below now carries a number.** None was answered by the probe (they
are business rulings, which is why they are questions), but none has to be judged in the abstract
any more. Two new ones were raised **by** the run.

| Q | Question | Blocks | ↺ What the run added |
|---|---|---|---|
| **Q13** | EN and FR — one alert per rule, or two? | Whether the per-rule family is 8 series or 16, and therefore registry seeding. At C17's volumes the split may make several rules undetectable. | **Priced, and it points one way.** 6 of 16 language-split rules cannot support a daily series (23–46 zero days of 88); at 8-rule grain only `find_advisor` and the app pair stay marginal. §2.0. |
| **Q14** | Re-ask **Q6** — Adobe page views or every hit? | Every page-view series. Now a plain choice rather than an existential one (§3.2), but it moves 2.885 → 1.343. | **Re-measured on the new population**: on qualified visits the two bases give **4.31** vs **1.65** pv/visit (median daily 6,886 `all_hits` vs 2,643 `adobe_pv`). The old 2.885 / 1.343 figures were computed on the URL scope and no longer apply. |
| **Q15** | `signin_sponsor/en` arrived at 143 chars ending `en-C`, one short of its structural twin. Confirm the full value. | Exact matching for that rule. Token matching is unaffected. | **Data says the character really was lost**: `exact = 0` against 5,847 token matches. The string as transcribed matches nothing in 88 days. |
| **Q16** | `signin_sponsor/fr` and `signin_advisor/fr` carry `ui_locales=en-CA` **inside** an `fr-CA` sign-in URL. Site bug or intended? | If a bug, the alert watches a broken link. | **Confirmed present in the feed**, not a transcription artifact — and both rules still read French because language is taken from the raw href first-match (C16). The site question stands. |
| **Q17** | `find_advisor/fr` carries `cid=CA-FR_ML_RE_IR_...`. Under the SME's own Q5 rule ("marketing = carries CID") this link **is** marketing, but the channel table says "ideally non-marketing". | Two of the SME's own rulings point opposite ways on one rule. | **Cheap either way**: 361 hits / 190 qualified visits over 88 days, p50 3/day, 23 zero days. It is also truncated past the 255-char cap. Whichever way it goes, it is a rounding-error rule. |
| **Q18** | `app_apple` / `app_android` have a **blank** Link Name in both languages. Confirm the href alone is the identifier — and that `?l=fr` / `&hl=fr` are two tracked links rather than one button with a browser-appended locale. | Whether those two rules can be language-split at all. | **The href alone is confirmed as the only identifier**: `token_name` drops to 3,874 of 4,636 for `app_apple/en`, and C16 shows `link_name` is `unknown` for 96% of app hits. The one-button-vs-two question remains genuinely open. |
| **Q19** | Does the `evar105 = 'ca-retirement : : GWAM'` brand tag still apply **on top of** the link rules, or do the rules replace it? | If both apply they AND together and shrink the population further, on top of whatever C17 reports. C18 sizes it. | **Not redundant — ANDing costs 7.4%** (8,957 of 121,303 qualified visits), concentrated in the app-download family (`tagged_share` 0.31–0.43) and `signin_member/fr` (0.567). §5.6. |
| **Q20** 🆕 | 41.9% of French member sign-in clicks fire on **Manulife Wealth FR** pages — the `wealth-ca` brand **Q3b ruled out** of Canada Retirement. Are those clicks in scope or not? | `signin_member/fr` entirely, and the coherence of D13 against Q3b. Our own two rulings disagree. | New from C15 — see §5.4. |
| **Q21** 🆕 | D12 dropped `%/group-plans%` from the ingest scope without knowing its cost. It is **~70% of the app-download clicks** (73.6% Apple EN, 67.1% Android EN). Re-admit the umbrella, accept the loss, or drop the app rules? | The two app-download rules, and whether D12 should be revisited. | New from C15 — see §5.4. |

---

## 8. Revision log

- **2026-08-05** — **the probe ran; §2 stops being a prediction.** `generated_at 2026-08-05T01:29:56`,
  88 days on `manulifeglobalprod`, 18/18 sections matching the manifest bytes+sha1, source parity
  41/41 cells against the committed notebook. **C17 cleared the gate** (median 1,599 qualified
  visits/day, no day below 100) at a **75% population cost** (`share_of_url_scope_retained` 0.250),
  accepted as the phase-1 scope. Added §2.0 (measured rule table), §5 rewritten from a reading order
  into the readings. Four claims **corrected against data**, not merely filled in: the FR mojibake is
  a spreadsheet artifact and not in the feed (§1); `token` collisions are `n=2 × 96`, not uniquely
  `n=1` (§2.4); the §4 privacy gate as written **could never pass** and now carries `--grep-sections`
  plus an SME-allowlist check (§4); and D14's cost moved from "≈100%" to a measured **95.8% /
  238,595 hits** (§2.5). Two new questions raised by the run — **Q20** (FR member clicks on
  Q3b-excluded Wealth pages) and **Q21** (the measured price of D12's `%/group-plans%` exclusion).
  Registry → **v0.8.1** (evidence only; no entry seeded, promoted or removed).
- **2026-08-04** — created. Records the SME link-rule spec received the same day, the scope redefinition (D12/D13/D14 + the D8 amendment), the measured collision analysis, the C13–C18 probe design, and Q13–Q19. Registry → v0.8.0. Ingest scope flipped `en_only` → `broad` with `%/group-plans%` deliberately excluded. **Probe not yet run** — every figure in §2 is derived from the spec strings, not from data; §5 says what to read first when it does.
