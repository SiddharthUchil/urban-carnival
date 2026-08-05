# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse — GWAM Canada Retirement: multi-channel discovery probe
# MAGIC
# MAGIC **Purpose.** The business SME sent a four-channel alerting scope for Canada Retirement
# MAGIC (Public Website / Web Member / Mobile / ManulifeID) defined by **report suite + segment
# MAGIC field**, not by URL. Three of the four report suites appear nowhere in this repo, and the
# MAGIC segment fields (`evar105`, "v185", a pagename group) have never been profiled. This probe
# MAGIC answers everything about that table that can be answered **from data**, so the questions we
# MAGIC put to the SME are only the ones that genuinely need a business ruling.
# MAGIC
# MAGIC **↺ 2026-07-29 — the alerting scope narrowed to the Public Website channel only** (SME
# MAGIC ruling, Abhisekh). The four-channel census sections `C1`-`C10` are KEPT: they are the
# MAGIC evidence base for the 14 registry entries now marked `deferred`, so a re-widening costs a
# MAGIC status flip rather than another discovery cycle. The same update answered the marketing
# MAGIC question ("marketing" = hits carrying the **CID** query-string parameter) and named three
# MAGIC anomaly signals of its own. That is what the two new sections are for: `C11` tests whether
# MAGIC the CID rule is expressible with the column we actually ingest, and `C12` profiles the
# MAGIC per-visit page-view distribution behind the new signals. `C3` also gains sizing for two
# MAGIC brand-tag variants the SME mentioned that appear nowhere in our data.
# MAGIC
# MAGIC Companion to [19-gwam-channel-readiness.md](../research/claude/19-gwam-channel-readiness.md)
# MAGIC and [20-gwam-sme-questions.md](../research/claude/20-gwam-sme-questions.md). Precedent:
# MAGIC CoverMe ran a discovery probe before its main EDA.
# MAGIC
# MAGIC **This is NOT the GWAM EDA.** `gwam_canada_retirement_eda.py` filters to two rsids and a URL
# MAGIC include-list; that filter would drop mobile-app hits entirely (app hits carry no page URL)
# MAGIC and it has no eVar105/eVar185 census. This probe is deliberately **unscoped**: it reads the
# MAGIC whole table and reports per-rsid, so a suite we do not yet know about still shows up.
# MAGIC
# MAGIC **Sections.**
# MAGIC
# MAGIC | id | Question it settles |
# MAGIC |---|---|
# MAGIC | `C1 rsid_census` | Do `manucustomer.prod`, a GRS+ suite, and a GBRS-mobile suite exist here? Enough history each? |
# MAGIC | `C2 web_vs_app` | Which suites are app (no URL) vs web — i.e. can a URL-based scope express them at all? |
# MAGIC | `C3 evar105_census` | Is `ca-retirement :  : GWAM` a literal value? What delimiter? How big is segment-scope vs today's URL scope? Plus: how much traffic do the `wealth-ca` / `pvt-wealth` brand variants carry (Q3b)? |
# MAGIC | `C4 platform_census` | Is `MPS Member` a value of eVar185 or eVar110? (resolves the long-open Platform conflict) |
# MAGIC | `C5 error_fields` | Is an Errors metric implementable, and from which field? |
# MAGIC | `C6 signin_fields` | Can a sign-in completion ratio be built, and from what? |
# MAGIC | `C7 event_census` | The `post_event_list` id space of each suite. |
# MAGIC | `C8 pagename_census` | Is "Canada Retirement App Pages v2" translatable into a pagename predicate? |
# MAGIC | `C9 manulifeid_split` | Does ANY field separate retirement sign-ins from other ManulifeID sign-ins? (the SME's own open item) |
# MAGIC | `C10 marketing_fields` | What could "ideally non-marketing" mean operationally? |
# MAGIC | `C11 cid_vs_campaign` | The SME's marketing rule is "carries CID". Is `post_campaign` — the column we actually ingest — equivalent to the presence of a `cid=` query parameter? |
# MAGIC | `C12 visit_shape` | What does the per-visit page-view distribution look like: how many visits have zero page views (the "< 1" signal), and how stable is the mass at exactly 2 (the duplication signal)? Also: ECID vs visid-pair visitor counts. |
# MAGIC | `C13 link_evar_census` | Do `evar101`/`193`/`194` carry data here, and is the stored href raw? **Is it truncated?** (a 100-char cap merges two rules permanently) |
# MAGIC | `C14 link_rule_match` | Per rule × language: how many hits match, under five different matching strategies, and how much does each strategy over-claim? Is there enough daily volume to detect on? |
# MAGIC | `C15 link_page_context` | Which pages host these clicks, and is any scope variant wide enough to see them? Plus the size of the D8 href collision. |
# MAGIC | `C16 link_language_split` | Can EN/FR be derived at all, and do four independent derivations agree? |
# MAGIC | `C17 qualified_visit_scope` | **The gate.** How big is the qualified-visit population, how much of today's URL-scoped population survives, and can it carry daily detection? |
# MAGIC | `C18 evar105_vs_rules` | Does D11's `evar105` brand tag still add anything on top of the rules, or is it redundant? |
# MAGIC
# MAGIC **↺ 2026-08-04 — the Public Website scope is now defined by 16 SME link rules.** The SME sent
# MAGIC 8 named link-click rules × EN/FR, each an (`evar193` Link Name, `evar194` Link Href) pair, and
# MAGIC ratified them as the **scope** for the site-wide Public Website metrics — not as an extra
# MAGIC breakdown. That is the doc-16 **D10** event: scope moving off URL patterns is a single clean
# MAGIC re-baseline. `C13`–`C18` exist to price it before anything is built. Two facts make it sharp:
# MAGIC the pipeline ingests **zero eVars** today, so the predicate is not evaluable anywhere in the
# MAGIC lake; and 5 of the 8 rules point at `id.manulife.ca` / `portal.manulife.ca`, both on the D8
# MAGIC login-host exclusion list — which the pipeline matches against the *page* url, not the href.
# MAGIC
# MAGIC **What this probe canNOT settle** — deliberately out of reach of any query, and therefore
# MAGIC still SME questions: whether the `wealth-ca` / `pvt-wealth` brand variants belong to Canada
# MAGIC Retirement (C3 only sizes them — Q3b); whether "page views" means hits or Adobe page views,
# MAGIC which C12 and C17 both profile under every available basis but cannot choose between (Q6/Q14);
# MAGIC whether 1/0 in the table means in/out of scope; whether the 2026-07-20 login-exclusion rule
# MAGIC (D8) is superseded; the numerator/denominator of "Sign in % rate completion"; the
# MAGIC friendly-name → rsid mapping for "GRS+" if no candidate in C1 is recognisable; and whether the
# MAGIC "Manulife Financial" Adobe instance is the same feed (an Adobe-admin question). The business
# MAGIC definition of "non-marketing" **was** on this list and is now answered (CID) — what remains is
# MAGIC the mechanical question C11 asks. From the 2026-08-04 spec, also out of reach: whether EN and
# MAGIC FR should alert as one series or two (Q13); the true value of `signin_sponsor/en`, delivered
# MAGIC one character short of its structural twin (Q15); whether the FR sponsor/advisor hrefs
# MAGIC carrying an inner `ui_locales=en-CA` is a site bug (Q16); whether `find_advisor/fr` — which
# MAGIC carries a `cid=` and is therefore *marketing* by the SME's own Q5 rule — is excluded from its
# MAGIC own alert (Q17); whether the blank Link Name on the two app-download rules is intended (Q18);
# MAGIC and whether the `evar105` brand tag survives the new scope (Q19 — C18 only sizes it).
# MAGIC
# MAGIC **How to run.** Databricks → Workspace → Import → File → select this `.py` (it imports as a
# MAGIC notebook — the file is in Databricks "source" format). Attach to any cluster with Unity
# MAGIC Catalog access (DBR 13+). Run the **C0 config cell** once so the widgets appear, then
# MAGIC **Run All**. C1 is the one full-history scan but touches only `rsid` + the partition column;
# MAGIC everything else runs inside `window_days`.
# MAGIC
# MAGIC **What to paste back.** Every section prints a `===== BEGIN SHAREABLE: <id> =====` block —
# MAGIC copy those verbatim, including any `----- part i of N -----` continuations.
# MAGIC
# MAGIC ⚠ **Check the manifest.** The final `run_manifest` block carries a `skipped` map. A section
# MAGIC that throws prints `===== SKIPPED: <id> | <reason> =====` and the run *continues* — that is
# MAGIC how a missing section went unnoticed for days on CoverMe (doc-17 E1). `skipped` must be `{}`;
# MAGIC if it isn't, paste the SKIPPED lines back too. Top-N caps are kept deliberately small because
# MAGIC Databricks silently truncates large stdout payloads mid-JSON (doc-16 §0.5).
# MAGIC
# MAGIC **A clean run of this version prints 19 `BEGIN SHAREABLE` blocks and reports
# MAGIC `n_sections: 18`.** That is not an off-by-one bug: `run_manifest` counts `RESULTS` *before*
# MAGIC emitting itself, so the manifest total is always one less than the block count. The previous
# MAGIC 12-section version reported `n_sections: 12` against 13 blocks the same way. Checking
# MAGIC `n_sections == 19` will look like a section vanished when nothing did. What to actually
# MAGIC assert: `19` blocks · `n_sections: 18` · `skipped == {}` · `complete: true`.
# MAGIC
# MAGIC ⚠ **The 2026-07-30 export of this notebook no longer matches this source.** `C11`'s emitted
# MAGIC note was corrected on 2026-08-04 (it claimed the pipeline strips query strings, which is not
# MAGIC true of the code), so that section's sha1 has changed and six sections were added. Do not
# MAGIC verify the old `.html` against this file — re-run and export fresh.
# MAGIC
# MAGIC `scripts/decode_databricks_export.py` checks all four against an exported `.html`/`.ipynb`,
# MAGIC re-hashes every block against the manifest, and flags stdout truncation — run it on the export
# MAGIC rather than eyeballing the counts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C0 — Config, constants, helpers

# COMMAND ----------

import json
import math
import hashlib
import datetime
import traceback

from pyspark.sql import functions as F

# ---------------------------------------------------------------- widgets ----
dbutils.widgets.text("table_fqn", "gwam_prod_catalog.inv_typed_common.adobe_hit_data", "1. Table (catalog.schema.table)")
dbutils.widgets.text("window_days", "90", "2. Profiling window (days back from latest data)")
dbutils.widgets.text("top_n", "25", "3. Top-N cap for value lists")
dbutils.widgets.text("rsid_focus",
                     "manulifeglobalprod,manufingbrsmobileapp.prod,manucustomer.prod,manugrs",
                     "4. rsids for per-suite deep dives (comma-sep; unknown names are reported, not fatal)")
dbutils.widgets.text("sme_suite_hints", "grs,gbrs,mobileapp,manucustomer,mps,globalprod",
                     "5. Substrings to flag as candidate SME suites in C1")

TABLE_FQN   = dbutils.widgets.get("table_fqn").strip()
WINDOW_DAYS = int(dbutils.widgets.get("window_days"))
TOP_N       = int(dbutils.widgets.get("top_n"))


def _csv(widget):
    return [p.strip() for p in dbutils.widgets.get(widget).split(",") if p.strip()]


RSID_FOCUS = [r.lower() for r in _csv("rsid_focus")]
SUITE_HINTS = [h.lower() for h in _csv("sme_suite_hints")]

# The SME's scope table, encoded so the emitted payloads can be read against it directly.
# Values are the SME's own labels, verbatim -- resolving them to real rsids is C1's job.
# ↺ 2026-07-29: only public_website is in the alerting scope now. All four stay here so the
# census sections keep reporting per-suite evidence for the deferred registry entries.
SME_CHANNELS = {
    "public_website": {"instance": "Manulife", "suite_label": "Manulife Global Prod",
                       "segment": "Brand (evar105) = 'ca-retirement :  : GWAM'"},
    "web_member":     {"instance": "Manulife", "suite_label": "GRS+",
                       "segment": "Platform - v185 = 'MPS Member'"},
    "mobile":         {"instance": "Manulife Financial", "suite_label": "GBRS Mobile App - Production",
                       "segment": "Canada Retirement App Pages v2"},
    "manulifeid":     {"instance": "Manulife", "suite_label": "manucustomer.prod",
                       "segment": None},  # SME: "Not sure how to seperate Retirement from other ManulifeID signins"
}

# Today's shipped pipeline scope, for the C3 segment-vs-URL comparison (conf/settings.py).
PIPELINE_RSID = "manulifeglobalprod"
URL_SCOPE_BROAD = ["%/group-retirement%", "%/group-plans%", "%/regimes-collectifs%"]
URL_SCOPE_EXCLUDE = ["%adobeaemcloud.com%", "%/ph/%"]

# C15/C17 size the SME's link rules against every scope variant that is actually on the table,
# so the ingest decision is priced rather than argued:
#   EN_ONLY       what the pipeline shipped until 2026-08-04 (settings.SCOPE_URL_LIKE).
#   BROAD         settings.SCOPE_URL_LIKE_BROAD as it stood -- includes the %/group-plans%
#                 umbrella, which also pulls in group-benefits / business / advisor.
#   BROAD_NARROW  what 2026-08-04 actually ratified: French admitted, group-plans NOT. The
#                 delta between BROAD and BROAD_NARROW *is* the un-signed-off widening, so it
#                 has to be a measured number and not a paragraph.
URL_SCOPE_EN_ONLY = ["%manulife.com/ca/en/personal/group-plans/group-retirement%"]
URL_SCOPE_BROAD_NARROW = ["%/group-retirement%", "%/regimes-collectifs%"]

# settings.SCOPE_LOGIN_HOST_EXCLUDE (doc-16 D8). Mirrored here so C15 can measure the
# collision the new scope creates: the pipeline matches this against the PAGE url
# (01_bronze_ingest.py), but 5 of the 8 SME rules have hrefs ON these hosts. If evar194 ever
# enters a url coalesce, this list deletes exactly the rows the SME asked us to alert on.
LOGIN_HOST_EXCLUDE = [
    "%portal.manulife.ca%", "%id.manulife.ca%", "%grsmembers.manulife.com%",
    "%gsrs1.manulife.com%", "%viproom.manulife.com%", "%portail.manuvie.ca%",
]

# Candidate delimiters for the eVar105 "Brand | Line of Business | Segment" triple. The docs
# describe the SHAPE, not the separator; the SME wrote "ca-retirement :  : GWAM". C3 measures
# which of these actually splits the values, rather than assuming one.
DELIM_CANDIDATES = [" : ", ":", " | ", "|", " - ", ","]

# The SME's brand-tag examples, verbatim from the 2026-07-29 update. The first is the value we
# already know (a second form of it); the other two name lines of business that appear NOWHERE
# in our data or docs. C3 sizes all three so doc-20 Q3b -- are wealth-ca / pvt-wealth inside
# Canada Retirement or outside it? -- can be priced instead of guessed. The scope predicate
# stays a parts-match on (ca-retirement AND gwam) until she rules.
SME_BRAND_EXAMPLES = ["Manulife: GWAM: group-plans:ca-retirement",
                      "Manulife: GWAM: wealth-ca",
                      "Manulife: GWAM : pvt-wealth"]
BRAND_VARIANTS = ["wealth-ca", "pvt-wealth"]

# Q5 ANSWERED 2026-07-29: "marketing" = hits carrying the CID campaign identifier, the standard
# query-string parameter appended to marketing URLs. Applied to a LOWERCASED url; group 1 is the
# value. C11 uses this to test the rule against post_campaign, the column the pipeline actually
# ingests.
#
# ↺ CORRECTED 2026-08-04. This comment used to say "the pipeline strips query strings by policy
# (ADR-0007), so a production CID rule needs an ADR amendment". That is NOT true of the code.
# Bronze projects post_page_url and writes it VERBATIM (conf/bronze_columns.py bronze_select);
# nothing in bronze, silver or gold strips a query string. ADR-0007 governs identity
# pseudonymization, not URL truncation. What strips is the *EDA notebooks*:
# gwam_canada_retirement_eda.py S4b/S4c apply a hard-coded ^([^?#]*) and S9 has an opt-in
# strip_url_query widget (default false). So a production CID rule is a regexp_extract over a
# column we already carry -- a design decision about where the parse lives, not an ADR amendment.
# The real constraint is downstream: SILVER_COLUMNS drops post_page_url, so any URL-derived
# field has to be computed at bronze or silver, never later.
CID_REGEX = r"[?&]cid=([^&#]*)"

# ---------------------------------------------------------- SME link rules (2026-08-04) ----
# The Business SME's link-click scope for Canada Retirement, encoded verbatim so the emitted
# payloads can be read against it directly -- same contract as SME_CHANNELS above. 8 named
# rules x {en, fr} = 16 records, each an (evar193 link name, evar194 href) pair.
#
# ⚠ These rules are the SCOPE, not a breakdown. Ratified 2026-08-04: a visit is in Public
# Website scope if it contains >= 1 rule-matching link click (doc-16 D13). C17 prices that
# against today's URL-scoped population; nothing here changes the pipeline.
#
# Transcription notes -- what arrived vs what we believe is true:
#   * The FR link names arrived MOJIBAKED (UTF-8 read as Latin-1): "crÃ©ez-en", "adhÃ©rer".
#     Both forms are carried. C14 reports which one the data actually holds -- if the SME's
#     own file was mangled, the tag on the site may be too, and we must not guess.
#   * signin_sponsor/en arrived at 143 chars ending "...ui_locales=en-C". Its structural twin
#     signin_advisor/en is 144. One char short of its sibling matches no Adobe limit
#     (props cap at 100, eVars at 255), so this reads as a copy/paste artifact, not data
#     truncation -- Q15. href_prefix carries the delivered value; token matching ignores it.
#   * signin_sponsor/fr and signin_advisor/fr are percent-encoded (%3A%2F%2F) while their EN
#     twins are not, and both carry an INNER "ui_locales%3Den-CA" inside an outer fr-CA URL
#     (Q16 -- site bug or intended?).
#
# Measured collision facts (verified against these exact strings, not assumed):
#   * Query-stripping does not blur these rules, it MERGES them. Path-only (^[^?#]*) collapses
#     {signin_member, signin_join} x {en, fr} into ONE bucket, and collapses en/fr for
#     signup_join, app_apple and app_android. 16 hrefs collapse to 10 distinct paths -- and
#     sponsor/advisor survive purely on a trailing slash (EN "/sponsor" vs FR "/sponsor/"),
#     so even trailing-slash normalization would break them. NEVER route evar194 through the
#     profiler's ^([^?#]*) helpers (gwam_canada_retirement_eda.py S4b/S4c).
#   * signin_member/en and signin_join/en are identical for their first 101 characters and
#     diverge at 102 ("member/" vs "enrolment/"). At a 100-char cap they are the SAME VALUE:
#     15 distinct of 16. At 128 all 16 separate. C13 measures max(char_length) -- a max of
#     exactly 100 or 255 means Adobe truncation and rules 1/5 are unrecoverable.
#   * find_advisor/fr is 262 chars -- ALREADY OVER the 255-byte eVar limit. If evar194 is
#     255-capped, exact matching can never fire for it. Its token sits at the front, which is
#     the whole reason the token strategy exists.
#   * Two hrefs are strict prefixes of others: signin_member/fr of signin_join/fr, and
#     app_apple/en of app_apple/fr. Naive startswith() over-claims on both -- that is what
#     m_prefix is for in C14, and why it is reported alongside m_token rather than instead.
#
# Matching contract:
#   token / anti_token  -- matched on the LOWERCASED, percent-DECODED value. Chosen so
#                          (token AND NOT anti_token AND lang) resolves all 16 uniquely;
#                          verified, not assumed (see C14's collision matrix).
#   lang_token / lang_anti_token -- matched on the LOWERCASED **RAW** value. Deliberate: the
#                          sponsor/advisor FR hrefs carry "ui_locales%3Den-CA" internally, so
#                          decoding first would turn them into false EN matches.
SME_LINK_RULES = [
    {"rule_id": "signin_member", "rule_name": "Sign in - Member", "lang": "en",
     "link_name": "Sign in", "link_name_mojibake": None,
     "href": "https://id.manulife.ca/?ui_locales=en-CA&goto=https://portal.manulife.ca/apps/groupretirement/portal/member/handlelogin?ui_locales=en-CA",
     "token": "member/handlelogin", "anti_token": None,
     "lang_token": "ui_locales=en-ca", "lang_anti_token": None},
    {"rule_id": "signin_member", "rule_name": "Sign in - Member", "lang": "fr",
     "link_name": "Ouvrir une session", "link_name_mojibake": None,
     "href": "https://id.manulife.ca/?ui_locales=fr-CA",
     # anti "goto=" is load-bearing: this href is a strict PREFIX of signin_join/fr.
     "token": "id.manulife.ca/?ui_locales=fr-ca", "anti_token": "goto=",
     "lang_token": "ui_locales=fr-ca", "lang_anti_token": None},

    {"rule_id": "signin_sponsor", "rule_name": "Sign in - Sponsor", "lang": "en",
     "link_name": "Sign in", "link_name_mojibake": None,
     # As delivered: 143 chars, ends "en-C". Q15.
     "href": "https://id.manulife.ca/sponsor?ui_locales=en-CA&goto=https://portal.manulife.ca/apps/groupretirement/portal/sponsor/handlelogin?ui_locales=en-C",
     "token": "/sponsor", "anti_token": None,
     "lang_token": "ui_locales=en-ca", "lang_anti_token": None},
    {"rule_id": "signin_sponsor", "rule_name": "Sign in - Sponsor", "lang": "fr",
     "link_name": "Ouvrir une session", "link_name_mojibake": None,
     "href": "https://id.manulife.ca/sponsor/?ui_locales=fr-CA&goto=https%3A%2F%2Fportal.manulife.ca%2Fapps%2Fgroupretirement%2Fportal%2Fsponsor%2Fhandlelogin%3Fui_locales%3Den-CA",
     "token": "/sponsor", "anti_token": None,
     "lang_token": "ui_locales=fr-ca", "lang_anti_token": None},

    {"rule_id": "signin_advisor", "rule_name": "Sign in - Advisor", "lang": "en",
     "link_name": "Sign in", "link_name_mojibake": None,
     "href": "https://id.manulife.ca/advisor?ui_locales=en-CA&goto=https://portal.manulife.ca/apps/groupretirement/portal/advisor/handlelogin?ui_locales=en-CA",
     "token": "/advisor", "anti_token": None,
     "lang_token": "ui_locales=en-ca", "lang_anti_token": None},
    {"rule_id": "signin_advisor", "rule_name": "Sign in - Advisor", "lang": "fr",
     "link_name": "Ouvrir une session", "link_name_mojibake": None,
     "href": "https://id.manulife.ca/advisor/?ui_locales=fr-CA&goto=https%3A%2F%2Fportal.manulife.ca%2Fapps%2Fgroupretirement%2Fportal%2Fadvisor%2Fhandlelogin%3Fui_locales%3Den-CA",
     "token": "/advisor", "anti_token": None,
     "lang_token": "ui_locales=fr-ca", "lang_anti_token": None},

    {"rule_id": "signup_join", "rule_name": "Sign up to join", "lang": "en",
     "link_name": "set one up to join", "link_name_mojibake": None,
     "href": "https://id.manulife.ca/register?ui_locales=en-CA&goto=https://portal.manulife.ca/apps/groupretirement/portal/enrolment/handlelogin?ui_locales=en-CA",
     "token": "/register", "anti_token": None,
     "lang_token": "ui_locales=en-ca", "lang_anti_token": None},
    {"rule_id": "signup_join", "rule_name": "Sign up to join", "lang": "fr",
     "link_name": "créez-en un pour vous inscrire", "link_name_mojibake": "crÃ©ez-en un pour vous inscrire",
     "href": "https://id.manulife.ca/register?ui_locales=fr-CA&goto=https://portal.manulife.ca/apps/groupretirement/portal/enrolment/handlelogin?ui_locales=fr-CA",
     "token": "/register", "anti_token": None,
     "lang_token": "ui_locales=fr-ca", "lang_anti_token": None},

    {"rule_id": "signin_join", "rule_name": "Sign in to join", "lang": "en",
     "link_name": "Sign in to join", "link_name_mojibake": None,
     "href": "https://id.manulife.ca/?ui_locales=en-CA&goto=https://portal.manulife.ca/apps/groupretirement/portal/enrolment/handlelogin?ui_locales=en-CA",
     # anti "/register" is load-bearing: signup_join's href ALSO contains enrolment/handlelogin.
     "token": "enrolment/handlelogin", "anti_token": "/register",
     "lang_token": "ui_locales=en-ca", "lang_anti_token": None},
    {"rule_id": "signin_join", "rule_name": "Sign in to join", "lang": "fr",
     "link_name": "Ouvrir une session pour adhérer", "link_name_mojibake": "Ouvrir une session pour adhÃ©rer",
     "href": "https://id.manulife.ca/?ui_locales=fr-CA&goto=https://portal.manulife.ca/apps/groupretirement/portal/enrolment/handlelogin?ui_locales=fr-CA",
     "token": "enrolment/handlelogin", "anti_token": "/register",
     "lang_token": "ui_locales=fr-ca", "lang_anti_token": None},

    {"rule_id": "app_apple", "rule_name": "Apple App Download - Link Click", "lang": "en",
     "link_name": "", "link_name_mojibake": None,      # blank in BOTH languages -- Q18
     "href": "https://apps.apple.com/ca/app/manulife-mobile/id1214009312",
     "token": "apps.apple.com/ca/app/manulife-mobile", "anti_token": None,
     # EN is the ABSENCE of the FR marker -- this href is a strict prefix of the FR one.
     "lang_token": None, "lang_anti_token": "?l=fr"},
    {"rule_id": "app_apple", "rule_name": "Apple App Download - Link Click", "lang": "fr",
     "link_name": "", "link_name_mojibake": None,
     "href": "https://apps.apple.com/ca/app/manulife-mobile/id1214009312?l=fr",
     "token": "apps.apple.com/ca/app/manulife-mobile", "anti_token": None,
     "lang_token": "?l=fr", "lang_anti_token": None},

    {"rule_id": "app_android", "rule_name": "Android App Download - Link Click", "lang": "en",
     "link_name": "", "link_name_mojibake": None,
     "href": "https://play.google.com/store/apps/details?id=ca.manulife.MobileGBRS&hl=en",
     "token": "play.google.com/store/apps/details", "anti_token": None,
     "lang_token": "hl=en", "lang_anti_token": None},
    {"rule_id": "app_android", "rule_name": "Android App Download - Link Click", "lang": "fr",
     "link_name": "", "link_name_mojibake": None,
     "href": "https://play.google.com/store/apps/details?id=ca.manulife.MobileGBRS&hl=fr",
     "token": "play.google.com/store/apps/details", "anti_token": None,
     "lang_token": "hl=fr", "lang_anti_token": None},

    {"rule_id": "find_advisor", "rule_name": "Find an Advisor", "lang": "en",
     "link_name": "Get started", "link_name_mojibake": None,
     "href": "https://www.manulife.ca/page/groupsavings-talk-to-an-advisor.html?",
     "token": "groupsavings-talk-to-an-advisor", "anti_token": None,
     "lang_token": "manulife.ca/page/groupsavings", "lang_anti_token": None},
    {"rule_id": "find_advisor", "rule_name": "Find an Advisor", "lang": "fr",
     "link_name": "Lancez-vous", "link_name_mojibake": None,
     # 262 chars -- over the 255-byte eVar limit. Exact match may be impossible; token is at
     # the front so token match survives. Carries cid= => marketing by the SME's own Q5 rule (Q17).
     "href": "https://www.manuvie.ca/page/solutionsepargne-parler-a-un-conseiller.html?cid=CA-FR_ML_RE_IR_RetirementWebsite_PRLandingPage_PlanRight________&utm_source=RetirementWebsite&utm_medium=IR&utm_campaign=_PRLandingPage&utm_content=_&utm_term=ML_RE_CA-FR_PlanRight_____",
     "token": "solutionsepargne-parler-a-un-conseiller", "anti_token": None,
     "lang_token": "manuvie.ca", "lang_anti_token": None},
]

# Derived once so every section shares one vocabulary. key = "<rule_id>_<lang>".
def rule_key(r):
    return f"{r['rule_id']}_{r['lang']}"


RULE_KEYS = [rule_key(r) for r in SME_LINK_RULES]
RULE_IDS = list(dict.fromkeys(r["rule_id"] for r in SME_LINK_RULES))

# Percent sequences that actually occur in the SME's hrefs. NOT a general decoder -- Spark has
# no unquote() and none exists in this repo. C13 reports pct_encoded_rows so a reader can see
# how much of the real data this shortcut covers.
PCT_DECODE = [("%3a", ":"), ("%2f", "/"), ("%3f", "?"), ("%3d", "=")]

RESULTS = {}
SKIPPED = {}
MAX_EMIT_STR = 2000


def _scrub_str(s):
    if len(s) > MAX_EMIT_STR:
        s = s[:MAX_EMIT_STR] + "...<trunc>"
    return s


def _scrub(obj):
    """Display hygiene only -- truncate over-long strings, round floats. No redaction
    (ADR-0007 §5 full-raw revision 2026-07-23)."""
    if isinstance(obj, dict):
        return {(_scrub_str(k) if isinstance(k, str) else k): _scrub(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_scrub(v) for v in obj]
    if isinstance(obj, str):
        return _scrub_str(obj)
    if isinstance(obj, float):
        return round(obj, 6) if math.isfinite(obj) else None
    return obj


def emit(section_id, payload):
    payload = _scrub(payload)
    RESULTS[section_id] = payload
    body = json.dumps(payload, separators=(",", ":"), default=str)
    print(f"===== BEGIN SHAREABLE: {section_id} =====")
    if len(body) <= 48000:
        print(body)
    else:
        n_parts = math.ceil(len(body) / 40000)
        for i in range(n_parts):
            print(f"----- part {i+1} of {n_parts} (concatenate parts to reassemble) -----")
            print(body[i * 40000:(i + 1) * 40000])
    print(f"===== END SHAREABLE: {section_id} =====")


def run_section(section_id, fn):
    print(f"\n>>> running {section_id} ...")
    t0 = datetime.datetime.now()
    try:
        fn()
        print(f">>> {section_id} done in {(datetime.datetime.now() - t0).total_seconds():.0f}s")
    except Exception as e:
        reason = f"{type(e).__name__}: {str(e)[:300]}"
        SKIPPED[section_id] = reason
        print(f"===== SKIPPED: {section_id} | {reason} =====")
        traceback.print_exc()


# ------------------------------------------------------------ data helpers ----
def qcol(name):
    """F.col with backtick quoting -- the schema has dotted column names
    (mobileappperformanceappid.*) that unquoted F.col parses as struct access."""
    return F.col("`" + name.replace("`", "``") + "`")


def nonblank(name):
    """Adobe feeds write empty strings, not NULLs."""
    c = qcol(name)
    return c.isNotNull() & (F.trim(c.cast("string")) != "")


def sql_col(name):
    return "`" + name.replace("`", "``") + "`"


def sql_regex_literal(delim):
    """A literal delimiter, as a SQL string holding a regex.

    Two escapes, both required. Spark's split() takes a REGEX, not a literal, so an
    unescaped '|' would mean alternation and split on nothing; then the SQL string literal
    itself processes backslashes, so each one has to survive as a pair."""
    escaped = "".join("\\" + ch if ch in r".^$*+?()[]{}|\\" else ch for ch in delim)
    return escaped.replace("\\", "\\\\").replace("'", "\\'")


def split_part(sql_expr, delim, idx):
    """try_element_at(split(expr, delim), idx), built as SQL.

    Two deliberate choices. (1) try_element_at, never element_at: Databricks runs ANSI mode
    and a short split throws ArrayIndexOutOfBounds mid-section -- the CoverMe E1 defect that
    silently killed a whole section. (2) via F.expr rather than F.try_element_at, because the
    Python binding only exists in PySpark 3.5+ and these notebooks target DBR 13+; this is the
    form the other EDA scripts already use."""
    return F.expr(f"try_element_at(split({sql_expr}, '{sql_regex_literal(delim)}'), {idx})")


base = spark.table(TABLE_FQN)
AVAILABLE = {c.lower() for c in base.columns}


def have(name):
    return name.lower() in AVAILABLE


def pick(*names):
    """First column that actually exists. Adobe feeds carry both `evarN` and `post_evarN`
    and which one is populated varies by suite -- never assume."""
    for n in names:
        if have(n):
            return n
    return None


def evar(n):
    """Prefer post_ (the processed value Workspace reports on), fall back to raw."""
    return pick(f"post_evar{n}", f"evar{n}")


def nonblank_rate(name):
    return F.avg(F.when(nonblank(name), F.lit(1.0)).otherwise(F.lit(0.0)))


def top_values(df, name, n=None, extra_filter=None):
    """Top-N non-blank values of one column with counts. Returns [] if the column is absent."""
    if not name or not have(name):
        return []
    n = n or TOP_N
    d = df.filter(nonblank(name))
    if extra_filter is not None:
        d = d.filter(extra_filter)
    rows = (d.groupBy(qcol(name).cast("string").alias("value"))
             .count().orderBy(F.desc("count")).limit(n).collect())
    return [{"value": r["value"], "count": int(r["count"])} for r in rows]


def per_rsid_rates(df, cols):
    """Per-rsid non-blank rate + row count for a set of columns. Absent columns are
    reported as null rather than dropped, so the payload documents what the feed lacks."""
    present = [(label, name) for label, name in cols if name and have(name)]
    aggs = [F.count(F.lit(1)).alias("rows")]
    aggs += [nonblank_rate(name).alias(label) for label, name in present]
    rows = df.groupBy(F.col("rsid").alias("rsid")).agg(*aggs).orderBy(F.desc("rows")).collect()
    present_labels = {label for label, _ in present}
    out = []
    for r in rows:
        rec = {"rsid": r["rsid"], "rows": int(r["rows"])}
        for label, _ in cols:
            rec[label] = float(r[label]) if label in present_labels and r[label] is not None else None
        out.append(rec)
    return out


def like_any(col, patterns):
    expr = F.lit(False)
    for p in patterns:
        expr = expr | col.like(p.lower())
    # NULL col makes col.like(...) NULL and False|NULL stays NULL; coalesce to False
    # so ~like_any(...) is well-defined for rows with no URL (e.g. mobile-app hits).
    return F.coalesce(expr, F.lit(False))


# ---- window frame -------------------------------------------------------------
# Derived from the data's own latest partition, so the probe needs no hardcoded dates.
_max_pd = base.agg(F.max("process_date").alias("m")).collect()[0]["m"]
MAX_DATE = str(_max_pd)[:10]
START_DATE = (datetime.date.fromisoformat(MAX_DATE) - datetime.timedelta(days=WINDOW_DAYS)).isoformat()
WIN = base.filter((F.col("process_date") >= F.lit(START_DATE)) & (F.col("process_date") <= F.lit(MAX_DATE)))

EVAR105, EVAR185, EVAR110 = evar(105), evar(185), evar(110)
EVAR103 = evar(103)
ERROR_EVARS = {f"evar{n}": evar(n) for n in (181, 182, 183, 184)}
SIGNIN_EVARS = {f"evar{n}": evar(n) for n in (122, 135)}

# The SME's link-rule dimensions (2026-08-04). evar() prefers post_ over raw; the 2026-07-02
# column census found post_evar193 / post_evar194 populated but the bare evar193 / evar194 NOT,
# while evar101 and post_evar101 are BOTH populated -- so which one pick() lands on is itself a
# finding, and C13 reports the non-blank rate of every candidate rather than trusting this.
EVAR101, EVAR193, EVAR194 = evar(101), evar(193), evar(194)

print(json.dumps({
    "table": TABLE_FQN, "n_columns": len(base.columns),
    "max_process_date": MAX_DATE, "window": [START_DATE, MAX_DATE], "window_days": WINDOW_DAYS,
    "resolved_columns": {
        "evar105": EVAR105, "evar185": EVAR185, "evar110": EVAR110, "evar103": EVAR103,
        "evar101": EVAR101, "evar193": EVAR193, "evar194": EVAR194,
        "error_evars": ERROR_EVARS, "signin_evars": SIGNIN_EVARS,
        "page_url": have("page_url"), "post_page_url": have("post_page_url"),
        "mobileappid": have("mobileappid"), "post_event_list": have("post_event_list"),
    },
}, indent=2, default=str))


# ---- link-rule matching -------------------------------------------------------
# Shared by C14-C17 so every section scopes on exactly the same predicate. Two forms of the
# href are needed and they are NOT interchangeable:
#   href_raw  lowercased only. Language derivation MUST use this -- the sponsor/advisor FR
#             hrefs carry an inner "ui_locales%3Den-CA", so decoding first would read them
#             as English.
#   href_dec  lowercased + the four percent sequences that occur in the spec. Token matching
#             uses this so an encoded "%2Fhandlelogin" still matches "/handlelogin".
def href_raw():
    return F.lower(qcol(EVAR194).cast("string")) if EVAR194 else F.lit(None).cast("string")


def href_dec():
    e = href_raw()
    for enc, dec in PCT_DECODE:
        e = F.regexp_replace(e, enc, dec)
    return e


def rule_match_expr(r, raw=None, dec=None):
    """token AND NOT anti_token (decoded) AND lang_token AND NOT lang_anti_token (raw).

    Verified to resolve all 16 (rule x lang) records uniquely against the SME's own strings.
    C14 re-measures that against real data instead of trusting it -- a value the feed stores
    differently (truncated, re-encoded, redirected) can still collide."""
    raw = href_raw() if raw is None else raw
    dec = href_dec() if dec is None else dec
    e = dec.contains(r["token"])
    if r["anti_token"]:
        e = e & ~dec.contains(r["anti_token"])
    if r["lang_token"]:
        e = e & raw.contains(r["lang_token"])
    if r["lang_anti_token"]:
        e = e & ~raw.contains(r["lang_anti_token"])
    return F.coalesce(e, F.lit(False))


def any_rule_expr(raw=None, dec=None):
    """True if the hit matches ANY of the 16 rules -- the qualifying-click predicate that
    doc-16 D13 defines the Public Website scope with."""
    raw = href_raw() if raw is None else raw
    dec = href_dec() if dec is None else dec
    e = F.lit(False)
    for r in SME_LINK_RULES:
        e = e | rule_match_expr(r, raw, dec)
    return e


VISIT_KEY_COLS = ["post_visid_high", "post_visid_low", "visit_num"]


def visit_key_expr():
    """gold's visit grain (gold_lib._key_expr with null_safe_keys=False): a plain concat_ws
    over the 3-part Adobe key. Kept identical so C17's numbers are comparable to gold's."""
    return F.concat_ws(":", *[qcol(c).cast("string") for c in VISIT_KEY_COLS])

# COMMAND ----------

# MAGIC %md
# MAGIC ## C1 — rsid census (full history, unfiltered)
# MAGIC
# MAGIC Settles: do the SME's three unknown report suites exist in this table, and does each carry
# MAGIC enough history to fit a baseline (the standing ≥30-day / ideally ≥90-day gate)? Full-history
# MAGIC scan, but it touches only `rsid` and the partition column.

# COMMAND ----------

def c1_rsid_census():
    rows = (base.groupBy(F.col("rsid").alias("rsid"))
                .agg(F.count(F.lit(1)).alias("rows"),
                     F.min("process_date").alias("first_day"),
                     F.max("process_date").alias("last_day"),
                     F.countDistinct("process_date").alias("n_days"))
                .orderBy(F.desc("rows")).collect())
    total = sum(int(r["rows"]) for r in rows) or 1
    census = []
    for r in rows:
        rsid = (r["rsid"] or "")
        hints = [h for h in SUITE_HINTS if h in rsid.lower()]
        census.append({
            "rsid": rsid, "rows": int(r["rows"]),
            "share_pct": round(100.0 * int(r["rows"]) / total, 4),
            "first_day": str(r["first_day"])[:10], "last_day": str(r["last_day"])[:10],
            "n_days": int(r["n_days"]),
            "clears_30d_gate": int(r["n_days"]) >= 30,
            "clears_90d_gate": int(r["n_days"]) >= 90,
            "sme_name_hints": hints,
        })
    found = {r["rsid"] for r in census}
    emit("rsid_census", {
        "n_rsids": len(census), "total_rows": total, "census": census,
        "sme_channel_lookup": {
            k: {"suite_label": v["suite_label"],
                "exact_rsid_present": v["suite_label"] in found,
                "hint_matches": [c["rsid"] for c in census
                                 if any(h in c["rsid"].lower()
                                        for h in SUITE_HINTS if h in v["suite_label"].lower().replace(" ", ""))][:10]}
            for k, v in SME_CHANNELS.items()},
        "focus_rsids_present": {r: (r in {f.lower() for f in found}) for r in RSID_FOCUS},
        "note": ("An rsid absent here is NOT proof the suite does not exist -- it may sit in a "
                 "different Adobe instance/feed. That distinction is an Adobe-admin question, "
                 "not a data one."),
    })


run_section("rsid_census", c1_rsid_census)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C2 — web vs app discriminator, per rsid
# MAGIC
# MAGIC Settles: which suites carry page URLs at all. Every scope predicate in the pipeline is a SQL
# MAGIC `LIKE` on a URL column, so a suite with no URL **cannot be scoped the way we scope today** —
# MAGIC this is the query that proves the Mobile channel needs a different mechanism.

# COMMAND ----------

def c2_web_vs_app():
    url_cols = [("page_url", pick("page_url")), ("post_page_url", pick("post_page_url")),
                ("post_pagename", pick("post_pagename")), ("pagename", pick("pagename"))]
    app_cols = [("mobileappid", pick("mobileappid")),
                ("mobiledayssincelastuse", pick("mobiledayssincelastuse")),
                ("mobileinstalldate", pick("mobileinstalldate")),
                ("mobileosversion", pick("mobileosversion"))]
    rates = per_rsid_rates(WIN, url_cols + app_cols)

    # The single discriminator that matters: does a usable URL exist after the D4 coalesce?
    have_url = [n for _, n in url_cols[:2] if n]
    if have_url:
        coalesced = F.coalesce(*[F.when(nonblank(n), qcol(n)) for n in have_url])
        url_rate = (WIN.groupBy(F.col("rsid").alias("rsid"))
                       .agg(F.avg(F.when(coalesced.isNotNull(), F.lit(1.0)).otherwise(F.lit(0.0)))
                             .alias("coalesced_url_rate")).collect())
        url_map = {r["rsid"]: float(r["coalesced_url_rate"]) for r in url_rate}
    else:
        url_map = {}

    for rec in rates:
        rec["coalesced_url_rate"] = url_map.get(rec["rsid"])
        r = rec["coalesced_url_rate"]
        rec["verdict"] = None if r is None else ("web" if r >= 0.90 else "app_or_mixed" if r <= 0.10 else "mixed")

    emit("web_vs_app", {
        "window": [START_DATE, MAX_DATE], "per_rsid": rates,
        "note": ("verdict=app_or_mixed means a URL LIKE filter cannot express that channel's scope. "
                 "Null rate = column absent from the feed, not zero population."),
    })


run_section("web_vs_app", c2_web_vs_app)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C3 — eVar105 census, delimiter, and segment-scope vs URL-scope sizing
# MAGIC
# MAGIC The highest-value section. Settles: (a) is `ca-retirement :  : GWAM` a literal value; (b) what
# MAGIC the real delimiter is, measured rather than assumed; (c) **how the segment-scoped population
# MAGIC compares to the population the pipeline ingests today** — i.e. the size of the re-baseline
# MAGIC that switching scope models would cause.

# COMMAND ----------

def c3_evar105_census():
    if not EVAR105:
        raise ValueError("no evar105/post_evar105 column in this feed")

    col = qcol(EVAR105).cast("string")
    scoped = WIN.filter(nonblank(EVAR105))

    # (b) measure the delimiter instead of trusting the doc's "Brand | LoB | Segment" shorthand.
    src = sql_col(EVAR105)
    delim_probe = {}
    for d in DELIM_CANDIDATES:
        n_parts = F.expr(f"size(split({src}, '{sql_regex_literal(d)}'))")
        row = scoped.agg(
            F.avg(n_parts.cast("double")).alias("avg_parts"),
            F.avg(F.when(n_parts == 3, F.lit(1.0)).otherwise(F.lit(0.0))).alias("pct_exactly_3"),
        ).collect()[0]
        delim_probe[d] = {"avg_parts": float(row["avg_parts"] or 0.0),
                          "pct_exactly_3": float(row["pct_exactly_3"] or 0.0)}
    best = max(delim_probe, key=lambda d: (delim_probe[d]["pct_exactly_3"], delim_probe[d]["avg_parts"]))

    brand = F.trim(split_part(src, best, 1))
    lob = F.trim(split_part(src, best, 2))
    segment = F.trim(split_part(src, best, 3))

    triples = (scoped.groupBy(F.col("rsid").alias("rsid"), brand.alias("brand"),
                              lob.alias("lob"), segment.alias("segment"))
                     .count().orderBy(F.desc("count")).limit(TOP_N * 4).collect())

    # (c) the re-baseline sizing: segment-scope vs today's shipped URL-scope, same window.
    is_ca_ret = F.lower(col).contains("ca-retirement")
    is_gwam = F.lower(col).contains("gwam")
    # (d) Q3b sizing for the 2026-07-29 brand variants. Substring counts only -- these are NOT
    # part of the predicate. If a variant overlaps ca-retirement it is already counted above;
    # an overlap near zero means adopting it would ADD that much traffic, not re-label it.
    variant_flags = {v: F.lower(col).contains(v) for v in BRAND_VARIANTS}
    url_cols = [n for n in (pick("page_url"), pick("post_page_url")) if n]
    if url_cols:
        urlc = F.lower(F.coalesce(*[F.when(nonblank(n), qcol(n)) for n in url_cols], F.lit("")))
        in_url_scope = like_any(urlc, URL_SCOPE_BROAD) & ~like_any(urlc, URL_SCOPE_EXCLUDE)
    else:
        in_url_scope = F.lit(False)

    variant_aggs = []
    for v in BRAND_VARIANTS:
        vf = variant_flags[v]
        key = v.replace("-", "_")
        variant_aggs.append(F.sum(F.when(vf & is_gwam, 1).otherwise(0)).alias(f"{key}_rows"))
        variant_aggs.append(
            F.sum(F.when(vf & is_ca_ret, 1).otherwise(0)).alias(f"{key}_x_ca_ret"))

    sizing = (WIN.filter(F.col("rsid") == F.lit(PIPELINE_RSID))
                 .agg(F.count(F.lit(1)).alias("rows"),
                      F.sum(F.when(is_ca_ret & is_gwam, 1).otherwise(0)).alias("segment_scope"),
                      F.sum(F.when(in_url_scope, 1).otherwise(0)).alias("url_scope_broad"),
                      F.sum(F.when(is_ca_ret & is_gwam & in_url_scope, 1).otherwise(0)).alias("both"),
                      F.sum(F.when(is_ca_ret & is_gwam & ~in_url_scope, 1).otherwise(0)).alias("segment_only"),
                      F.sum(F.when(~(is_ca_ret & is_gwam) & in_url_scope, 1).otherwise(0)).alias("url_only"),
                      *variant_aggs,
                      )).collect()[0]

    per_rsid_match = (WIN.groupBy(F.col("rsid").alias("rsid"))
                         .agg(F.count(F.lit(1)).alias("rows"),
                              nonblank_rate(EVAR105).alias("evar105_populated"),
                              F.avg(F.when(is_ca_ret & is_gwam, 1.0).otherwise(0.0)).alias("ca_ret_gwam_rate"))
                         .orderBy(F.desc("rows")).limit(TOP_N).collect())

    emit("evar105_census", {
        "column": EVAR105, "window": [START_DATE, MAX_DATE],
        "sme_claimed_value": "ca-retirement :  : GWAM",
        "sme_brand_examples": SME_BRAND_EXAMPLES,
        "delimiter_probe": delim_probe, "chosen_delimiter": best,
        "top_values_all_suites": top_values(WIN, EVAR105),
        "top_triples": [{"rsid": r["rsid"], "brand": r["brand"], "lob": r["lob"],
                         "segment": r["segment"], "count": int(r["count"])} for r in triples],
        "per_rsid": [{"rsid": r["rsid"], "rows": int(r["rows"]),
                      "evar105_populated": float(r["evar105_populated"] or 0.0),
                      "ca_ret_gwam_rate": float(r["ca_ret_gwam_rate"] or 0.0)} for r in per_rsid_match],
        "scope_sizing_on_pipeline_rsid": {
            "rsid": PIPELINE_RSID, "window_rows": int(sizing["rows"]),
            "segment_scope_rows": int(sizing["segment_scope"]),
            "url_scope_broad_rows": int(sizing["url_scope_broad"]),
            "in_both": int(sizing["both"]),
            "segment_only": int(sizing["segment_only"]),
            "url_only": int(sizing["url_only"]),
            "note": ("segment_only = traffic a segment-scoped pipeline would GAIN; url_only = traffic "
                     "it would LOSE. Both are re-baseline magnitude: conf/settings.py:25-31 requires a "
                     "full mode=backfill with gold truncated for any scope change. ↺ 2026-07-29: with "
                     "the other three channels deferred, this trade IS the whole segment-vs-URL "
                     "decision -- the 'URL cannot express mobile' argument no longer applies."),
        },
        "brand_variant_sizing": {
            "rsid": PIPELINE_RSID, "window_rows": int(sizing["rows"]),
            "ca_retirement_rows": int(sizing["segment_scope"]),
            "variants": {v: {"rows_with_gwam": int(sizing[f'{v.replace("-", "_")}_rows']),
                             "overlap_with_ca_retirement": int(sizing[f'{v.replace("-", "_")}_x_ca_ret'])}
                         for v in BRAND_VARIANTS},
            "note": ("Sizes doc-20 Q3b, it does not answer it: are wealth-ca / pvt-wealth inside "
                     "Canada Retirement? rows_with_gwam is what including a variant would add; a "
                     "near-zero overlap_with_ca_retirement confirms the variant is additive rather "
                     "than already counted. The predicate stays a parts-match on (ca-retirement AND "
                     "gwam) until the SME rules."),
        },
    })


run_section("evar105_census", c3_evar105_census)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C4 — Platform census: eVar185 vs eVar110
# MAGIC
# MAGIC Settles the Web Member predicate AND a long-open spec conflict: doc-16 §3 lists **both**
# MAGIC eVar110 and eVar185 as "Platform" and flags the collision as unresolved (doc-15 §8 Q14). The
# MAGIC SME's "v185 = MPS Member" is Adobe shorthand for eVar185. Whichever column actually carries
# MAGIC `MPS Member` is the field of record.

# COMMAND ----------

def c4_platform_census():
    out = {"window": [START_DATE, MAX_DATE], "columns": {"evar185": EVAR185, "evar110": EVAR110},
           "sme_claimed_value": "MPS Member"}
    for label, name in (("evar185", EVAR185), ("evar110", EVAR110)):
        if not name:
            out[label] = {"present": False}
            continue
        mps = F.lower(qcol(name).cast("string")).contains("mps")
        rows = (WIN.groupBy(F.col("rsid").alias("rsid"))
                   .agg(F.count(F.lit(1)).alias("rows"),
                        nonblank_rate(name).alias("populated"),
                        F.avg(F.when(mps, 1.0).otherwise(0.0)).alias("mps_rate"))
                   .orderBy(F.desc("rows")).limit(TOP_N).collect())
        out[label] = {
            "present": True, "column": name,
            "top_values_all_suites": top_values(WIN, name),
            "per_rsid": [{"rsid": r["rsid"], "rows": int(r["rows"]),
                          "populated": float(r["populated"] or 0.0),
                          "mps_rate": float(r["mps_rate"] or 0.0)} for r in rows],
        }
    emit("platform_census", out)


run_section("platform_census", c4_platform_census)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C5 — Error fields
# MAGIC
# MAGIC Settles whether an Errors metric is implementable at all. The repo has NO error metric; the
# MAGIC EDDL dictionary labels eVar181-184 as Error Code / Description / Type / Category and lists
# MAGIC `event173`, but nothing has ever been profiled or built.

# COMMAND ----------

def c5_error_fields():
    cols = [(k, v) for k, v in ERROR_EVARS.items()]
    out = {"window": [START_DATE, MAX_DATE], "columns": ERROR_EVARS,
           "per_rsid_population": per_rsid_rates(WIN, cols),
           "top_values_all_suites": {k: top_values(WIN, v) for k, v in ERROR_EVARS.items() if v}}
    if have("post_event_list"):
        ev = F.col("post_event_list").cast("string")
        has173 = (ev == F.lit("173")) | ev.startswith("173,") | ev.contains(",173,") | ev.endswith(",173")
        rows = (WIN.groupBy(F.col("rsid").alias("rsid"))
                   .agg(F.count(F.lit(1)).alias("rows"),
                        F.avg(F.when(has173, 1.0).otherwise(0.0)).alias("event173_rate"))
                   .orderBy(F.desc("rows")).limit(TOP_N).collect())
        out["event173"] = [{"rsid": r["rsid"], "rows": int(r["rows"]),
                            "event173_rate": float(r["event173_rate"] or 0.0)} for r in rows]
    emit("error_fields", out)


run_section("error_fields", c5_error_fields)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C6 — Sign-in fields
# MAGIC
# MAGIC Settles whether a "Sign in % rate completion" is buildable. The metric ENGINE already
# MAGIC supports it — `gold_lib.build_kpis_spark` has a `ratio` kind that divides one metric by
# MAGIC another (CoverMe's funnel uses it). What is missing is the inputs: does anything mark a
# MAGIC sign-in attempt vs a sign-in success?

# COMMAND ----------

def c6_signin_fields():
    cols = [(k, v) for k, v in SIGNIN_EVARS.items()]
    focus = WIN.filter(F.lower(F.col("rsid")).isin(RSID_FOCUS)) if RSID_FOCUS else WIN
    emit("signin_fields", {
        "window": [START_DATE, MAX_DATE], "columns": SIGNIN_EVARS,
        "per_rsid_population": per_rsid_rates(WIN, cols),
        "top_values_all_suites": {k: top_values(WIN, v) for k, v in SIGNIN_EVARS.items() if v},
        "top_values_focus_suites": {k: top_values(focus, v) for k, v in SIGNIN_EVARS.items() if v},
        "focus_rsids": RSID_FOCUS,
        "note": ("A usable completion ratio needs two distinguishable states (attempt vs success). "
                 "If evar122 'Login Step' carries ordered step values, those are the numerator and "
                 "denominator; if it does not, the ratio has to come from events (see event_census)."),
    })


run_section("signin_fields", c6_signin_fields)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C7 — post_event_list id census, per rsid
# MAGIC
# MAGIC GWAM's 23 tracked event ids are all unresolved, and the registry's own `report_suite_caveat`
# MAGIC warns that CoverMe's ids do not transfer. The three new suites' event spaces are wholly
# MAGIC unknown — Errors and Sign-in may well be events rather than eVars.

# COMMAND ----------

def c7_event_census():
    if not have("post_event_list"):
        raise ValueError("no post_event_list column in this feed")
    ev = F.explode(F.split(F.col("post_event_list").cast("string"), ",")).alias("tok")
    exploded = (WIN.filter(nonblank("post_event_list"))
                   .select(F.col("rsid"), ev))
    # Adobe serializes as `id` or `id=value`; split_part keeps a malformed token from
    # throwing under ANSI mode.
    eid = F.trim(split_part("tok", "=", 1))
    per_rsid = (exploded.select("rsid", eid.alias("event_id"))
                        .filter(F.col("event_id").isNotNull() & (F.col("event_id") != ""))
                        .groupBy("rsid", "event_id").count()
                        .orderBy(F.desc("count")).limit(TOP_N * 12).collect())
    grouped = {}
    for r in per_rsid:
        grouped.setdefault(r["rsid"], []).append({"event_id": r["event_id"], "count": int(r["count"])})
    emit("event_census", {
        "window": [START_DATE, MAX_DATE],
        "per_rsid_top_events": {k: v[:TOP_N] for k, v in grouped.items()},
        "n_rsids_with_events": len(grouped),
        "note": "Ids only -- this feed has no event dictionary. Labels must come from the EDDL workbook or the SME.",
    })


run_section("event_census", c7_event_census)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C8 — Pagename census on the focus suites
# MAGIC
# MAGIC Settles whether "Canada Retirement App Pages v2" (the Mobile channel's segment) is
# MAGIC translatable into a pagename predicate we can implement, or whether we need the Adobe
# MAGIC segment definition from the SME.

# COMMAND ----------

def c8_pagename_census():
    name = pick("post_pagename", "pagename")
    if not name:
        raise ValueError("no post_pagename/pagename column in this feed")
    out = {"window": [START_DATE, MAX_DATE], "column": name, "per_rsid": {}}
    prefix = F.trim(split_part(f"cast({sql_col(name)} as string)", ":", 1))
    for rsid in RSID_FOCUS:
        d = WIN.filter(F.lower(F.col("rsid")) == F.lit(rsid))
        pref = (d.filter(nonblank(name)).groupBy(prefix.alias("prefix"))
                 .count().orderBy(F.desc("count")).limit(TOP_N).collect())
        out["per_rsid"][rsid] = {
            "top_pagenames": top_values(d, name),
            "top_prefixes": [{"prefix": r["prefix"], "count": int(r["count"])} for r in pref],
            "retirement_like": top_values(
                d, name, extra_filter=(F.lower(qcol(name).cast("string")).contains("retire")
                                       | F.lower(qcol(name).cast("string")).contains("ret:")
                                       | F.lower(qcol(name).cast("string")).contains("retraite"))),
        }
    emit("pagename_census", out)


run_section("pagename_census", c8_pagename_census)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C9 — Can retirement be separated from other ManulifeID sign-ins?
# MAGIC
# MAGIC This is the SME's OWN flagged unknown ("Not sure how to seperate Retirement from other
# MAGIC ManulifeID signins"). We answer it from data if we can, and confirm it as genuinely blocked
# MAGIC if we cannot. Every candidate discriminator gets profiled on the ManulifeID suite.

# COMMAND ----------

def c9_manulifeid_split():
    target = SME_CHANNELS["manulifeid"]["suite_label"]  # "manucustomer.prod"
    d = WIN.filter(F.lower(F.col("rsid")) == F.lit(target.lower()))
    n = d.count()
    out = {"window": [START_DATE, MAX_DATE], "rsid": target, "rows_in_window": n}
    if n == 0:
        out["verdict"] = ("rsid not present in this table in this window -- the ManulifeID channel "
                          "cannot be profiled here at all. This is a data-ACCESS question for the SME "
                          "/ Adobe admin, not a modelling one.")
        emit("manulifeid_split", out)
        return

    candidates = {"evar105_brand": EVAR105, "evar185_platform": EVAR185,
                  "evar110_platform": EVAR110, "evar103_site_type": EVAR103}
    out["candidate_discriminators"] = {
        label: {"column": name, "top_values": top_values(d, name)} if name else {"present": False}
        for label, name in candidates.items()}
    out["candidate_population"] = per_rsid_rates(d, list(candidates.items()))

    for label, name in (("referrer", pick("referrer", "post_referrer")),
                        ("page_url", pick("page_url", "post_page_url"))):
        if name:
            host = split_part(
                f"regexp_replace(lower(cast({sql_col(name)} as string)), '^https?://', '')", "/", 1)
            rows = (d.filter(nonblank(name)).groupBy(host.alias("host"))
                     .count().orderBy(F.desc("count")).limit(TOP_N).collect())
            out[f"top_{label}_hosts"] = [{"host": r["host"], "count": int(r["count"])} for r in rows]

    out["note"] = ("If no field above isolates a retirement subset, the honest answer to the SME is "
                   "that this channel cannot be scoped to Canada Retirement with the data as tagged "
                   "-- which is a tagging change, not something we can solve downstream.")
    emit("manulifeid_split", out)


run_section("manulifeid_split", c9_manulifeid_split)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C10 — Marketing discriminator candidates
# MAGIC
# MAGIC The SME wants Page Views / Visits / Visitors "ideally non-marketing". Nothing in the repo
# MAGIC defines marketing traffic for GWAM. This profiles what could carry that meaning, so the SME
# MAGIC question can offer concrete options instead of asking an open-ended one.

# COMMAND ----------

def c10_marketing_fields():
    cols = [("campaign", pick("post_campaign", "campaign")),
            ("ref_type", pick("post_ref_type", "ref_type")),
            ("referrer", pick("post_referrer", "referrer")),
            ("channel", pick("post_channel", "channel"))]
    emit("marketing_fields", {
        "window": [START_DATE, MAX_DATE],
        "columns": {k: v for k, v in cols},
        "per_rsid_population": per_rsid_rates(WIN, cols),
        "top_values_all_suites": {k: top_values(WIN, v) for k, v in cols if v},
        "note": ("Campaign-tagged share is the most likely operational definition of 'marketing', "
                 "with ref_type as the fallback. The SME picks; this just bounds the options. "
                 "↺ 2026-07-29: she picked neither -- the rule is 'carries the CID query "
                 "parameter'. C11 tests whether campaign is the same thing."),
    })


run_section("marketing_fields", c10_marketing_fields)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C11 — Is `post_campaign` the same thing as a `cid=` query parameter?
# MAGIC
# MAGIC The SME's marketing rule (2026-07-29) is **"carries CID"** — a query-string parameter. Our
# MAGIC pipeline ingests Adobe's `post_campaign` column, so before anyone writes an exclusion rule we
# MAGIC need to know whether the column we have stands in for the parameter she named.
# MAGIC
# MAGIC ↺ **Corrected 2026-08-04.** This cell used to say the pipeline "strips query strings by policy
# MAGIC (ADR-0007)". It does not: bronze writes `post_page_url` verbatim and nothing in bronze, silver
# MAGIC or gold touches a query string. ADR-0007 governs identity pseudonymization. The stripping is
# MAGIC in the *EDA notebooks* only. The real constraint is that `SILVER_COLUMNS` drops the URL
# MAGIC column, so a CID parse has to happen at bronze or silver — a design decision, not an ADR
# MAGIC amendment.
# MAGIC
# MAGIC Read the result knowing the two are *not* expected to match row-for-row: Adobe persists a
# MAGIC campaign value across the visit, while `cid=` appears only on the landing hit. So
# MAGIC `campaign_only >> cid_only` is normal and healthy. The two figures that matter are
# MAGIC **`cid_only` ≈ 0** (nothing carries CID that the column misses) and a high
# MAGIC **`equal_when_both / both`** (when both are present they agree). If either fails, the rule
# MAGIC cannot be implemented from `post_campaign` and needs the raw query string parsed upstream.
# MAGIC
# MAGIC **Privacy.** This section reads raw URLs transiently and emits **counts only** — never a
# MAGIC query-string value, never a URL. That matches the repo's posture (the EDA notebooks strip
# MAGIC query strings because session tokens live there) while still answering the question.

# COMMAND ----------

def c11_cid_vs_campaign():
    url_col = pick("page_url", "post_page_url")
    if not url_col:
        raise ValueError("no page_url/post_page_url column in this feed")
    camp_col = pick("post_campaign", "campaign")

    lurl = F.lower(qcol(url_col).cast("string"))
    cid = F.regexp_extract(F.coalesce(lurl, F.lit("")), CID_REGEX, 1)
    has_cid = F.length(cid) > 0
    if camp_col:
        camp = F.lower(F.trim(qcol(camp_col).cast("string")))
        has_camp = nonblank(camp_col)
    else:
        camp, has_camp = F.lit(""), F.lit(False)

    def xtab(df):
        r = df.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.when(has_cid, 1).otherwise(0)).alias("cid_rows"),
            F.sum(F.when(has_camp, 1).otherwise(0)).alias("campaign_rows"),
            F.sum(F.when(has_cid & has_camp, 1).otherwise(0)).alias("both"),
            F.sum(F.when(has_cid & ~has_camp, 1).otherwise(0)).alias("cid_only"),
            F.sum(F.when(~has_cid & has_camp, 1).otherwise(0)).alias("campaign_only"),
            F.sum(F.when(has_cid & has_camp & (cid == camp), 1).otherwise(0)).alias("equal_when_both"),
        ).collect()[0]
        out = {k: int(r[k] or 0) for k in ("rows", "cid_rows", "campaign_rows", "both",
                                          "cid_only", "campaign_only", "equal_when_both")}
        out["agreement_when_both"] = (out["equal_when_both"] / out["both"]) if out["both"] else None
        return out

    d = WIN.filter(F.col("rsid") == F.lit(PIPELINE_RSID))
    payload = {
        "window": [START_DATE, MAX_DATE], "rsid": PIPELINE_RSID,
        "url_column": url_col, "campaign_column": camp_col,
        "cid_regex": CID_REGEX,
        "suite_all": xtab(d),
    }
    if EVAR105:
        c105 = F.lower(qcol(EVAR105).cast("string"))
        payload["segment_scope"] = xtab(
            d.filter(c105.contains("ca-retirement") & c105.contains("gwam")))
    payload["note"] = (
        "Tests the 2026-07-29 marketing rule (Q5: marketing = carries CID) against the column we "
        "ingest. EXPECTED asymmetry: post_campaign persists across a visit while cid= appears only "
        "on landing URLs, so campaign_only >> cid_only is normal. The real checks are cid_only ~ 0 "
        "and a high agreement_when_both. Counts only -- no query-string values or URLs are emitted "
        "(ADR-0007 posture). ↺ CORRECTED 2026-08-04: this note used to end 'bronze projects "
        "post_page_url and strips query strings, so parsing cid= in production needs an ADR "
        "amendment'. Bronze does NOT strip -- it writes post_page_url verbatim, and nothing in "
        "bronze/silver/gold touches a query string; only the EDA notebooks strip. What actually "
        "constrains a production CID rule is that SILVER_COLUMNS drops post_page_url, so the parse "
        "must live at bronze or silver. That is a design decision, not an ADR amendment.")
    emit("cid_vs_campaign", payload)


run_section("cid_vs_campaign", c11_cid_vs_campaign)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C12 — Per-visit page-view distribution, and the ECID-vs-visid-pair visitor grain
# MAGIC
# MAGIC The SME named three anomaly signals on 2026-07-29: **unique ECID visitors**, **page views
# MAGIC per visit < 1**, and **pages consistently at exactly 2** as a duplication indicator. None of
# MAGIC the three exists as a declared metric today, and two of them are per-visit ratios we have
# MAGIC never measured. This section profiles the distribution first, so thresholds come from data.
# MAGIC
# MAGIC Three things worth knowing about how to read it:
# MAGIC
# MAGIC - **"< 1" is really a zero-page-view-visit detector.** A daily total of page views over
# MAGIC   visits can only fall below 1 if visits exist that contain no page view at all, so
# MAGIC   `share_pv_eq_0` is the direct measurement of her signal.
# MAGIC - **The "exactly 2" signal is about consistency, not level.** A stable point mass at 2 is
# MAGIC   what indicates duplication, so the daily series matters more than the window average —
# MAGIC   and no current scorer (robust-z, level-shift, ECOD, rules) expresses "unusually stable".
# MAGIC - **The page-view basis is still an open SME question** (doc-20 Q6). `pv_basis` in the
# MAGIC   payload records which definition was available; the fallbacks are deliberate, not silent.
# MAGIC
# MAGIC The `visitor_grain` block settles something else: gold counts visitors as
# MAGIC `countDistinct(mcvisid)` (the ECID, which is what the SME named) while the EDA notebooks
# MAGIC count the `post_visid` pair. Both are defensible; they are not equal, and until now nobody
# MAGIC had measured the gap.

# COMMAND ----------

def c12_visit_shape():
    key_cols = ["post_visid_high", "post_visid_low", "visit_num"]
    missing = [c for c in key_cols if not have(c)]
    if missing:
        raise ValueError(f"visit key columns missing from this feed: {missing}")

    # Page-view basis, best available. page_event == 0 is Adobe's own page-view marker; a
    # populated pagename is the weaker proxy; all-hits is the honest last resort. Whichever
    # is used is reported, because doc-20 Q6 has not been answered.
    #
    # try_cast, never cast: Databricks runs ANSI mode, so a single non-numeric page_event
    # value would throw and kill the section -- the CoverMe E1 defect. try_cast yields NULL
    # instead, which simply fails the == 0 test. Verified against a non-numeric row locally.
    pe = pick("post_page_event", "page_event")
    pn = pick("post_pagename", "pagename")
    if pe:
        pv_flag = F.expr(f"try_cast({sql_col(pe)} as int)") == F.lit(0)
        pv_basis = f"try_cast({pe} as int) == 0 (Adobe page-view marker)"
    elif pn:
        pv_flag, pv_basis = nonblank(pn), f"nonblank({pn}) (proxy -- no page_event column)"
    else:
        pv_flag, pv_basis = F.lit(True), "ALL HITS (no page_event and no pagename column)"

    # NULLs coalesced positionally so distinct visit keys cannot collide (the NULL_SAFE_KEYS
    # convention from cm_registry).
    key_exprs = [F.coalesce(qcol(c).cast("string"), F.lit("~null~")) for c in key_cols]
    key = [e.alias(c) for e, c in zip(key_exprs, key_cols)]
    bucket = (F.when(F.col("pvs") == 0, "0")
               .when(F.col("pvs") == 1, "1")
               .when(F.col("pvs") == 2, "2")
               .when(F.col("pvs") <= 5, "3-5")
               .otherwise("6+"))

    def per_visit(df):
        return df.groupBy(*key).agg(F.count(F.lit(1)).alias("hits"),
                                    F.sum(F.when(pv_flag, 1).otherwise(0)).alias("pvs"),
                                    F.min("process_date").alias("visit_date"))

    def shape(pv):
        agg = pv.agg(F.count(F.lit(1)).alias("visits"),
                     F.sum("pvs").alias("pvs"),
                     F.avg((F.col("pvs") == 2).cast("double")).alias("eq2"),
                     F.avg((F.col("pvs") == 0).cast("double")).alias("eq0")).collect()[0]
        n = int(agg["visits"] or 0)
        dist = {r["b"]: int(r["count"])
                for r in pv.groupBy(bucket.alias("b")).count().collect()}
        daily = (pv.groupBy("visit_date")
                   .agg(F.count(F.lit(1)).alias("visits"),
                        F.sum("pvs").alias("pvs"),
                        F.avg((F.col("pvs") == 2).cast("double")).alias("share_eq2"))
                   .orderBy("visit_date").collect())
        return {
            "visits": n,
            "pv_per_visit": (float(agg["pvs"] or 0) / n) if n else None,
            "share_pv_eq_0": float(agg["eq0"] or 0.0),
            "share_pv_eq_2": float(agg["eq2"] or 0.0),
            "bucket_dist": dist,
            "daily": [{"date": str(r["visit_date"])[:10], "visits": int(r["visits"]),
                       "pv_per_visit": (float(r["pvs"] or 0) / int(r["visits"])) if r["visits"] else None,
                       "share_eq2": float(r["share_eq2"] or 0.0)} for r in daily],
        }

    d = WIN.filter(F.col("rsid") == F.lit(PIPELINE_RSID))
    payload = {"window": [START_DATE, MAX_DATE], "rsid": PIPELINE_RSID,
               "visit_key": key_cols, "pv_basis": pv_basis,
               "suite_all": shape(per_visit(d))}
    if EVAR105:
        c105 = F.lower(qcol(EVAR105).cast("string"))
        payload["segment_scope"] = shape(per_visit(
            d.filter(c105.contains("ca-retirement") & c105.contains("gwam"))))

    ecid = pick("mcvisid", "post_mcvisid")
    if ecid:
        rows = (d.groupBy("process_date")
                 .agg(F.countDistinct(qcol(ecid)).alias("ecid"),
                      F.countDistinct(F.concat_ws(":", *key_exprs[:2])).alias("pair"))
                 .orderBy("process_date").collect())
        payload["visitor_grain"] = {
            "ecid_column": ecid,
            "daily": [{"date": str(r["process_date"])[:10], "ecid": int(r["ecid"]),
                       "pair": int(r["pair"])} for r in rows],
            "note": ("Quantifies a divergence nobody had measured: gold counts visitors as "
                     "countDistinct(mcvisid) (gold_lib.py:94) -- the ECID the SME named -- while "
                     "the EDA notebooks count the post_visid pair "
                     "(gwam_canada_retirement_eda.py:1320). Registry entry gwam_pw_visitors."),
        }
    else:
        payload["visitor_grain"] = {"ecid_column": None,
                                    "note": "no mcvisid column in this feed -- ECID grain unmeasurable"}

    payload["note"] = (
        "Profiles the SME's 2026-07-29 anomaly signals before they are declared. share_pv_eq_0 IS "
        "the 'page views per visit < 1' signal (registry gwam_pw_pv_per_visit): a daily ratio can "
        "only drop below 1 if zero-page-view visits exist. share_pv_eq_2 plus its daily series IS "
        "the duplication signal (registry gwam_pw_pv_per_visit_dup2), where the SME's concern is "
        "CONSISTENCY at 2 rather than the level -- read the daily spread, not the average. Both "
        "metrics are blocked on doc-19 G2 (SeriesSpec has no numerator/denominator) and doc-20 Q6 "
        "(which page-view basis is meant -- see pv_basis above).")
    emit("visit_shape", payload)


run_section("visit_shape", c12_visit_shape)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C13 — Link-rule eVar census (evar101 / evar193 / evar194)
# MAGIC
# MAGIC The SME's 2026-08-04 scope names three dimensions the pipeline has never ingested — bronze
# MAGIC projects 17 columns and **zero** eVars. Before any rule can be matched we need to know these
# MAGIC columns carry data here, and in what shape.
# MAGIC
# MAGIC ⚠ **The single most important number in this run is `evar194.len_max`.** Adobe props cap at
# MAGIC 100 characters and eVars at 255. `signin_member/en` and `signin_join/en` are identical for
# MAGIC their first **101** characters — so a max of exactly 100 means those two rules are one value
# MAGIC in the data and no matching strategy can recover them. A max of exactly 255 means
# MAGIC `find_advisor/fr` (262 chars) is stored truncated and can never match exactly.
# MAGIC
# MAGIC **Privacy.** Hrefs the SME gave us are echoed verbatim; every other discovered href has its
# MAGIC parameter *values* elided (`?a=<v>&b=<v>`), keeping scheme/host/path and parameter keys —
# MAGIC enough to answer "is it raw / is it encoded / which params", and no session token.

# COMMAND ----------

_KNOWN_HREFS = {r["href"].lower() for r in SME_LINK_RULES}


def elide_href(v):
    """Emit a DISCOVERED url with parameter values removed. A url the SME handed us is echoed
    verbatim -- we may republish what we were given, not what we found (C11 set this posture)."""
    if v is None:
        return None
    s = str(v)
    if s.lower() in _KNOWN_HREFS:
        return s
    head, sep, qs = s.partition("?")
    if not sep:
        return head
    keys = []
    for kv in qs.split("&"):
        k, eq, _ = kv.partition("=")
        keys.append(k + "=<v>" if eq else k)
    return head + "?" + "&".join(keys)


def c13_link_evar_census():
    d = WIN.filter(F.col("rsid") == F.lit(PIPELINE_RSID))

    # Every candidate spelling, not just the one evar() picked. The 2026-07-02 census found
    # post_evar193/194 populated but bare evar193/194 absent, while 101 has both -- so which
    # column carries the value is itself a finding, and pick() must be evidenced not trusted.
    cands = [f"{p}{n}" for n in (101, 193, 194) for p in ("post_evar", "evar")]
    present = [c for c in cands if have(c)]
    cand_rates = {c: None for c in cands}
    if present:
        row = d.agg(*[nonblank_rate(c).alias(c) for c in present]).collect()[0]
        for c in present:
            cand_rates[c] = float(row[c]) if row[c] is not None else None

    resolved = {"evar101": EVAR101, "evar193": EVAR193, "evar194": EVAR194}
    stats, aggs = {}, [F.count(F.lit(1)).alias("rows")]
    for label, name in resolved.items():
        if not name:
            continue
        nb = F.when(nonblank(name), qcol(name).cast("string"))
        aggs += [
            nonblank_rate(name).alias(f"{label}__rate"),
            F.approx_count_distinct(nb).alias(f"{label}__distinct"),
            F.min(F.length(nb)).alias(f"{label}__len_min"),
            F.max(F.length(nb)).alias(f"{label}__len_max"),
            F.expr(f"percentile_approx(nullif(length({sql_col(name)}), 0), array(0.5,0.95,0.99))")
             .alias(f"{label}__len_pct"),
        ]

    # Is the stored href raw, or has something upstream already stripped/encoded it?
    if EVAR194:
        raw = href_raw()
        marks = {
            "has_query": raw.contains("?"), "has_fragment": raw.contains("#"),
            "pct_encoded": raw.rlike("%[0-9a-f]{2}"),
            "pct_3a_colon": raw.contains("%3a"), "pct_2f_slash": raw.contains("%2f"),
            "has_goto": raw.contains("goto="), "has_ui_locales": raw.contains("ui_locales"),
            "host_id_manulife": raw.contains("id.manulife.ca"),
            "host_portal_manulife": raw.contains("portal.manulife.ca"),
        }
        aggs += [F.sum(F.when(F.coalesce(e, F.lit(False)), 1).otherwise(0)).alias(f"enc__{k}")
                 for k, e in marks.items()]
        aggs.append(F.sum(F.when(nonblank(EVAR194), 1).otherwise(0)).alias("n_href"))
        if EVAR193:
            aggs += [
                F.sum(F.when(nonblank(EVAR194) & ~nonblank(EVAR193), 1).otherwise(0)).alias("href_no_name"),
                F.sum(F.when(~nonblank(EVAR194) & nonblank(EVAR193), 1).otherwise(0)).alias("name_no_href"),
            ]

    row = d.agg(*aggs).collect()[0]
    fields = set(row.asDict().keys())
    total = int(row["rows"])
    for label in resolved:
        if f"{label}__rate" not in fields:
            stats[label] = None
            continue
        pct = row[f"{label}__len_pct"]
        stats[label] = {
            "column": resolved[label],
            "nonblank_rate": float(row[f"{label}__rate"] or 0.0),
            "apx_distinct": int(row[f"{label}__distinct"] or 0),
            "len_min": int(row[f"{label}__len_min"]) if row[f"{label}__len_min"] is not None else None,
            "len_max": int(row[f"{label}__len_max"]) if row[f"{label}__len_max"] is not None else None,
            "len_p50_p95_p99": [int(x) for x in pct] if pct else None,
        }

    payload = {
        "window": [START_DATE, MAX_DATE], "rsid": PIPELINE_RSID, "rows": total,
        "candidate_nonblank_rates": cand_rates,
        "resolved_columns": resolved,
        "stats": stats,
        "per_rsid": per_rsid_rates(WIN, [("evar101", EVAR101), ("evar193", EVAR193),
                                         ("evar194", EVAR194)]),
    }

    if EVAR194:
        n_href = int(row["n_href"] or 0)
        payload["encoding"] = {k: int(row[f"enc__{k}"] or 0) for k in
                               ("has_query", "has_fragment", "pct_encoded", "pct_3a_colon",
                                "pct_2f_slash", "has_goto", "has_ui_locales",
                                "host_id_manulife", "host_portal_manulife")}
        payload["encoding"]["n_href_nonblank"] = n_href
        if EVAR193:
            payload["cooccurrence"] = {"href_without_name": int(row["href_no_name"] or 0),
                                       "name_without_href": int(row["name_no_href"] or 0)}
        payload["top_hrefs"] = [{"value": elide_href(x["value"]), "count": x["count"]}
                                for x in top_values(d, EVAR194)]
        payload["top_hrefs_id_manulife"] = [
            {"value": elide_href(x["value"]), "count": x["count"]}
            for x in top_values(d, EVAR194, extra_filter=href_raw().contains("id.manulife.ca"))]

    payload["top_link_names"] = top_values(d, EVAR193)
    payload["top_page_names"] = top_values(d, EVAR101)

    payload["truncation_verdict"] = (
        "READ evar194.len_max FIRST. == 100 -> the feed caps at prop length: signin_member/en and "
        "signin_join/en share their first 101 chars and are THE SAME VALUE, unrecoverable. == 255 "
        "-> eVar cap: find_advisor/fr (262 chars) is stored truncated, exact match impossible but "
        "its token is at the front so token match survives. Anything else -> no cap hit in this "
        "window, and both rules stay separable.")
    payload["note"] = (
        "evar101/193/194 are documented for Canada Retirement in data/EDDL_datalayer.xlsx (tab "
        "'EDDL for CAR_WIP': eVar101 Page Name, eVar193 Link Name, eVar194 Link Href) and already "
        "labelled in gwam_canada_retirement_eda.py EVAR_LABELS -- but NOTHING reads them: "
        "conf/bronze_columns.py projects 17 columns and no eVar. Adding them is a bronze schema "
        "change plus a backfill, gated on this section.")
    emit("link_evar_census", payload)


run_section("link_evar_census", c13_link_evar_census)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C14 — Link-rule match: five strategies, side by side
# MAGIC
# MAGIC Per rule × language, how many hits match under each of five matching strategies. The
# MAGIC deliverable is the **comparison**, not one number: `m_path` is expected to over-claim (it
# MAGIC collapses `signin_member`+`signin_join` into one bucket and merges en/fr for three more
# MAGIC rules), and `m_prefix` is expected to over-claim on the two rules whose href is a strict
# MAGIC prefix of another. Seeing them disagree is the measurement.
# MAGIC
# MAGIC `m_token` is the working definition — token AND NOT anti-token on the percent-decoded value,
# MAGIC plus the language marker on the **raw** value. Verified to resolve all 16 uniquely against
# MAGIC the SME's own strings; the collision matrix here re-tests that against what the feed
# MAGIC actually stores, which can differ (truncated, re-encoded, redirected).

# COMMAND ----------

def _series_stats(vals):
    nz = [v for v in vals if v > 0]
    s = sorted(vals)
    return {
        "total": int(sum(vals)), "days": len(vals), "days_nonzero": len(nz),
        "days_zero": len(vals) - len(nz),
        "min": int(s[0]) if s else None,
        "p50": int(s[len(s) // 2]) if s else None,
        "max": int(s[-1]) if s else None,
    }


def c14_link_rule_match():
    if not EVAR194:
        emit("link_rule_match", {"skipped": "no evar194/post_evar194 column in this feed -- "
                                            "the SME's rules are unmatchable here"})
        return

    d = WIN.filter(F.col("rsid") == F.lit(PIPELINE_RSID))
    raw, dec = href_raw(), href_dec()
    path = F.regexp_extract(raw, r"^([^?#]*)", 1)
    name_c = F.lower(F.trim(qcol(EVAR193).cast("string"))) if EVAR193 else None

    strat, tok_exprs = {}, {}
    for r in SME_LINK_RULES:
        k, h = rule_key(r), r["href"].lower()
        m_tok = rule_match_expr(r, raw, dec)
        tok_exprs[k] = m_tok
        if name_c is None:
            m_name = F.lit(None).cast("boolean")
        elif r["link_name"] == "":
            m_name = name_c.isNull() | (name_c == F.lit(""))
        else:
            wanted = [r["link_name"].lower()]
            if r["link_name_mojibake"]:
                wanted.append(r["link_name_mojibake"].lower())
            m_name = name_c.isin(wanted)
        strat[k] = {
            "exact": raw == F.lit(h),
            "prefix": raw.startswith(h),
            "path": path == F.lit(h.split("?")[0].split("#")[0]),
            "token": m_tok,
            "token_name": m_tok & F.coalesce(m_name, F.lit(False)),
        }

    aggs = [F.count(F.lit(1)).alias("rows")]
    for k, ss in strat.items():
        for lbl, e in ss.items():
            aggs.append(F.sum(F.when(F.coalesce(e, F.lit(False)), 1).otherwise(0)).alias(f"{k}__{lbl}"))
    # Name-encoding verdict: does the feed hold the true accents or the Latin-1 mangling?
    if name_c is not None:
        for r in SME_LINK_RULES:
            if not r["link_name_mojibake"]:
                continue
            k = rule_key(r)
            aggs += [
                F.sum(F.when(name_c == F.lit(r["link_name"].lower()), 1).otherwise(0)).alias(f"moj__{k}__true"),
                F.sum(F.when(name_c == F.lit(r["link_name_mojibake"].lower()), 1).otherwise(0)).alias(f"moj__{k}__mojibake"),
            ]
    row = d.agg(*aggs).collect()[0]

    by_rule = {}
    for k, ss in strat.items():
        by_rule[k] = {lbl: int(row[f"{k}__{lbl}"] or 0) for lbl in ss}

    # Collision matrix -- how many rules does a single hit satisfy? >1 under `path` is the
    # direct measurement of the query-stripping merge.
    collisions = {}
    for lbl in ("path", "token"):
        n = F.lit(0)
        for k in strat:
            n = n + F.when(F.coalesce(strat[k][lbl], F.lit(False)), 1).otherwise(0)
        rows = (d.filter(nonblank(EVAR194)).groupBy(n.alias("n")).count()
                 .orderBy("n").collect())
        collisions[lbl] = {str(int(r["n"])): int(r["count"]) for r in rows}

    # Daily series, parallel-array encoded. One `dates` list + one int array per rule is ~7 KB;
    # per-day {date, hits} objects for 16 rules would be ~86 KB, which crosses the 48 000-byte
    # emit split and risks the Databricks stdout truncation of doc-16 §0.5.
    daily = (d.groupBy("process_date")
              .agg(*[F.sum(F.when(tok_exprs[k], 1).otherwise(0)).alias(k) for k in RULE_KEYS])
              .orderBy("process_date").collect())
    dates = [str(r["process_date"])[:10] for r in daily]
    series = {k: [int(r[k] or 0) for r in daily] for k in RULE_KEYS}

    unmatched = top_values(d, EVAR194, extra_filter=~any_rule_expr(raw, dec))

    payload = {
        "window": [START_DATE, MAX_DATE], "rsid": PIPELINE_RSID, "rows": int(row["rows"]),
        "strategies": ["exact", "prefix", "path", "token", "token_name"],
        "by_rule": by_rule,
        "collision_hist": collisions,
        "dates": dates,
        "daily_token_hits": series,
        "volume": {k: _series_stats(series[k]) for k in RULE_KEYS},
        "unmatched_href_top": [{"value": elide_href(x["value"]), "count": x["count"]} for x in unmatched],
    }
    if name_c is not None:
        payload["name_encoding"] = {
            rule_key(r): {"true_accents": int(row[f"moj__{rule_key(r)}__true"] or 0),
                          "mojibake": int(row[f"moj__{rule_key(r)}__mojibake"] or 0)}
            for r in SME_LINK_RULES if r["link_name_mojibake"]}
    payload["note"] = (
        "Expected disagreements, stated up front so a clean-looking table is not misread: "
        "`path` MERGES {signin_member, signin_join} x {en,fr} into one bucket and merges en/fr "
        "for signup_join / app_apple / app_android -- its collision_hist should show n=2 and n=4 "
        "mass. `prefix` over-claims on signin_member/fr (a strict prefix of signin_join/fr) and "
        "app_apple/en (a strict prefix of app_apple/fr). `token` should show ONLY n=1. If it "
        "does not, the feed stores something the spec did not anticipate -- read "
        "unmatched_href_top and link_evar_census.stats.evar194 before trusting any count. "
        "`volume` is the feasibility answer: a rule whose p50 daily hits is single-digit cannot "
        "support daily anomaly detection no matter how cleanly it matches.")
    emit("link_rule_match", payload)


run_section("link_rule_match", c14_link_rule_match)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C15 — Where the link clicks fire, and the D8 collision
# MAGIC
# MAGIC A link click happens **on a page**. The rules say nothing about which page, and the pipeline
# MAGIC filters on the page url — so if these clicks fire on pages outside the ingest scope, the
# MAGIC scope never sees them. This sizes each rule against `en_only`, the old three-pattern `broad`,
# MAGIC and the two-pattern `broad_narrow` that 2026-08-04 actually ratified.
# MAGIC
# MAGIC ⚠ **The D8 collision.** 5 of 8 rules target `id.manulife.ca` / `portal.manulife.ca`, both on
# MAGIC `SCOPE_LOGIN_HOST_EXCLUDE`. The pipeline matches that list against the PAGE url, so the
# MAGIC clicks survive — `excl_by_page_url` should be ≈0 while `excl_by_href` should be ≈100% for
# MAGIC those rules. That gap is the cost of ever folding `post_evar194` into a url expression.

# COMMAND ----------

def _page_url_expr():
    parts = [F.when(nonblank(c), qcol(c).cast("string")) for c in ("page_url", "post_page_url") if have(c)]
    return F.lower(F.coalesce(*parts)) if parts else F.lit(None).cast("string")


def c15_link_page_context():
    if not EVAR194:
        emit("link_page_context", {"skipped": "no evar194 column -- no rule to place on a page"})
        return

    d = WIN.filter(F.col("rsid") == F.lit(PIPELINE_RSID))
    raw, dec = href_raw(), href_dec()
    pu = _page_url_expr()

    in_en = like_any(pu, URL_SCOPE_EN_ONLY)
    excl = like_any(pu, URL_SCOPE_EXCLUDE)
    in_broad = like_any(pu, URL_SCOPE_BROAD) & ~excl
    in_narrow = like_any(pu, URL_SCOPE_BROAD_NARROW) & ~excl
    login_page = like_any(pu, LOGIN_HOST_EXCLUDE)
    login_href = like_any(raw, LOGIN_HOST_EXCLUDE)

    aggs = []
    for r in SME_LINK_RULES:
        k = rule_key(r)
        m = rule_match_expr(r, raw, dec)

        def s(cond, alias):
            aggs.append(F.sum(F.when(m & F.coalesce(cond, F.lit(False)), 1).otherwise(0)).alias(alias))

        aggs.append(F.sum(F.when(m, 1).otherwise(0)).alias(f"{k}__matched"))
        s(in_en, f"{k}__en_only")
        s(in_broad, f"{k}__broad")
        s(in_narrow, f"{k}__broad_narrow")
        s(in_broad & ~in_narrow, f"{k}__group_plans_only")
        s(~in_en & ~in_broad, f"{k}__neither")
        s(login_page, f"{k}__excl_by_page_url")
        s(login_href, f"{k}__excl_by_href")
        aggs.append(F.sum(F.when(m & ~F.coalesce(pu.isNotNull(), F.lit(False)), 1).otherwise(0))
                     .alias(f"{k}__page_url_blank"))
    row = d.agg(*aggs).collect()[0]

    by_rule = {}
    for r in SME_LINK_RULES:
        k = rule_key(r)
        by_rule[k] = {lbl: int(row[f"{k}__{lbl}"] or 0) for lbl in
                      ("matched", "en_only", "broad", "broad_narrow", "group_plans_only",
                       "neither", "excl_by_page_url", "excl_by_href", "page_url_blank")}

    # Where do they fire? Host + query-stripped path is safe to echo: no token lives there.
    matched = d.filter(any_rule_expr(raw, dec))
    hp = F.regexp_extract(F.regexp_replace(pu, r"^[a-z]+://", ""), r"^([^?#]*)", 1)
    hosts = (matched.groupBy(F.regexp_extract(hp, r"^([^/]+)", 1).alias("host"))
                    .count().orderBy(F.desc("count")).limit(TOP_N).collect())
    paths = (matched.groupBy(hp.alias("host_path")).count()
                    .orderBy(F.desc("count")).limit(TOP_N).collect())

    payload = {
        "window": [START_DATE, MAX_DATE], "rsid": PIPELINE_RSID,
        "scope_variants": {"en_only": URL_SCOPE_EN_ONLY, "broad": URL_SCOPE_BROAD,
                           "broad_narrow": URL_SCOPE_BROAD_NARROW, "exclude": URL_SCOPE_EXCLUDE},
        "by_rule": by_rule,
        "top_hosts": [{"host": r["host"], "count": int(r["count"])} for r in hosts],
        "top_host_paths": [{"host_path": r["host_path"], "count": int(r["count"])} for r in paths],
        "top_page_names_on_matched": top_values(matched, EVAR101),
    }
    payload["note"] = (
        "Read three columns. (1) `neither` -- rule hits firing on pages no scope variant covers; "
        "any real volume there means the ingest widening does not reach them and another scope "
        "question is open (settings.py already records epargnemanuvie.ca as uncovered). "
        "(2) `group_plans_only` -- traffic the dropped %/group-plans% pattern WOULD have admitted; "
        "this is the price of the 2026-08-04 decision to admit French without the umbrella. "
        "(3) `excl_by_page_url` vs `excl_by_href` -- the D8 collision. The first is what the "
        "pipeline does today (match the PAGE url) and should be ~0; the second is what would "
        "happen if evar194 joined the url coalesce, and should be ~100% for the five id/portal "
        "rules. The difference is the size of that mistake. doc-16 D14 forbids it; this measures it.")
    emit("link_page_context", payload)


run_section("link_page_context", c15_link_page_context)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C16 — Language derivation: four independent readings
# MAGIC
# MAGIC The rules are specified per language but the feed has no EN/FR field for GWAM (`language` is
# MAGIC a raw Adobe numeric lookup id and the decode tables are not in this repo). Four independent
# MAGIC derivations are computed and cross-tabulated; the agreement matrix is the finding.
# MAGIC
# MAGIC ⚠ `regexp_extract` returns the **first** match, and language is read off the **raw** href.
# MAGIC Both are deliberate: the sponsor/advisor FR hrefs carry an inner `ui_locales%3Den-CA`, so
# MAGIC decoding first — or taking the last match — would read them as English.

# COMMAND ----------

def c16_link_language_split():
    if not EVAR194:
        emit("link_language_split", {"skipped": "no evar194 column -- no href to read language from"})
        return

    d = WIN.filter(F.col("rsid") == F.lit(PIPELINE_RSID))
    raw, dec = href_raw(), href_dec()
    pu = _page_url_expr()
    matched = d.filter(any_rule_expr(raw, dec))

    lang_param = F.regexp_extract(raw, r"[?&](?:ui_locales|hl|l)=([a-z]{2})", 1)
    l_param = F.when(lang_param == "fr", "fr").when(lang_param == "en", "en").otherwise("unknown")
    l_host = (F.when(raw.rlike(r"manuvie\.ca|epargnemanuvie"), "fr")
               .when(raw.rlike(r"manulife\.ca|manulife\.com"), "en").otherwise("unknown"))
    fr_names = [r["link_name"].lower() for r in SME_LINK_RULES if r["lang"] == "fr" and r["link_name"]]
    fr_names += [r["link_name_mojibake"].lower() for r in SME_LINK_RULES if r["link_name_mojibake"]]
    en_names = [r["link_name"].lower() for r in SME_LINK_RULES if r["lang"] == "en" and r["link_name"]]
    if EVAR193:
        nm = F.lower(F.trim(qcol(EVAR193).cast("string")))
        l_name = F.when(nm.isin(fr_names), "fr").when(nm.isin(en_names), "en").otherwise("unknown")
    else:
        l_name = F.lit("unknown")
    l_page = (F.when(pu.rlike(r"/fr/|/ca/fr|regimes-collectifs|manuvie"), "fr")
               .when(pu.rlike(r"/en/|/ca/en"), "en").otherwise("unknown"))

    derivations = {"href_param": l_param, "href_host": l_host,
                   "link_name": l_name, "page_url": l_page}

    counts = {}
    for lbl, e in derivations.items():
        rows = matched.groupBy(e.alias("v")).count().orderBy(F.desc("count")).collect()
        counts[lbl] = {str(r["v"]): int(r["count"]) for r in rows}

    keys = list(derivations)
    agree = {}
    aggs = []
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            a, b = derivations[keys[i]], derivations[keys[j]]
            both = (a != "unknown") & (b != "unknown")
            aggs += [F.sum(F.when(both, 1).otherwise(0)).alias(f"{keys[i]}|{keys[j]}__n"),
                     F.sum(F.when(both & (a == b), 1).otherwise(0)).alias(f"{keys[i]}|{keys[j]}__same")]
    if aggs:
        row = matched.agg(*aggs).collect()[0]
        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                p = f"{keys[i]}|{keys[j]}"
                n, same = int(row[f"{p}__n"] or 0), int(row[f"{p}__same"] or 0)
                agree[p] = {"comparable": n, "agree": same,
                            "agreement_rate": (same / n) if n else None}

    per_rule = {}
    aggs = []
    for r in SME_LINK_RULES:
        k = rule_key(r)
        m = rule_match_expr(r, raw, dec)
        for lbl, e in derivations.items():
            for v in ("en", "fr", "unknown"):
                aggs.append(F.sum(F.when(m & (e == v), 1).otherwise(0)).alias(f"{k}__{lbl}__{v}"))
    row = matched.agg(*aggs).collect()[0]
    for r in SME_LINK_RULES:
        k = rule_key(r)
        per_rule[k] = {lbl: {v: int(row[f"{k}__{lbl}__{v}"] or 0) for v in ("en", "fr", "unknown")}
                       for lbl in derivations}

    # If a real language dimension exists we should stop deriving. Cheap to check.
    lang_like = sorted(c for c in AVAILABLE if "locale" in c or c == "language"
                       or c.endswith("_language") or c == "geo_country")[:12]
    native = {}
    if lang_like:
        rr = d.agg(*[nonblank_rate(c).alias(c) for c in lang_like]).collect()[0]
        native = {c: (float(rr[c]) if rr[c] is not None else None) for c in lang_like}

    emit("link_language_split", {
        "window": [START_DATE, MAX_DATE], "rsid": PIPELINE_RSID,
        "derivations": list(derivations),
        "counts": counts, "pairwise_agreement": agree, "per_rule": per_rule,
        "native_language_candidates": native,
        "note": (
            "Four derivations are emitted separately and never reconciled here on purpose -- a "
            "single 'language' column would hide the disagreement, and the disagreement is the "
            "finding. href_param is the most trustworthy for the sign-in family and reads the "
            "RAW value first-match, which is what keeps signin_sponsor/fr and signin_advisor/fr "
            "(inner ui_locales%3Den-CA inside an outer fr-CA url -- Q16) on the French side. "
            "link_name cannot split app_apple / app_android at all: the SME's own table has the "
            "Link Name blank in BOTH languages (Q18). If native_language_candidates shows a real "
            "populated locale column, prefer it and retire all four."),
    })


run_section("link_language_split", c16_link_language_split)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C17 — Qualified-visit scope: what the re-baseline actually costs
# MAGIC
# MAGIC **The gate section.** Under the 2026-08-04 ruling (doc-16 D13) a visit is in Public Website
# MAGIC scope if it contains ≥1 rule-matching link click. Every one of gold's 42 series would be
# MAGIC rebuilt on that population, so its size and stability decide whether the scope as specified
# MAGIC can support daily anomaly detection at all.
# MAGIC
# MAGIC Emits the qualified population, today's URL-scoped population, and the **overlap** between
# MAGIC them — the direct measure of how far the existing series move. Page views are reported under
# MAGIC **both** `PAGE_VIEW_BASIS` branches so doc-20 Q6/Q14 can be answered from one run.

# COMMAND ----------

def c17_qualified_visit_scope():
    if not EVAR194:
        emit("qualified_visit_scope", {"skipped": "no evar194 column -- the qualifying predicate "
                                                  "is not expressible on this feed"})
        return
    missing = [c for c in VISIT_KEY_COLS if not have(c)]
    if missing:
        emit("qualified_visit_scope", {"skipped": f"visit key columns absent: {missing}"})
        return

    d = WIN.filter(F.col("rsid") == F.lit(PIPELINE_RSID)).withColumn("_vk", visit_key_expr())
    raw, dec = href_raw(), href_dec()
    pu = _page_url_expr()
    qualifying = any_rule_expr(raw, dec)

    # Qualify the VISIT, then keep all of its hits -- that is what "a visit is in scope if it
    # contains a qualifying click" means, and it is why page views stay coherent under adobe_pv.
    qkeys = d.filter(qualifying).select("_vk").distinct()
    qual = d.join(qkeys, "_vk", "left_semi")

    ecid = pick("mcvisid", "post_mcvisid")
    pv_adobe = (F.expr("try_cast(post_page_event as int) = 0") if have("post_page_event")
                else F.lit(None).cast("boolean"))

    def daily(frame):
        aggs = [F.count(F.lit(1)).alias("hits"),
                F.countDistinct("_vk").alias("visits"),
                F.sum(F.when(F.coalesce(pv_adobe, F.lit(False)), 1).otherwise(0)).alias("pv_adobe")]
        if ecid:
            aggs.append(F.countDistinct(qcol(ecid)).alias("visitors_ecid"))
        aggs.append(F.countDistinct(F.concat_ws(":", qcol("post_visid_high").cast("string"),
                                                qcol("post_visid_low").cast("string")))
                     .alias("visitors_pair"))
        rows = frame.groupBy("process_date").agg(*aggs).orderBy("process_date").collect()
        return rows

    q_rows = daily(qual)
    dates = [str(r["process_date"])[:10] for r in q_rows]

    def pack(rows):
        idx = {str(r["process_date"])[:10]: r for r in rows}
        out = {}
        for fld in ("hits", "visits", "pv_adobe", "visitors_ecid", "visitors_pair"):
            if rows and fld in rows[0].asDict():
                out[fld] = [int(idx[dt][fld] or 0) if dt in idx else 0 for dt in dates]
        # page views under all_hits IS the hit count, by construction (gold_lib._pv_int)
        out["pv_all_hits"] = out.get("hits", [])
        return out

    populations = {"qualified": pack(q_rows)}
    excl = like_any(pu, URL_SCOPE_EXCLUDE)
    for lbl, pats in (("en_only", URL_SCOPE_EN_ONLY), ("broad", URL_SCOPE_BROAD),
                      ("broad_narrow", URL_SCOPE_BROAD_NARROW)):
        cond = like_any(pu, pats) & (~excl if lbl != "en_only" else F.lit(True))
        populations[lbl] = pack(daily(d.filter(cond)))

    # Overlap on the visit key -- the number that prices the re-baseline.
    overlap = {}
    n_q = qkeys.count()
    for lbl, pats in (("en_only", URL_SCOPE_EN_ONLY), ("broad_narrow", URL_SCOPE_BROAD_NARROW)):
        cond = like_any(pu, pats) & (~excl if lbl != "en_only" else F.lit(True))
        uk = d.filter(cond).select("_vk").distinct()
        n_u = uk.count()
        n_both = qkeys.join(uk, "_vk", "inner").count()
        overlap[lbl] = {"qualified_visits": n_q, "url_scoped_visits": n_u, "both": n_both,
                        "qualified_only": n_q - n_both, "url_scoped_only": n_u - n_both,
                        "share_of_url_scope_retained": (n_both / n_u) if n_u else None}

    # Which rules actually qualify visits, and which are rounding error?
    contrib = {}
    for r in SME_LINK_RULES:
        k = rule_key(r)
        contrib[k] = int(d.filter(rule_match_expr(r, raw, dec)).select("_vk").distinct().count())

    # The 02_silver_conform.py:70-73 warning: if the visid parts are degenerate on this suite,
    # visits_total collapses toward distinct(visit_num) and every count above is fiction.
    card = d.agg(F.approx_count_distinct(qcol("post_visid_high")).alias("hi"),
                 F.approx_count_distinct(qcol("post_visid_low")).alias("lo"),
                 F.approx_count_distinct(qcol("visit_num")).alias("vn"),
                 F.approx_count_distinct("_vk").alias("vk")).collect()[0]
    qcard = qual.agg(F.approx_count_distinct(qcol("post_visid_high")).alias("hi"),
                     F.approx_count_distinct(qcol("post_visid_low")).alias("lo")).collect()[0]

    qv = populations["qualified"].get("visits", [])
    s = sorted(qv)
    verdict = {
        "median_daily_qualified_visits": int(s[len(s) // 2]) if s else 0,
        "min_daily_qualified_visits": int(s[0]) if s else 0,
        "max_daily_qualified_visits": int(s[-1]) if s else 0,
        "days_below_100": sum(1 for v in qv if v < 100),
        "days_at_zero": sum(1 for v in qv if v == 0),
        "days": len(qv),
    }

    emit("qualified_visit_scope", {
        "window": [START_DATE, MAX_DATE], "rsid": PIPELINE_RSID,
        "visit_key": VISIT_KEY_COLS, "ecid_column": ecid,
        "adobe_pv_available": bool(have("post_page_event")),
        "dates": dates,
        "populations": populations,
        "visit_overlap": overlap,
        "per_rule_qualified_visits": contrib,
        "identity_cardinality": {
            "suite": {"post_visid_high": int(card["hi"] or 0), "post_visid_low": int(card["lo"] or 0),
                      "visit_num": int(card["vn"] or 0), "visit_key": int(card["vk"] or 0)},
            "qualified": {"post_visid_high": int(qcard["hi"] or 0), "post_visid_low": int(qcard["lo"] or 0)},
        },
        "verdict": verdict,
        "note": (
            "THIS IS THE GATE. `verdict.median_daily_qualified_visits` decides whether the scope "
            "as specified can carry daily anomaly detection; if it is small the answer goes back "
            "to the SME before anything is built, because all 42 gold series would be rebuilt on "
            "this population. `visit_overlap.share_of_url_scope_retained` is how much of today's "
            "series survives the change. `per_rule_qualified_visits` shows which of the 16 rules "
            "carry the scope and which are rounding error -- a rule contributing near-zero visits "
            "adds governance weight and no signal. Page views are given BOTH ways: pv_all_hits is "
            "the hit count by construction (gold_lib._pv_int returns lit(1)), pv_adobe counts "
            "try_cast(post_page_event as int)=0 -- doc-20 Q6/Q14. Finally check "
            "identity_cardinality: 02_silver_conform.py:70-73 warns post_visid_high/low can be "
            "<=1 on this suite, in which case visits collapse toward distinct(visit_num) and "
            "every visit count here is an artefact of visit_num alone."),
    })


run_section("qualified_visit_scope", c17_qualified_visit_scope)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C18 — eVar105 brand tag vs the link rules
# MAGIC
# MAGIC D11 (2026-07-29) defined the Public Website scope as `rsid` + an `evar105` brand-tag
# MAGIC parts-match on `ca-retirement` AND `gwam`. The 2026-08-04 spec never mentions `evar105`.
# MAGIC Either the brand tag still applies on top of the rules, or the rules replace it — that is a
# MAGIC business ruling (Q19), but it should be priced first.

# COMMAND ----------

def c18_evar105_vs_rules():
    if not EVAR105 or not EVAR194:
        emit("evar105_vs_rules", {"skipped": f"evar105={EVAR105}, evar194={EVAR194} -- "
                                             "both are required to cross-tabulate"})
        return
    missing = [c for c in VISIT_KEY_COLS if not have(c)]
    if missing:
        emit("evar105_vs_rules", {"skipped": f"visit key columns absent: {missing}"})
        return

    d = WIN.filter(F.col("rsid") == F.lit(PIPELINE_RSID)).withColumn("_vk", visit_key_expr())
    raw, dec = href_raw(), href_dec()
    c105 = F.lower(qcol(EVAR105).cast("string"))
    tagged = c105.contains("ca-retirement") & c105.contains("gwam")
    qualifying = any_rule_expr(raw, dec)

    row = d.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(tagged, 1).otherwise(0)).alias("tagged"),
        F.sum(F.when(qualifying, 1).otherwise(0)).alias("rule_hits"),
        F.sum(F.when(tagged & qualifying, 1).otherwise(0)).alias("both"),
        F.sum(F.when(~tagged & qualifying, 1).otherwise(0)).alias("rule_untagged"),
    ).collect()[0]

    qk = d.filter(qualifying).select("_vk").distinct()
    tk = d.filter(tagged).select("_vk").distinct()
    n_q, n_t = qk.count(), tk.count()
    n_both = qk.join(tk, "_vk", "inner").count()

    per_rule = {}
    aggs = [F.sum(F.when(rule_match_expr(r, raw, dec) & tagged, 1).otherwise(0)).alias(f"{rule_key(r)}__tagged")
            for r in SME_LINK_RULES]
    aggs += [F.sum(F.when(rule_match_expr(r, raw, dec), 1).otherwise(0)).alias(f"{rule_key(r)}__all")
             for r in SME_LINK_RULES]
    rr = d.agg(*aggs).collect()[0]
    for r in SME_LINK_RULES:
        k = rule_key(r)
        a, t = int(rr[f"{k}__all"] or 0), int(rr[f"{k}__tagged"] or 0)
        per_rule[k] = {"matched": a, "also_brand_tagged": t,
                       "tagged_share": (t / a) if a else None}

    emit("evar105_vs_rules", {
        "window": [START_DATE, MAX_DATE], "rsid": PIPELINE_RSID,
        "evar105_column": EVAR105,
        "predicate": "lower(evar105) contains 'ca-retirement' AND contains 'gwam' (D11 parts-match)",
        "hits": {"rows": int(row["rows"]), "brand_tagged": int(row["tagged"]),
                 "rule_matching": int(row["rule_hits"]), "both": int(row["both"]),
                 "rule_matching_but_untagged": int(row["rule_untagged"])},
        "visits": {"rule_qualified": n_q, "brand_tagged": n_t, "both": n_both,
                   "qualified_only": n_q - n_both, "tagged_only": n_t - n_both},
        "per_rule": per_rule,
        "note": (
            "Q19. If rule_matching_but_untagged is ~0 the brand tag is redundant on top of the "
            "rules and D11's predicate can retire cleanly. If it is large, the two scopes "
            "disagree and the SME must say which wins -- keeping both would AND them together "
            "and shrink the population further, on top of whatever C17 already reports. Note "
            "this is only meaningful if evar105 is populated on this suite at all; read C3's "
            "census first."),
    })


run_section("evar105_vs_rules", c18_evar105_vs_rules)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run manifest
# MAGIC
# MAGIC ⚠ `skipped` MUST be `{}`. A non-empty map means a section threw and its questions are still
# MAGIC open — do not read the run as complete coverage.

# COMMAND ----------

def c_run_manifest():
    # Byte length + sha1 of every shareable section, from the exact JSON that was
    # printed — the truncation guard the header warns about (doc-16 §0.5).
    sections = {}
    for sid, payload in RESULTS.items():
        body = json.dumps(payload, separators=(",", ":"), default=str)
        sections[sid] = {"bytes": len(body), "sha1": hashlib.sha1(body.encode("utf-8")).hexdigest()}
    emit("run_manifest", {
        "notebook": "gwam_channel_discovery",
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "table": TABLE_FQN, "window": [START_DATE, MAX_DATE], "window_days": WINDOW_DAYS,
        "sections": sections, "n_sections": len(sections),
        "skipped": SKIPPED,
        "complete": len(SKIPPED) == 0,
        "sme_channels": SME_CHANNELS,
    })


run_section("run_manifest", c_run_manifest)
