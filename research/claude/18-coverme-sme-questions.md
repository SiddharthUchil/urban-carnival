# CoverMe Analytics — Questions for the Business / SME

> **STATUS (2026-07-27): answers received from Kerrian (SME).**
> **Answered:** Q1 targets · Q2 scope · Q3 funnel basis · Q5 bots (rule confirmed).
> **Pending with Kerrian:** Q4 language field · eVar148 bot-detector verification ·
> **Q10 events 510-513/514** (new — surfaced by the post-E1 decode).
> **Not yet raised:** Q6-Q9 (sent as a non-blocking attachment).
> Her answers appear inline as **A (Kerrian, 2026-07-27)** blocks below.

> Send-ready shortlist, reconciled against the business data map
> **`CoverMeDataMap.xlsx`** (tabs: `data_feed_columns`, `post_eVar`, `post_prop`,
> `post_event_list`). We used **Status = Enabled** variables only and treated the
> **`Notes = "Anomaly Detection"`** flags as the sanctioned target set. Every
> question the map already answers has been removed — only genuine business
> decisions remain. Supersedes the technical agenda in
> [17-coverme-eda-readiness.md](17-coverme-eda-readiness.md) §4.

**Context for the reader:** We've profiled ~3.4 years of CoverMe web data (57.7M
visits) and cross-checked it against your Adobe data map. The map resolved the
event and field definitions we were missing — thank you. What's left is a short
set of **business decisions** the map doesn't capture. Most are a sentence or a
yes/no.

---

## Part 1 — What we've taken from your data map (please just confirm)

Using only **Enabled** variables, we've locked in:

- **The quote → application funnel** (your five Anomaly-Detection-flagged events):
  **Quote Start** (event29) → **Quote Complete** (event30) → **Save Quote**
  (event33) → **App Start** (event70) → **App Confirm** (event41).
- **The variables you flagged for Anomaly Detection:** Product Category (eVar4),
  Product ID (eVar5), Sponsor/Distributor (eVar6), Quote Session ID (eVar11),
  Transaction ID (eVar16), Current Page (eVar52), Experience Cloud ID (eVar111),
  Bot Detector (eVar148) — plus the flagged feed columns: campaign, geo_city,
  geo_country, geo_region, os, page_url, pagename, referrer, user_agent.

**Q1. Is this Anomaly-Detection target list complete and current?**
Anything to add or drop — and is there a priority order (which matter most to you)?

> **A (Kerrian, 2026-07-27): ✅ Complete and current** (note some targets are also marked on the
> `data_feed_columns` tab — already in our set). **Priority = the transactional events** (Quote
> Start, Quote Complete, Save Quote, App Start, App Confirm), **paired with the eVars as
> breakdowns** — e.g. the funnel events by Product Category (`post_evar4`). Also flagged: other
> feed columns are needed to calculate certain events per the Adobe documentation (follow-up:
> map those calculated-metric dependencies).

---

## Part 2 — Decisions the data map doesn't cover

**Q2. Scope — what counts as "CoverMe"?**  *(blocks the build)*
The map doesn't define in-scope sites. Traffic is mostly **coverme.com** (EN),
**pourmeproteger.com** (FR), and **insttrip** — but 300+ hosts exist, plus
test/UAT sites and some older life-insurance pages. **Which sites/sections are
in-scope — and are the older life pages and `insttrip` (travel?) included?**

> **A (Kerrian, 2026-07-27): ✅ `coverme.com` and `pourmeproteger.com` are the only two
> currently valid domains. Everything else is either legacy or dev.** → Go-forward detector
> scope = the 2 prod domains; the ~71k legacy life-insurance hits and unclassified prod-adjacent
> hosts are OUT; `insttrip` is retired (its history to 2024-03-11 stays in the archive for
> baseline context only).

**Q3. How should we count a conversion?**
For the funnel, do you count **visits** that reach each step, or **unique
people**? Is there an official conversion-rate you report today? (The map has a
"Conductor Denominator" event — is that the intended denominator?)

> **A (Kerrian, 2026-07-27): ✅ Visits.** Unique visitors are unreliable (cookies / browser
> settings), so visit-level calculation is acceptable. **Save Quote is an optional step** — no
> save needed to proceed to application. Crucially: **a visitor can return and start their
> application from a saved quote**, so that visit carries App Start (and possibly App Confirm)
> with **no quote events at all** → the funnel is non-monotonic across visits; step ratios are
> population-level proxies, never within-visit sequences.

**Q4. Which is the language field of record?**
The map has three — **eVar8**, **eVar149**, and **prop5** (plus the browser's
Accept-Language). Traffic is ~50/50 English/French. **Which do we trust?**

> **A (Kerrian, 2026-07-27): ⏳ Investigating** — we flagged that eVar8 reports ~96% EN while
> ~half of traffic is on the French domain, so it may be mis-tagged. She will confirm which
> field to use; "they should ideally align based on the domain." **Interim rule until her
> ruling: derive language from domain** (coverme.com = EN, pourmeproteger.com = FR).

**Q5. How do we exclude bots?**
The map offers three signals — the **`exclude_hit`** feed flag, **eVar116**
(Bot Traffic), and **eVar148** (Bot Detector). **Which is authoritative, and do
you already exclude bots in your own reporting?**

> **A (Kerrian, 2026-07-27): ✅ Dropping `exclude_hit > 0` is correct.** ⏳ She is also
> confirming with the implementation team that the **eVar148 Bot Detector** works correctly —
> its purpose is to catch bots that slip past Adobe's built-in filters. Until confirmed, treat
> eVar148/eVar116 as corroboration signals, not the exclusion rule.

**Q6. Customer / user identity.**
The map shows **Customer ID (eVar173)** and **User ID (eVar14 / eVar172)** as
**Enabled**, but our data sample only shows anonymous visitor IDs. **Are the
customer/user IDs actually populated (e.g. for logged-in users), and may we use
them?** This decides whether we can follow people or only browsers.

**Q7. Consent / personal data.**
Consent is captured (OneTrust — eVar65 / eVar81) and a **Hashed Email ID**
(eVar121) exists. **Must we honor opt-out for analytics, and is there anything
we're not allowed to store or analyze?**

**Q8. Missing days.**
~30 days across the history have no data at all. **Real outages (site down) or
gaps in the data feed?** A rough list of known outages keeps us from false alarms.

**Q9. US traffic.**
~12% of visits come from the **USA** (vs 84% Canada). **Is that expected**, or a
sign of bots / a specific product (e.g. travel)?

**Q10. Events 510–513 (and 514) — what are they?** *(added 2026-07-27)*
Surfaced by the verified post-E1 `event_decode`: events **510/511/512/513 each fire
on ≈ 43.5% of all hits** (514 on 5.3%) but appear in neither the data map nor the
notebook dictionaries — they carry the literal label
`unknown — resolve via CoverMe event dictionary`. They are among the most common
things happening on the site.

> **A (Kerrian, 2026-07-27): ⏳ Investigating** — she will confirm what they are and
> get back to us.

---

*Fastest path: **Q1** (confirm the targets) and **Q2** (scope) unblock the build
immediately.* **Both answered 2026-07-27 — the build is unblocked.**
