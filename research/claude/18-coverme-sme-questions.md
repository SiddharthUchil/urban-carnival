# CoverMe Analytics — Questions for the Business / SME

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

---

## Part 2 — Decisions the data map doesn't cover

**Q2. Scope — what counts as "CoverMe"?**  *(blocks the build)*
The map doesn't define in-scope sites. Traffic is mostly **coverme.com** (EN),
**pourmeproteger.com** (FR), and **insttrip** — but 300+ hosts exist, plus
test/UAT sites and some older life-insurance pages. **Which sites/sections are
in-scope — and are the older life pages and `insttrip` (travel?) included?**

**Q3. How should we count a conversion?**
For the funnel, do you count **visits** that reach each step, or **unique
people**? Is there an official conversion-rate you report today? (The map has a
"Conductor Denominator" event — is that the intended denominator?)

**Q4. Which is the language field of record?**
The map has three — **eVar8**, **eVar149**, and **prop5** (plus the browser's
Accept-Language). Traffic is ~50/50 English/French. **Which do we trust?**

**Q5. How do we exclude bots?**
The map offers three signals — the **`exclude_hit`** feed flag, **eVar116**
(Bot Traffic), and **eVar148** (Bot Detector). **Which is authoritative, and do
you already exclude bots in your own reporting?**

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

---

*Fastest path: **Q1** (confirm the targets) and **Q2** (scope) unblock the build
immediately.*
