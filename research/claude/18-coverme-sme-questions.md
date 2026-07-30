# CoverMe Analytics — Questions for the Business / SME

> **STATUS (↺ updated 2026-07-29): a second round of answers received from Kerrian (SME).**
> **Answered:** Q1 targets · Q2 scope · Q3 funnel basis · Q5 bots (rule confirmed) ·
> ↺ **Q4 language** (the domain rule is approved) · ↺ **Q7 consent & personal data**
> (no PII comes from Adobe; eVar65 is *cookie* consent) · ↺ **Q8 the missing days**
> (root cause: the migration to Databricks left a source file un-refreshed).
> **Still pending with Kerrian:** **Q10 events 510-513/514** · eVar148 bot-detector
> verification · whether **eVar149** becomes the permanent language field of record ·
> per-date confirmation of the ~30 missing days.
> **Not yet raised:** Q6, Q9.
> Her answers appear inline as **A (Kerrian, date)** blocks below.
>
> **Why this round matters beyond the answers themselves:** Q7 was the last governance gate on
> the CoverMe pipeline. With no PII arriving from Adobe, the medallion build is cleared and the
> backfill job is no longer blocked on a sign-off ([17 §4 item 9](17-coverme-eda-readiness.md)).

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
  *(For our engineers: interface event N appears in the raw feed's `post_event_list` as
  N+199 — event29→228, event30→229, event33→232, event70→269, event41→240 — which is why
  docs 17 and the metric registry cite the 200-range ids for the same five events.)*
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

> **↺ A (Kerrian, 2026-07-29): ✅ ANSWERED — proceed with the domain rule.** She confirmed we can
> go with our interim rule, so **language is derived from the domain** (coverme.com = EN,
> pourmeproteger.com = FR) and that is now the approved field of record rather than a stopgap.
> **Nothing in the pipeline changes** — this is exactly what silver already computes
> ([cm_silver_lib.py](../../databricks/src/cm_silver_lib.py) `lang_from_host_expr`), so there is
> no rebuild and no number moves. eVar8 stays flagged as suspect and is **not** used.
>
> **One forward note that is worth acting on later, not now.** She added that **eVar149 should
> always be language** — the reasoning being that a French page's URL is sometimes an English
> translation, which makes the *URL* the weaker signal in principle even though it is the more
> reliable one in our sample. She will confirm eVar149 in future. So the honest state is: the
> domain rule is approved and correct to ship, and **eVar149 is the likely permanent field of
> record**. Switching later means reworking one expression and rebuilding silver — cheap, but not
> free, and it would move the EN/FR shares. We are not pre-emptively switching on a "should".

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

> **↺ A (Kerrian, 2026-07-29): ✅ ANSWERED — and this clears our last governance gate.**
> **No PII comes from Adobe**, so bringing this data into our medallion architecture is approved.
> **Consent is captured via OneTrust in eVar65 — it is consent to *cookies*, and carries no PII.**
>
> That second point matters more than it first appears, because it **reframes the opt-out question
> rather than answering it as asked.** We had measured eVar65 as ~91.7% "opt-out" and treated that
> as a potential instruction to drop those hits — which would have discarded roughly nine-tenths of
> the traffic and made anomaly detection pointless. But a *cookie*-consent preference is not an
> analytics-suppression flag, so there is nothing to honour by exclusion here: **aggregate anomaly
> KPIs may include those hits.** Our pipeline lands no consent column at all today, and on this
> answer it needs none.
>
> Note what this does **not** cover, so nobody over-reads it: the direct identifier columns
> (eVar14/172/173 User & Customer ID, eVar121 Hashed Email) are excluded from bronze by our own
> policy regardless — see `SENSITIVE_COLUMNS` in
> [coverme_bronze_columns.py](../../databricks/conf/coverme_bronze_columns.py). Her answer says
> nothing arrives that would need protecting; our belt-and-braces exclusion stays anyway.
>
> ⚠ **This is a verbal clearance, recorded here as the artifact.** If your governance process needs
> a written data-owner approval on file, this is the point to convert it into one.

**Q8. Missing days.**
~30 days across the history have no data at all. **Real outages (site down) or
gaps in the data feed?** A rough list of known outages keeps us from false alarms.

> **↺ A (Kerrian / Abhisekh, 2026-07-29): ✅ ANSWERED in substance — feed gaps, not outages.**
> The gaps trace to the **migration of this data to Databricks**: a source file was not updated
> during the move (most likely `hit_data`). So these are **collection/export gaps, not days when
> the site was down** — which is the answer that matters for us, because it means the days are
> *missing* rather than genuinely zero.
>
> Two consequences, both already the way we had guessed: the correct treatment is to
> **impute/interpolate rather than train on zeros**, and the detector must **mask those dates**
> before fitting baselines so a feed gap is never learned as normal or alerted as a drop.
>
> ⏳ **Still open:** the exact date list. We have shared our ~30 dates (clusters including
> 2023-04-09→12, 2023-12-19→21, 2025-08-05→07, 2025-12-02→07 and 2026-06-07→23) and she will come
> back confirming which are explained by the migration. Worth noting the 2026-06-07→23 cluster is
> the largest and most recent, so it is the one most likely to distort current baselines.

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
