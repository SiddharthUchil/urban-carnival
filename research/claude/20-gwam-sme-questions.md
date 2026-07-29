# Canada Retirement Analytics — Questions for the Business / SME

> **STATUS (2026-07-28): drafted, not yet sent.**
> **Blocking (3):** Q1 sign-in traffic vs the July 20 rule · Q2 the two missing report-suite IDs ·
> Q3 whether the Brand segment replaces our current filter.
> **Waiting on our own discovery run (not on you):** Q7, Q8, Q9, Q11 — a Databricks probe
> ([`eda/gwam_channel_discovery.py`](../../eda/gwam_channel_discovery.py)) answers part of each from
> the data. Send those *with* the findings attached rather than as open questions.
> **Non-blocking:** Q12.
> Answers will be merged inline below as **A (SME, date)** blocks.

> Reconciled against the four-channel alerting scope table received 2026-07-28
> (Public Website / Web Member / Mobile / ManulifeID). Every question the table already answers has
> been removed — only genuine business decisions remain. Supersedes the technical agenda in
> [19-gwam-channel-readiness.md](19-gwam-channel-readiness.md) §4; question numbers match that table
> one-to-one.

**Context for the reader:** Thank you for the scope table — it is the clearest statement of what
"Canada Retirement" means for alerting that we've had, and it changes our plan in a good way. Before
we build, there are a few things we need from you. Three of them genuinely block us; the rest are
short. Where we could answer something ourselves by looking at the data, we have, and we've said so
rather than asking you.

One thing worth flagging up front: what you've described is meaningfully **wider** than what we
monitor today. We currently watch one website, filtered by web-page addresses. You've described four
channels — including a mobile app and the sign-in system — identified by tags rather than addresses.
That's a better definition. It's also a change we can only make once, cleanly, because it resets every
historical baseline we use to decide what "normal" looks like. Hence Q3.

---

## Part 1 — The three decisions that block the build

**Q1. Is sign-in traffic now in scope?** *(blocks the build)*

On **20 July** we were given a business rule that **individual login traffic is out of scope for
anomaly detection**. We implemented it, and it is enforced everywhere — it excludes the member portal,
the Manulife ID sign-in pages, and their French equivalents.

Your new table asks us to monitor **sign-in completion rate and sign-in errors on ManulifeID**, and to
monitor the **Web Member** channel — which is the signed-in member experience. That is the same traffic
the July rule removes.

We have not changed anything: the July rule still stands in the code. **Which one governs?** If
sign-in is now in scope, we also need to know whether that reverses the rule generally, or just for
these specific metrics.

This is the largest question on the list — that excluded traffic is roughly **94%** of one of our
report suites, so the answer changes the size of the problem by an order of magnitude.

---

**Q2. What are the actual report-suite IDs for "GRS+" and "manucustomer.prod"?** *(blocks the build)*

Of the four report suites in your table, we can only confidently locate one:

| Your label | What we can find |
|---|---|
| Manulife Global Prod | ✅ `manulifeglobalprod` — this is what we monitor today |
| GBRS Mobile App - Production | 🟡 Probably `manufingbrsmobileapp.prod` — it's the **largest** report suite in our data (about 57% of all rows) and we've never used it. Can you confirm it's the same one? |
| GRS+ | ❌ We can't find a matching report suite |
| manucustomer.prod | ❌ Not present in the data we can see |

**Could you give us the exact report-suite IDs** (the short codes, as they appear in Adobe) for GRS+
and ManulifeID? If those suites aren't in the dataset we've been given access to, that's a different
problem — an access request rather than a build task — and we'd want to start it now.

---

**Q3. Does the Brand tag replace the way we currently define "Canada Retirement"?** *(blocks the build)*

Today we identify Canada Retirement traffic by **matching web-page addresses** (anything containing
`/group-retirement`, `/group-plans`, `/regimes-collectifs`). Your table instead identifies it by the
**Brand tag**: `ca-retirement :  : GWAM`.

Your way is better, and it matches our own data dictionary — that tag is documented as
`Brand | Line of Business | Segment`, and your value reads as Brand = *ca-retirement*, Line of Business
= *(blank)*, Segment = *GWAM*. It's also **language-neutral**, which would fix a known gap: our
address-matching approach has been under-counting French traffic, and that's the single biggest hole
in our current scope.

Two things we need before switching:

1. **Please confirm the tag is applied consistently** to the pages you'd expect — we'll verify the
   volumes from our side and send you the numbers.
2. **Please be aware of the cost, and sign off on it.** Switching how we define scope changes every
   historical number we've built. All our "what's normal" baselines and alert thresholds have to be
   rebuilt from scratch, and it can only be done as a single clean cutover — not gradually, because a
   partial change looks exactly like a real anomaly to the detector. We'd rather do it **once, now,
   before we tune anything**, than after.

---

## Part 2 — Confirming what we read, and the gaps

**Q4. Have we read the table correctly?**
We've assumed **1 = in scope, 0 = not in scope**, giving 17 metric-and-channel combinations. And we've
assumed each channel is alerted on **separately** — so a drop on Mobile fires even if the website is
fine — rather than the four being added together into one number. **Are both right?**

**Q5. What counts as "marketing" traffic?**
You've noted Page Views / Visits / Visitors should ideally be **non-marketing**. We don't have a
definition of marketing traffic for these sites. Is it *campaign-tagged visits* (i.e. arrivals with a
tracking code), traffic from a particular referrer type, or a specific set of pages? Any rule we can
apply consistently works — we just need to know which one you mean.

**Q6. "Page Views" — Adobe page views, or every hit?**
We currently count **every tracked interaction**, which includes things like link clicks and is a
larger number than Adobe's "Page Views" metric. Which do you want? This one materially changes the
number you'd see on a dashboard.

**Q7. Errors — which field, and what does "count" mean?**
You've asked for error counts on three channels. We have four candidate fields tagged in the data
dictionary — Error Code, Error Description, Error Type, Error Category — and a possible error event.
**Which is the one of record?** And should "count" mean *how many errors happened*, or *how many
visits were affected*? (The second is usually the more useful alert; a single broken session can throw
dozens of errors.) *We're checking which of these are actually populated and will send you what we
find.*

**Q8. "Sign in % rate completion" — what divided by what?**
We want to build exactly the number you have in mind. **What's the numerator and the denominator** —
successful sign-ins divided by sign-in attempts? And counted per **visit**, per **attempt**, or per
**person**? (People retry, so the three give noticeably different rates.) *We're checking what the
data can distinguish and will bring you options if it can't support the obvious definition.*

**Q9. Separating Retirement from other ManulifeID sign-ins — your open item.**
You flagged this yourself, and it's a real one: without a way to tell retirement sign-ins apart from
every other Manulife sign-in, we can't scope that channel to Canada Retirement.

*We're looking at every field on that data that might separate them and will send you what we find.*
If nothing in the data distinguishes them, the honest answer is that this needs a **tagging change on
the sign-in pages** — it isn't something we can solve downstream. Worth knowing early either way.

**Q10. Is "Manulife Financial" the same data feed?**
Your table lists Mobile under a different Adobe instance ("Manulife Financial") from the other three
("Manulife"). We can see a mobile-app report suite in the data we already have, which suggests it's the
same feed — but **could you confirm?** If it's a separate feed, we need to request access, and that's
a longer lead time than anything else on this list.

**Q11. What is the "Canada Retirement App Pages v2" segment?**
That's the segment name for the Mobile channel. We can't implement a segment name — we need the
**definition** (which pages or rules it includes). Could you export or describe it? *We're profiling
the app's page names in the meantime and may be able to reconstruct it.*

**Q12. Thresholds and owners.** *(not blocking — we can start with defaults)*
For each of the 17 metrics: roughly **how big a change is worth waking someone up for**, and **who
should receive the alert**? We'll start with statistical defaults and tune with you once real alerts
are flowing — this is genuinely easier to answer by reacting to a few weeks of alerts than in the
abstract.

---

*Fastest path: **Q1** (is sign-in in scope) and **Q2** (the two missing report-suite IDs) unblock the
most. **Q3** we can prepare for immediately — but it should be decided before we fit any baselines,
not after.*
