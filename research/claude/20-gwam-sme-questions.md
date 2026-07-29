# Canada Retirement Analytics — Questions for the Business / SME

> **STATUS (2026-07-29): send-ready — our discovery run is complete.**
> The Databricks probe ([`eda/gwam_channel_discovery.py`](../../eda/gwam_channel_discovery.py)) ran
> clean on 2026-07-29 over 90 days of data, and its findings are folded into the questions below as
> **"What we found"** blocks. Nothing here is an open question we could have answered ourselves.
> **Blocking (3):** Q1 sign-in traffic vs the July 20 rule · Q2 **access to the ManulifeID data**
> (changed — see below) · Q3 sign-off on the scope switch.
> **Answered by the probe, now only needing confirmation:** Q7, Q11 · **answered negatively:** Q9.
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
short. Where we could answer something ourselves by looking at the data, we have — and we've put what
we found under each question rather than asking you.

**Good news first.** We went and looked for your four channels in the data we hold, and **we found
three of them**, including two we had never used before. Your Web Member channel in particular was
easy to confirm: the "MPS Member" tag you named accounts for every tagged record in that data set,
which is about as clean a match as we could hope for. The mobile app turns out to be the **single
largest** source of data we have — roughly two-thirds of everything — and we have never touched it.

**The one exception is ManulifeID.** That data set is not in the feed we've been given, at all. That
turns Q2 from "what's the ID?" into "please start an access request", and because access takes longer
than anything else on this list, it's the one we'd like to start today.

One thing worth flagging up front: what you've described is meaningfully **wider** than what we
monitor today. We currently watch one website, filtered by web-page addresses. You've described four
channels — including a mobile app and the sign-in system — identified by tags rather than addresses.
That's a better definition. It's also a change we can only make once, cleanly, because it resets every
historical baseline we use to decide what "normal" looks like. Hence Q3 — though our measurements
suggest that switch is **much cheaper than we feared**.

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

> **What we found.** The question is now concentrated on the **Web Member** channel. Because the
> ManulifeID data isn't in our feed at all (Q2), we couldn't build those metrics today even if the
> rule were lifted tomorrow — so the live collision is Web Member, which is a substantial data set
> (322 million records, nearly 900 days of history). We also found that the sign-in journey is
> partly visible *inside* the Web Member data — about 1.2 million sign-in page views and 2.3 million
> account-selection steps in the last 90 days. That's a real option for the sign-in metrics you asked
> for, and it's exactly the traffic the July rule removes. So the cost of answering "the July rule
> stands" is higher than it looked: it would take the sign-in metrics off the table entirely, not
> just narrow them.

---

**Q2. Please confirm three report suites — and start an access request for the fourth.**
*(the access request blocks the build)*

We searched the whole data set and found three of your four channels:

| Your label | What we found | What we need |
|---|---|---|
| Manulife Global Prod | ✅ `manulifeglobalprod` — what we monitor today. Note it only holds **138 days** of history, from 10 March 2026. | Nothing |
| GBRS Mobile App - Production | ✅ `manufingbrsmobileapp.prod` — the **largest** data set we have (about **69%** of all records), 881 days of history, never used. | Please confirm it's the same one |
| GRS+ | ✅ **`manugrs`** — we're confident. Your own "MPS Member" tag covers **100%** of the tagged records in it, and just over half the data set overall. | Please confirm the name matches |
| manucustomer.prod | ❌ **Not in our feed — zero records.** Not a naming problem; the data simply isn't there. | **An access request, please** |

> **What we found.** The first three are settled from data, so the confirmations above are a
> formality — we'll proceed on them unless you tell us otherwise. **ManulifeID is the real blocker.**
> We can't profile it, can't scope it, and can't build any of its four metrics until someone grants
> access to that data set. Access requests take longer than anything else on this list, so **could
> you start that one now**, independently of the rest of these answers?

---

**Q3. Does the Brand tag replace the way we currently define "Canada Retirement"?** *(blocks the build)*

Today we identify Canada Retirement traffic by **matching web-page addresses** (anything containing
`/group-retirement`, `/group-plans`, `/regimes-collectifs`). Your table instead identifies it by the
**Brand tag**: `ca-retirement :  : GWAM`.

Your way is better, and it matches our own data dictionary — that tag is documented as
`Brand | Line of Business | Segment`, and your value reads as Brand = *ca-retirement*, Line of Business
= *(blank)*, Segment = *GWAM*.

> **What we found — and one correction to our own thinking.**
>
> **Your tag is real.** `ca-retirement :  : GWAM` appears exactly as you wrote it, on about 777,000
> records in the last 90 days. (There's a second Canada-Retirement spelling too —
> `Manulife : GWAM : group-plans:ca-retirement` — so we'll match on the parts, not the whole string.)
>
> **But the switch turns out to be nearly a no-op on the website — in both directions.** We measured
> both definitions side by side over the same 90 days on the public website:
>
> | | Records |
> |---|---|
> | What we monitor today (address matching) | 1,418,435 |
> | Your tag | 1,304,325 |
> | Both agree | 1,302,889 |
> | **We'd gain** | **1,436** |
> | **We'd lose** | **60,594** |
>
> The two agree on about **96%** of the traffic. We'd expected your tag to be *wider* — specifically,
> we thought it would fix a French-language gap. It doesn't; our address list already covers the
> French pages, so on this channel the tag is marginally *narrower*.
>
> **That doesn't change our recommendation — it changes the reason for it.** The case for switching
> isn't "more traffic on the website." It's that **the other three channels can't be expressed by
> web-page addresses at all** — the mobile app has no page addresses whatsoever, so no address rule
> can ever include it. We need the tag-based model for those three, and the good news is that
> adopting it costs us almost nothing on the website we already monitor.

Two things we need before switching:

1. **Please confirm the tag is applied consistently** to the pages you'd expect. The ~60,000 records
   we'd lose are the ones to look at — if those are pages you *do* consider Canada Retirement, the tag
   has gaps we should fix before switching rather than after.
2. **Please be aware of the cost, and sign off on it.** Switching how we define scope changes every
   historical number we've built. All our "what's normal" baselines and alert thresholds have to be
   rebuilt from scratch, and it can only be done as a single clean cutover — not gradually, because a
   partial change looks exactly like a real anomaly to the detector. We'd rather do it **once, now,
   before we tune anything**, than after. Our measurements say this is a cheap change *today* and an
   expensive one in three months.

---

## Part 2 — Confirming what we read, and the gaps

**Q4. Have we read the table correctly?**
We've assumed **1 = in scope, 0 = not in scope**, giving 17 metric-and-channel combinations. And we've
assumed each channel is alerted on **separately** — so a drop on Mobile fires even if the website is
fine — rather than the four being added together into one number. **Are both right?**

**Q5. What counts as "marketing" traffic — on the public website?**
You've noted Page Views / Visits / Visitors should ideally be **non-marketing**. Is it
*campaign-tagged visits* (i.e. arrivals with a tracking code), traffic from a particular referrer
type, or a specific set of pages? Any rule we can apply consistently works.

> **What we found — this only matters for one channel.** Campaign tracking codes are present on
> **57%** of public-website records, but on only **0.5%** of Web Member and **0.02%** of Mobile.
> That makes sense: those two are signed-in experiences, so they're **non-marketing by
> construction**. So we only need a rule for the public website, and the other three channels need
> no filter at all. Unless you say otherwise, we'll take "non-marketing" to mean *"no campaign
> tracking code"* on the public website and *"everything"* elsewhere.

**Q6. "Page Views" — Adobe page views, or every hit?**
We currently count **every tracked interaction**, which includes things like link clicks and is a
larger number than Adobe's "Page Views" metric. Which do you want? This one materially changes the
number you'd see on a dashboard.

**Q7. Errors — which field, and what does "count" mean?**
**Which of the four error fields is the one of record?** And should "count" mean *how many errors
happened*, or *how many visits were affected*? (The second is usually the more useful alert; a single
broken session can throw dozens of errors.)

> **What we found — good news: this is buildable.** We'd assumed error data might not exist. It does,
> and at real volume. On **Web Member**, three of the four fields are well populated — Error Code on
> 52% of records (12.2M), Error Description 70% (16.4M), Error Category 61% (14.4M). On **Mobile**,
> Error Category is on 18% of records (37.6M) — the largest error footprint we have anywhere.
>
> Two things we ruled *out*, both of which we'd previously assumed were in play: the fourth field
> (**Error Type**) is essentially unused on your channels, and the dedicated **error event** we
> expected to use turns out to belong to the John Hancock and investments products. Neither is a
> Canada Retirement signal.
>
> One honest caveat: our profiling listed the most common error *values* across all products
> together, so we can't yet tell you which specific messages belong to *your* channels. We're
> running a short follow-up query for that. What we can already say is that **Error Code is mostly
> the literal text "N/A"** on Web Member (about 72% of the time), which makes it a poor choice for
> the metric.
>
> So all we need from you is **which field** and **which grain**. Our suggestion, unless you prefer
> otherwise: **Error Description** as the primary, counted as **affected visits**.

**Q8. "Sign in % rate completion" — what divided by what?**
**What's the numerator and the denominator** — successful sign-ins divided by sign-in attempts? And
counted per **visit**, per **attempt**, or per **person**? (People retry, so the three give noticeably
different rates.)

> **What we found — the obvious fields are empty, so we need your definition more than usual.** We
> expected two fields ("Login Step" and "Login Method") to mark attempts and successes. Both are
> **completely unpopulated** on the Canada Retirement data — they're only used by the US John Hancock
> products. So we can't derive this the easy way.
>
> What we *can* see is the sign-in journey as a sequence of pages. In the last 90 days, on Web
> Member: **1,160,058** sign-in page views, followed by **2,312,927** account-selection steps (the
> page people reach once signed in). On Mobile there's a single **"CIAM Sign In"** step with 9.3
> million views. So a workable definition would be *"people who reached the post-sign-in page ÷
> people who reached the sign-in page, per visit."*
>
> There's also one lead we haven't finished chasing: your data dictionary defines dedicated
> **"Login Start", "Login Complete" and "Login Error"** measures, which would be a much cleaner
> basis for this than counting pages. They didn't show up among the most frequent measures on any
> of your channels, but our scan only captured the top 25, so they may simply be less common rather
> than absent. **We're checking those specifically** — if they're there, we'd recommend using them
> instead, and this question gets easier.
>
> **So: is the page-based definition the number you have in mind, as a fallback?** And if you meant
> something closer to *successful authentications ÷ authentication attempts* at the system level,
> that isn't in this data at all and we'd need to talk about where it lives.

**Q9. Separating Retirement from other ManulifeID sign-ins — your open item.**
You flagged this yourself, and it's a real one.

> **What we found — we couldn't even look.** The ManulifeID data set isn't in our feed (Q2), so there
> were no records to profile. We can't answer whether a field separates retirement sign-ins from the
> rest until access is granted.
>
> Worth setting expectations now, though: this stays a live risk *after* access. If nothing in that
> data distinguishes retirement sign-ins, the honest answer is that it needs a **tagging change on
> the sign-in pages** — not something we can solve downstream. If there's anyone on the ManulifeID
> side who'd know the answer without waiting for the access request, that would save us a cycle.

**Q10. Is "Manulife Financial" the same data feed?**
Your table lists Mobile under a different Adobe instance ("Manulife Financial") from the other three
("Manulife"). **Could you confirm?**

> **What we found.** The mobile app data **is** in the feed we already have — 881 days of it, going
> back to January 2024. So in practice this looks like a labelling difference rather than a separate
> feed, and it isn't blocking us. We'd still like it confirmed, because if the instance really is
> separate we may be looking at a partial copy without knowing it.

**Q11. What is the "Canada Retirement App Pages v2" segment?**
We can't implement a segment name — we need the **definition** (which pages or rules it includes).

> **What we found — we think we've reconstructed it; please check our work.** The app's page names
> sort cleanly into product lines by their opening word: **`GB`** pages are Group Benefits (claims,
> coverage), **`MM`** is the app shell, **`CIAM Sign In`** is sign-in, and **`MPS`** is the retirement
> platform — matching the "MPS Member" tag you gave us for Web Member. The MPS pages are things like
> *MPS Account Balances* (8.1M views) and *MPS Transaction/Contribution History* (1.5M).
>
> So our proposed rule is: **app pages whose name begins with "MPS"**. Could you either confirm that,
> or export the real segment definition so we can check it against ours? A prefix rule is a good
> approximation but it will miss any page the real segment includes under a different name.

**Q12. Thresholds and owners.** *(not blocking — we can start with defaults)*
For each of the 17 metrics: roughly **how big a change is worth waking someone up for**, and **who
should receive the alert**? We'll start with statistical defaults and tune with you once real alerts
are flowing — this is genuinely easier to answer by reacting to a few weeks of alerts than in the
abstract.

---

*Fastest path, updated after our discovery run: **Q2's access request for ManulifeID** should be
started today — it has the longest lead time and nothing else can shorten it. **Q1** (is sign-in in
scope) unblocks the most build work. **Q3** should be decided before we fit any baselines, not after —
and we now know it's a cheap switch, so there's little reason to defer it.*

*Everything else on this list we can proceed on with our own assumptions if we don't hear back — we've
stated each one explicitly above so you can override it rather than having to answer it.*
