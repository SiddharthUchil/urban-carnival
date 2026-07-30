# Canada Retirement Analytics — Questions for the Business / SME

> **STATUS (2026-07-29, updated): ↺ answers received from Abhisekh — most of this list is closed.**
> He has ruled that **only the Public Website is in scope for now**, which withdraws seven of the
> twelve questions outright, and he answered Q5. What remains is short:
> **Still open (4):** **Q3b** (new — are `wealth-ca` and `pvt-wealth` part of Canada Retirement?) ·
> **Q3** (sign-off on the scope switch, now re-priced) · **Q6** (page views vs hits — *escalated to
> blocking*) · **Q12** (thresholds and owners).
> **Answered:** Q5 (marketing = the CID query parameter) · Q3 partly (brand-tag examples).
> **Withdrawn — the channels they concerned are out of scope:** Q1, Q2, Q7, Q8, Q9, Q10, Q11.
> **Shrunk to one channel:** Q4.
> **New from him, not asked by us:** three anomaly signals — see **Part 4**.
> Answers are merged inline as **A (SME, date)** blocks; withdrawn questions are marked ⬜ and kept.

> Reconciled against the four-channel alerting scope table received 2026-07-28
> (Public Website / Web Member / Mobile / ManulifeID). Every question the table already answers has
> been removed — only genuine business decisions remain. Supersedes the technical agenda in
> [19-gwam-channel-readiness.md](19-gwam-channel-readiness.md) §4; question numbers match that table
> one-to-one.
>
> **↺ 2026-07-29:** the four-channel framing below is superseded in scope but kept intact, because the
> questions are still the right ones if scope ever re-widens. Doc 19's banner explains what the
> narrowing changed structurally.

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

> ### ↺ Update after your reply (2026-07-29)
>
> **"Currently we are only going with Public Website in scope."** That changes the three paragraphs
> above, so to restate where we now are:
>
> - We are building for **one channel — the public website** — which is the channel we already monitor.
>   So this is no longer a widening; it is a **sharpening** of the definition we have.
> - **The sign-in question goes away for now.** Both channels that needed login traffic are out of
>   scope, so we are no longer asking you to revisit the July 20 rule. It stays as written. (If Web
>   Member or ManulifeID come into scope later, that question comes back exactly as it was — nothing
>   about it was settled, it just stopped applying.)
> - **We are not pursuing the ManulifeID access request.** That was the longest-lead item on the list
>   and it retires unfiled.
> - **Your marketing answer (Q5) is exactly what we needed** and we have recorded it. There is a
>   mechanical wrinkle on our side before we can apply it — see the note under that question. It does
>   not need anything from you.
> - **Your three anomaly suggestions are the most useful thing in the reply** — they are the first
>   metric ideas we have had that came from knowing the site rather than from reading a report-suite
>   list. They are in **Part 4**, with one follow-up question.
>
> Two things we still need from you, both small: **Q3b** (below — two brand tags we do not recognise)
> and **Q6** (page views vs hits, which has become more important than it looked, because your
> per-visit suggestions divide by it).

---

## Part 1 — The three decisions that block the build

> **↺ 2026-07-29: all three are closed or re-priced.** Q1 and Q2 are **withdrawn** (their channels are
> out of scope); Q3 splits into the original sign-off, still open and re-priced, plus a new **Q3b**.

**Q1. Is sign-in traffic now in scope?** ⬜ **WITHDRAWN 2026-07-29** *(was: blocks the build)*

> **A (Abhisekh, 2026-07-29).** Not applicable for now — only the Public Website is in scope, and
> neither sign-in metric nor the Web Member channel is being monitored. **We are not asking you to
> revisit the July 20 rule**, and we have not changed it: it stands exactly as written and the code
> still enforces it.
>
> One note for the record, so nobody later mistakes this for a decision: **the question was never
> answered, it stopped applying.** If Web Member or ManulifeID come into scope, the collision between
> the July 20 rule and those channels returns unchanged, along with the ~94% figure below.

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
⬜ **WITHDRAWN 2026-07-29** *(was: the access request blocks the build)*

> **A (Abhisekh, 2026-07-29).** Only the Public Website is in scope, so:
> **please disregard the access request** — we no longer need `manucustomer.prod`, and it was the
> longest-lead item on this list. The only suite that matters now is `manulifeglobalprod`, which we
> already monitor and which was never in doubt. The other two identifications stay on record as
> confirmed-from-data in case scope widens later; no confirmation needed from you today.

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
> *↺ corrected (2026-07-29 audit): the "We'd gain 1,436" figure is an undercount — the measurement
> had a defect that dropped records with no page address (the app-traffic shape) from that bucket.
> The "We'd lose" and "Both agree" figures are unaffected. We will re-run the measurement; the
> conclusion (near-no-op on the website, the case for switching is the other three channels) is
> unlikely to change, but the exact gain number will move.*
>
> **That doesn't change our recommendation — it changes the reason for it.** The case for switching
> isn't "more traffic on the website." It's that **the other three channels can't be expressed by
> web-page addresses at all** — the mobile app has no page addresses whatsoever, so no address rule
> can ever include it. We need the tag-based model for those three, and the good news is that
> adopting it costs us almost nothing on the website we already monitor.

> ### ↺ A (Abhisekh, 2026-07-29) — partly answered, and the recommendation above no longer holds
>
> You gave us three examples of how the Brand tag is applied:
>
> ```
> Manulife: GWAM: group-plans:ca-retirement
> Manulife: GWAM: wealth-ca
> Manulife: GWAM : pvt-wealth
> ```
>
> **The first one confirms what we found** — that is the second Canada-Retirement spelling we
> mentioned, so matching on the parts (`ca-retirement` + `GWAM`) rather than the whole string is the
> right approach. **The other two we do not recognise at all**; they appear nowhere in our data notes.
> That is the new **Q3b** below.
>
> **We have to withdraw our own recommendation, in fairness.** We argued for switching to the tag
> because the other three channels could not be expressed by page addresses. Those channels are now out
> of scope, so that argument is gone — and what is left is the bare trade in the table above: **gain
> ~1,436 records, lose 60,594.** On those numbers, switching is no longer something we would recommend
> on technical grounds.
>
> **It may still be the right call, for a different reason:** the tag is *your* definition of Canada
> Retirement, and it does not break when the website is restructured, whereas our address list does.
> That is a governance argument rather than a coverage one. **So the question back to you is narrower
> than before:** do you want the Brand tag to be the definition of record, accepting that we lose about
> 60,000 records' worth of pages that your tag does not currently cover? If yes, point 1 below (are
> those ~60,000 records pages you consider Canada Retirement?) becomes the thing to check first.

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

**Q3b. Are `wealth-ca` and `pvt-wealth` part of Canada Retirement?** 🚩 *(new 2026-07-29 — blocks the
scope definition)*

Two of the three Brand-tag examples you sent name things we have never seen:

| Brand tag | Our reading | Question |
|---|---|---|
| `Manulife: GWAM: group-plans:ca-retirement` | Group retirement plans — **the channel we monitor** | None; this confirms what we found |
| `Manulife: GWAM: wealth-ca` | Canadian wealth management? A **different** line of business? | **In or out of Canada Retirement?** |
| `Manulife: GWAM : pvt-wealth` | Private wealth — sounds like a separate business again | **In or out?** |

**Why we're asking rather than deciding.** If those two are simply *other* GWAM businesses that happen
to share the tag structure, including them would silently widen "Canada Retirement" to cover products
you never asked us to monitor — and every alert we raise on them would be noise to whoever owns them.
If they *are* part of the retirement product from your side, excluding them means we are under-watching
it. Either way it is your call, not a technical one.

**What we're doing meanwhile.** We have **held** our definition at group retirement only
(`ca-retirement` + `GWAM`), because that is the one you confirmed, and we are **measuring** how much
traffic the other two carry so you can see the size of the decision. If they turn out to be tiny, this
question is academic; if they are large, it matters a lot. We will have those numbers from our next data
run, and we'll send them to you rather than asking you to guess.

---

## Part 2 — Confirming what we read, and the gaps

**Q4. Have we read the table correctly?** 🟡 *(↺ mostly moot 2026-07-29)*
We've assumed **1 = in scope, 0 = not in scope**, giving 17 metric-and-channel combinations. And we've
assumed each channel is alerted on **separately** — so a drop on Mobile fires even if the website is
fine — rather than the four being added together into one number. **Are both right?**

> **↺ 2026-07-29.** Your ruling answers most of this by making it unnecessary: with one channel in
> scope there is no roll-up question, and the 17 combinations reduce to **three** — page views, visits
> and visitors on the public website. The only part still worth a word: your table marks Errors as
> **0** for the public website, so we are **not** building an error metric. If you did want website
> errors watched, that would be a change to the table rather than a reading of it.

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

> ### ✅ A (Abhisekh, 2026-07-29) — ANSWERED
>
> > "We are capturing **CID campaign identifier — query string**. Campaign ID. It is the standard query
> > string parameter appended to marketing URLs."
>
> That is the rule, and it is close to what we guessed — marketing traffic is traffic that **arrives
> with a `CID` tracking code in the web address**. Recorded. Nothing further needed from you.
>
> **One wrinkle on our side, flagged for transparency rather than as a question.** We do not currently
> keep the part of the web address where `CID` lives: we deliberately discard everything after the `?`
> because session tokens can appear there, and that is a privacy decision we made early. We hold
> Adobe's own "tracking code" field instead, which is populated on 57% of public-website records and is
> *normally* filled from a parameter like `cid` — but we have not verified that they are the same thing
> on this report suite. So we are doing two things: **checking whether the field we hold matches the
> `CID` parameter** in our next data run, and if it does not, deciding whether to start extracting just
> the `CID` value at load time (which needs a small amendment to our privacy design, not a new
> question for you).
>
> **Until one of those lands, our page-view / visit / visitor counts include marketing traffic.** We
> would rather tell you that than quietly ship a number that does not match your definition. Your
> wording — "*ideally* non-marketing" — suggests that is an acceptable starting point; tell us if not.

**Q6. "Page Views" — Adobe page views, or every hit?** 🚩 *(↺ escalated 2026-07-29 — now blocks two
metrics)*
We currently count **every tracked interaction**, which includes things like link clicks and is a
larger number than Adobe's "Page Views" metric. Which do you want? This one materially changes the
number you'd see on a dashboard.

> **↺ Why this got more important (2026-07-29).** When we sent this it was a question about what a
> dashboard number means. Your anomaly suggestions in **Part 4** are both *page views per visit* — so
> "page views" is now the top half of a fraction, and the answer changes whether "less than 1" or
> "consistently 2" mean anything at all. If we count every interaction, a typical visit has well over
> 2 and the "consistently 2" signal would never fire; if we count Adobe page views, 2 is an ordinary
> number and the signal is meaningful. **We need your answer before we can set any threshold on those
> two.** In the meantime we are measuring the distribution under *both* definitions so that whichever
> you pick, we have the numbers ready.

> **↺ 2026-07-29 — Q7 through Q11 are all WITHDRAWN.** Every one of them concerns Errors, sign-in, the
> mobile app, or ManulifeID, and all of those left scope with the single-channel ruling. **No answers
> needed.** They are kept in full below, findings and all, for one reason: if scope ever widens, this is
> the research already done — including two useful negative results (the sign-in fields are empty on
> Canada; the error event belongs to John Hancock) that would otherwise be rediscovered the hard way.

**Q7. Errors — which field, and what does "count" mean?** ⬜ **WITHDRAWN 2026-07-29**
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

**Q8. "Sign in % rate completion" — what divided by what?** ⬜ **WITHDRAWN 2026-07-29**
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

**Q9. Separating Retirement from other ManulifeID sign-ins — your open item.** ⬜ **WITHDRAWN 2026-07-29**
You flagged this yourself, and it's a real one.

> **What we found — we couldn't even look.** The ManulifeID data set isn't in our feed (Q2), so there
> were no records to profile. We can't answer whether a field separates retirement sign-ins from the
> rest until access is granted.
>
> Worth setting expectations now, though: this stays a live risk *after* access. If nothing in that
> data distinguishes retirement sign-ins, the honest answer is that it needs a **tagging change on
> the sign-in pages** — not something we can solve downstream. If there's anyone on the ManulifeID
> side who'd know the answer without waiting for the access request, that would save us a cycle.

**Q10. Is "Manulife Financial" the same data feed?** ⬜ **WITHDRAWN 2026-07-29**
Your table lists Mobile under a different Adobe instance ("Manulife Financial") from the other three
("Manulife"). **Could you confirm?**

> **What we found.** The mobile app data **is** in the feed we already have — 881 days of it, going
> back to January 2024. So in practice this looks like a labelling difference rather than a separate
> feed, and it isn't blocking us. We'd still like it confirmed, because if the instance really is
> separate we may be looking at a partial copy without knowing it.

**Q11. What is the "Canada Retirement App Pages v2" segment?** ⬜ **WITHDRAWN 2026-07-29**
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

> **↺ 2026-07-29:** now **five** metrics on one channel rather than 17 across four — the three traffic
> counts plus the two per-visit signals from Part 4. Still not blocking, and now a much shorter
> conversation.

---

## Part 4 — ↺ Your three anomaly suggestions (new 2026-07-29)

These came from you rather than from us, and they are the most useful thing in the reply — the first
metric ideas we have had that come from knowing how the site behaves rather than from reading a list of
what Adobe collects. Here is what we can and cannot do with each.

**1. "Unique ECID — unique visitor."**
✅ **Already built.** This is exactly what our "visitors" metric counts — the ECID is the identifier we
use. No work needed.

> One internal inconsistency your suggestion exposed, worth mentioning because it could otherwise make
> two of our own numbers disagree: our production pipeline counts visitors by ECID (what you named),
> while our exploratory analysis notebooks count them by a different Adobe visitor identifier. The two
> are usually close but not identical. We are measuring the gap and will standardise on ECID, since
> that is what you asked for. Nothing needed from you.

**2. "Page view per visit — if page view < 1 could be an anomaly."**
🟡 **Building it; one thing to note about what it detects.** A per-visit average can only drop below 1
if some visits contain **no page view at all** — so this is really a detector for *visits that record
activity but no page*. That is a genuinely good signal (it usually means either tracking is
half-broken or something non-human is hitting the site), and we are measuring how often it happens
today so we can set a sensible threshold. Depends on your Q6 answer.

**3. "If all pages are consistently at 2 — sometimes an indicator of duplication, especially when we
see consistently 2."**
🟡 **Building it, and this one is the most interesting.** We read your emphasis on *consistently* as
the actual signal: it is not that 2 is a bad number, it is that a **stable** 2 suggests every page is
being counted twice rather than people genuinely viewing two pages. That is a different shape of alert
from everything else we run — our detectors look for values that *move*, and this one looks for a
value that *stops moving*. We are building the measurement now and will need to add a new kind of check
for the "unusually steady" part. Also depends on Q6.

**A question back on this one:** have you seen the doubling before, and if so **where** — a particular
set of pages, a particular period, after a tag release? If you can point us at even one known instance,
we can calibrate against a case you already believe is duplication rather than guessing what counts as
"consistently."

---

*Fastest path, ↺ updated 2026-07-29 after your ruling: **Q3b** (are `wealth-ca` and `pvt-wealth` part
of Canada Retirement?) is now the top item — it defines what we monitor, and we cannot hold our
definition provisionally forever. **Q6** (page views vs hits) is next, because your two per-visit
suggestions both divide by it. **Q3** (Brand tag as the definition of record) should still be decided
before we fit baselines rather than after, though with the other channels out of scope it is now a
governance choice rather than a coverage one.*

*Nothing else on this list needs you. Q1, Q2 and Q7–Q11 are withdrawn; Q5 is answered; Q4 and Q12 we
can proceed on with the assumptions stated above — they are written down explicitly so you can override
them rather than having to answer them.*
