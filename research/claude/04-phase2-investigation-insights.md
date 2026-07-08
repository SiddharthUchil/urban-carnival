# 04 — Phase 2: Investigation & Insights (Offline + Online)

> Turns a detected anomaly into **what changed, where, when, why, and what to do**. Detection upstream is
> [03](03-phase1-anomaly-detection.md); the Gen-AI agent/prompt/guardrail detail and Akka path are in
> [05-genai-and-akka.md](05-genai-and-akka.md); diagram D3 in [06-diagrams.md](06-diagrams.md).

## 1. Scope

Phase 2 = **automated root-cause analysis (RCA) + actionable recommendations**, correlating anomalies with
**deployments, tagging changes, campaigns, outages, and business trends**, and building **institutional
memory**. Like Phase 1 it has an **offline** plane (deep, thorough) and an **online** plane (fast triage).

## 2. The investigation pipeline (both planes share these stages)

```
anomaly → (a) localize: which dimension values moved?  → (b) correlate: what changed near then?
        → (c) hypothesize + rank causes  → (d) recommend actions  → (e) narrate + record
```

### (a) Localize — contribution analysis (kept from Gemini, made rigorous)
Rank which dimension values (eVar journey-stage, prop page/content, geo, `browser`/`os`, `mobile`, channel)
drove the shift, using **Adobe-style Contribution Analysis**:

- **Cramér's V** — strength of association between the anomaly (period: normal vs. anomalous) and each
  dimension, from the χ² contingency table: `V = sqrt( χ² / (n · min(r-1, c-1)) )`, in [0,1]. Ranks *which
  dimension* matters.
- **Pearson standardized residuals** — per cell `r_ij = (O_ij − E_ij) / sqrt(E_ij)` (with finite-sample
  correction); `|r| > ~2–3` flags *which specific value* (e.g., `browser = Safari 17.4`) over/under-fired.
- **SHAP** — for continuous engineered features where contingency tables don't apply.
- **Scalability:** for high-cardinality dims, pre-aggregate and screen top contributors (offline does the
  full scan; online does a localized top-k on the recent window — see §4).

### (b) Correlate — change-event join
Time-window join of the anomaly against a `change_events` table (deployment/release logs, CMS/tag-manager
publishes, campaign launches, Azure platform/outage signals, known incidents). A change within the
lead-window before onset becomes candidate evidence (with lag tolerance).

> **⚠ Evidence-base gap (2026-07-02 profiling).** The current corpus contains **zero** deployment logs,
> tag-change history, release notes, campaign calendars, or CMS change feeds, and **11 expected
> correlation keys are absent** from the hit schema: `url`, `page_path`, `deployment_id`, `release_id`,
> `campaign_id`, `content_id`, `tag_name`, `owner`, `site_id`, `brand`, `region`
> ([10](10-data-profile-alignment.md)). The `change_events` join is therefore **design-forward** — the
> stage is specified and schema-ready, but **acquiring these sources/keys is a Phase-2 entry criterion**.
> Until they exist, stages (a) localize and (c)–(e) run without change correlation, and narratives must
> abstain from deployment/campaign causal claims.

**External environment signals** (macroeconomic indicators, regulatory/policy events, market-wide
calendars) are a candidate **Phase-3** `change_events` class — useful to separate market-wide shifts
from platform/tagging issues, but deferred under the same acquisition gating as the sources above
(assessment in [11 §1](11-privacy-identity-governance.md)).

### (c)–(e) Hypothesize, recommend, narrate — Gen-AI multi-agent (see [05](05-genai-and-akka.md))
A **Supervisor** orchestrates specialized agents over the localized + correlated evidence, then a Narrative
agent writes the report. **The LLM never invents numbers** — it composes only over retrieved quantitative
evidence (grounding guardrails in [05 §4](05-genai-and-akka.md)). These agents run on an
**Adaptive-ML-tuned specialist SLM** (frontier LLM for cold-start / hardest cases), continuously improved
from analyst feedback — see [07](07-adaptive-ml-integration.md).

## 3. Offline plane (deep investigation + memory)

Triggered on confirmed batch anomalies (and to enrich earlier online flags):

1. **Full contribution analysis** across all dimensions (Cramér's V ranking → Pearson residual drill-down).
2. **Full change-event correlation** + historical comparison ("similar to INC-… last quarter").
3. **Multi-agent RCA** (Supervisor → Data/SQL, Change/System-Status, Contribution, Narrative agents).
4. **RAG over institutional memory** — retrieve similar past anomalies, causes, and resolutions from a
   **Vector Search** index over `anomaly_insights` + runbooks; ground hypotheses and recommendations in them.
5. **Outputs:** structured report → `anomaly_insights` (Delta) + **Databricks AI/BI dashboard**; for
   actionable incidents, auto-draft a **ServiceNow/Jira** ticket (AI-written description, human approves).

### `anomaly_insights` report schema (structured, validated)
`anomaly_id` · `summary` · `what`/`where`/`when` · `ranked_hypotheses`[{cause, evidence_refs, confidence}] ·
`recommended_actions`[{action, rationale, owner_hint}] · `correlated_changes`[] · `similar_incidents`[] ·
`evidence`(quantitative refs) · `confidence_overall` · `needs_human_review`(bool) · `model`/`prompt_version`.

### Example (illustrative, schema-faithful)
> **INC-2026-0142 — Quote-start drop, mobile Safari.** Quote-start events fell **−38%** vs. expected
> (2026-06-28 14:00–16:00 ET), concentrated in `browser = Safari 17.4` / `mobile = 1` (**Cramér's V 0.79**,
> Pearson residual **−13.6**). Correlates with **frontend release `web-cms@2026.06.28`** 22 min before onset.
> **Top hypothesis (0.82):** JS error in the release breaks quote-start tag on Safari 17.4. **Actions:**
> (1) roll back/patch the release; (2) verify quote-start tag in Safari 17.4; (3) CDN cache flush. Similar to
> INC-2026-0091 (tag regression). *Evidence: contribution table, release log, event-volume series.*

## 4. Online plane (fast triage)

On **severe** anomalies flagged by the scheduled micro-batch plane only (severity gate — cost & noise
control; cadence follows the feed, [ADR-0001 v2](adr/adr-0001-near-real-time-microbatch.md)):

1. **Fast localize:** top-k dimension residual shift on the most recent scoring window (cheap subset of §2a).
2. **Recent-change lookup:** `change_events` within the lead-window for the affected segment (once the
   change-event sources exist — see §2b gap).
3. **Concise Gen-AI narrative:** a single focused prompt → *"explain in one paragraph + top-3 likely causes +
   top-3 actions,"* grounded strictly on the localized evidence + retrieved runbook snippets.
4. **ChatOps:** post an anomaly card to **Teams/Slack**; analysts ask follow-ups ("show related anomalies",
   "compare to last week") — the bot re-queries the lakehouse. Low-confidence/empty-evidence cases **abstain**
   and route to a human rather than guess.

The online narrative is **provisional**; the offline plane later produces the authoritative report and
reconciles.

## 5. Recommendations & actionable insights (Phase-2 deliverable per README)

- **Runbook Advisor**: maps the ranked cause to recommended actions using a runbook/playbook RAG corpus +
  past resolutions — so recommendations are grounded in what actually worked before, not generic advice.
- **Owner routing**: severity + class + affected system → suggested owner/queue (martech vs. platform vs.
  marketing) for "calmer triage with enough context to act."
- **Institutional memory**: every confirmed insight + resolution feeds back into the Vector Search corpus,
  compounding RCA quality over time (a stated README success criterion).
- **Self-improving model**: analyst **confirm / dismiss / edit** actions on `anomaly_insights` are logged via
  **Adaptive Engine's Metrics-logging API** as preference/reward data, continuously RL-tuning the specialist
  RCA SLM (PPO/GRPO/DPO). The loop that grows the KB *also* trains the model — see
  [07](07-adaptive-ml-integration.md).

## 6. Guardrails (summary; full detail in [05 §4](05-genai-and-akka.md))

Grounding on retrieved quantitative evidence only · atomic-claim verification + **abstention** when
unsupported · **structured JSON output** validated against the report schema · **confidence gating** (low
confidence → human review) · **HITL** approval before any external action (ticket/rollback suggestion) ·
deterministic fallback if an agent/LLM fails. Every prompt + evidence + response is **logged** for audit
(PIPEDA / Responsible AI, [02 §9](02-solution-architecture.md)).

## 7. Akka-readiness

Each agent (Supervisor, Data/SQL, Change, Contribution, Narrative, Runbook, ChatOps) is built as a **stateless
service over the lakehouse with a typed contract**, so it maps cleanly onto an Akka **durable agent / workflow**
later without redesign — see [05 §5](05-genai-and-akka.md) and
[ADR-0004](adr/adr-0004-akka-migration-strategy.md).
