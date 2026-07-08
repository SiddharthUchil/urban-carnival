# 09 — References & Source Notes

> Sources for the targeted research that grounds this package. Confidence flags mark how well each
> **load-bearing** claim was verified. URLs were live as of the June 2026 research pass; product names/prices
> change — re-verify at build time.

## Load-bearing claims & confidence

| Claim | Confidence | Basis |
|---|---|---|
| **GWAM lands as** `gwam_prod_catalog.inv_typed_common.adobe_hit_data` (UC/Delta, Databricks SQL); **CoverMe as** `martech.adobe_coverme.hit_data` (ADLS external table, **Synapse serverless SQL**) | **HIGH** | `new_data/README.md` + data inventory (2026-07-02) — provisional pending data-platform-owner confirmation |
| Both feeds share the **canonical ~1,198-column Adobe hit schema**; dictionary decodes 95.98% cols / 100% eVars / 100% props; **29 rows tagged "Anomaly Detection"** (9+8+12); 24 sensitive columns; 11 correlation keys missing | **HIGH** | `new_data/` profiling artifacts (machine-generated, 2026-07-02) |
| **Lakehouse Federation supports Azure Synapse** via connection `TYPE sqldw` (foreign catalog in UC) | **HIGH** | https://learn.microsoft.com/en-us/azure/databricks/query-federation/sqldw |
| **Multiple engines can read one ADLS external location**; UC external tables over the same files are supported | **HIGH** | MS Learn external-locations-adls + tables/external |
| Adobe **Data Feed is batch** (hours, up to 12h+, no SLA) | **HIGH** | Two official Experience League pages + FAQ, verbatim |
| **Edge Network** is the only sub-10-min Adobe path; AEP streaming <15 min, CJA real-time <7–17 min | **HIGH** | Experience League (verbatim tables) — *relevant only to the deferred Phase-3+ hot lane ([ADR-0001 v2](adr/adr-0001-near-real-time-microbatch.md))* |
| Adobe Real-Time Reporting API ~30 s; BDIA 20–50 min | **MEDIUM** | Dev FAQ 404'd; search summary + one third-party guide |
| DLT renamed **Lakeflow (Spark) Declarative Pipelines** | **HIGH** | Databricks docs (updated 2026) |
| `trigger(continuous)` **unsupported on Databricks**; micro-batch floor ≈ 3–5 s; real-time mode preview <1 s tail | **HIGH** | Databricks Structured Streaming triggers docs |
| Lakehouse Monitoring has **built-in Anomaly Detection** (freshness/completeness, Preview) | **MEDIUM-HIGH** | Azure Databricks data-quality-monitoring docs |
| Akka is **not literally exactly-once**; steps at-least-once + developer idempotency/saga | **HIGH** | Akka workflow docs (own warning) |
| Akka is **JVM-only**; Python ML via HTTP/gRPC from Workflows/Consumers | **HIGH** | Akka inter-agent-comms docs |
| **Point-adjusted F1 is statistically broken** | **HIGH** | Kim et al. 2021 (peer-reviewed critique) |
| **Manulife selected Adaptive ML** (Dec 22 2025) for RL fine-tuning/serving of open SLMs | **HIGH** | PR Newswire release + multiple outlets (Globe & Mail, Morningstar, Investing.com) |
| Adaptive ML is **independent** (not acquired); $20M seed incl. **Databricks Ventures** | **MEDIUM-HIGH** | Sifted, Wikipedia, startup profiles — cross-checked |
| **Synapse serverless SQL does not support DDM/RLS on external tables**; secure views in a custom schema + `IS_MEMBER()` are the standard workaround | **HIGH** | MS Learn Q&A (serverless DDM) + DDM overview (dedicated/SQL DB scope); basis for [ADR-0007](adr/adr-0007-identity-privacy-layer.md) / [11 §4](11-privacy-identity-governance.md) |
| **No person-level identifier in the CoverMe feed**: `cust_visid`/`post_cust_visid` all-NULL; `userid` a single constant on every row | **HIGH** | Full-database confirmation by the data owner (2026-07-04) — supersedes the 10-row sample's fill rates |

## Adobe Analytics (ingestion & latency)
- Analytics latency technote — https://experienceleague.adobe.com/en/docs/analytics/technotes/latency
- Data Feed best practices — https://experienceleague.adobe.com/en/docs/analytics/export/analytics-data-feed/data-feeds-best-practices
- Data Feed FAQ — https://experienceleague.adobe.com/en/docs/analytics/export/analytics-data-feed/df-faq
- CJA real-time reporting — https://experienceleague.adobe.com/en/docs/analytics-platform/using/cja-components/real-time-reporting/real-time
- AEP streaming ingestion — https://experienceleague.adobe.com/en/docs/experience-platform/ingestion/streaming/overview
- Event Forwarding — https://experienceleague.adobe.com/en/docs/experience-platform/tags/event-forwarding/overview

## Databricks / Azure platform
- Lakehouse Federation — Azure Synapse (`sqldw`) — https://learn.microsoft.com/en-us/azure/databricks/query-federation/sqldw
- UC external locations on ADLS Gen2 — https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/external-locations-adls
- UC external tables — https://learn.microsoft.com/en-us/azure/databricks/tables/external
- Structured Streaming triggers — https://docs.databricks.com/aws/en/structured-streaming/triggers
- Watermarking — https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/watermarks
- Lakeflow Declarative Pipelines (overview + expectations) — https://learn.microsoft.com/en-us/azure/databricks/ldp/ · …/dlt/expectations
- Lakehouse Monitoring — https://learn.microsoft.com/en-us/azure/databricks/lakehouse-monitoring
- UC data-quality Anomaly Detection — https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-quality-monitoring/anomaly-detection
- Model Serving — https://learn.microsoft.com/en-us/azure/databricks/machine-learning/model-serving
- `mlflow.pyfunc.spark_udf` — https://mlflow.org (API docs)
- SQL Alerts — https://learn.microsoft.com/en-us/azure/databricks/sql/user/alerts
- Notification destinations (Teams/PagerDuty/Webhook) — https://learn.microsoft.com/en-us/azure/databricks/admin/workspace-settings/notification-destinations

## Privacy & identity (Canada / Synapse / Adobe) — see [11-privacy-identity-governance.md](11-privacy-identity-governance.md)
- DDM on Synapse serverless (not supported; view workaround) — https://learn.microsoft.com/en-us/answers/questions/2120635/dynamic-data-masking-on-synapse-serverless-sql-dat
- Dynamic Data Masking overview (Azure SQL / Synapse dedicated) — https://learn.microsoft.com/en-us/azure/azure-sql/database/dynamic-data-masking-overview?view=azuresql
- PIPEDA in brief — OPC — https://www.priv.gc.ca/en/privacy-topics/privacy-laws-in-canada/the-personal-information-protection-and-electronic-documents-act-pipeda/pipeda_brief/
- Quebec Law 25 (Act 25) — CAI guidance — https://www.cai.gouv.qc.ca/ *(penalty figures: confirm with legal — secondary sources conflict)*
- Bill C-27 (CPPA/AIDA) status — **MEDIUM** confidence, legislative flux — track via https://www.parl.ca/legisinfo/
- Adobe CJA stitching overview — https://experienceleague.adobe.com/en/docs/analytics-platform/using/stitching/overview · field-based stitching — …/stitching/fbs *(relevant only to the deferred stitching option, [ADR-0007](adr/adr-0007-identity-privacy-layer.md))*
- Adobe FPID / ECID (first-party device IDs) — https://experienceleague.adobe.com/en/docs/experience-platform/edge/identity/first-party-device-ids

## Anomaly-detection libraries
- pyod — https://github.com/yzhao062/pyod
- darts — https://github.com/unit8co/darts (`darts.ad` API: generated_api/darts.ad.html)
- anomaly-detection-resources — https://github.com/yzhao062/anomaly-detection-resources
- ADBench — Han et al., NeurIPS 2022 · TSB-AD — NeurIPS 2024 · MetaOD — Zhao et al., NeurIPS 2021

## Gen-AI: frameworks, RAG, guardrails
- Mosaic AI Agent Framework + Agent Evaluation — https://www.databricks.com/blog/announcing-mosaic-ai-agent-framework-and-agent-evaluation
- Agent Framework tutorial — https://learn.microsoft.com/en-us/azure/databricks/generative-ai/tutorials/agent-framework-notebook
- Azure AI Foundry Agent Service — https://learn.microsoft.com/en-us/azure/foundry/agents/overview · function calling …/how-to/tools/function-calling
- Responses API — https://azure.microsoft.com/en-us/blog/announcing-the-responses-api-and-computer-using-agent-in-azure-ai-foundry/
- Mosaic AI Vector Search GA — https://www.databricks.com/blog/announcing-mosaic-ai-vector-search-general-availability-databricks · reranking …/reranking-mosaic-ai-vector-search-faster-smarter-retrieval-rag-agents
- Vector Search retrieval quality — https://learn.microsoft.com/en-us/azure/databricks/generative-ai/vector-search-retrieval-quality
- Claude in Microsoft Foundry — https://azure.microsoft.com/en-us/blog/introducing-anthropics-claude-models-in-microsoft-foundry-bringing-frontier-intelligence-to-azure/
- Databricks Foundation Model APIs (supported models, incl. Claude) — https://learn.microsoft.com/en-us/azure/databricks/machine-learning/foundation-model-apis/supported-models
- ai-data-science-team — https://github.com/business-science/ai-data-science-team
- RAG grounding / hallucination mitigation surveys (illustrative) — arXiv:2510.24476, arXiv:2603.17872

## Adaptive ML / model tuning (RLOps) — see [07-adaptive-ml-integration.md](07-adaptive-ml-integration.md)
- Manulife selects Adaptive ML (announcement) — https://www.prnewswire.com/news-releases/manulife-selects-adaptive-ml-as-reinforcement-learning-engine-to-scale-enterprise-ai-302647271.html *(also Newswire.ca, Globe & Mail, Morningstar, Investing.com; manulife.com mirror may 403)*
- Adaptive Engine (product) — https://www.adaptive-ml.com/engine · company — https://www.adaptive-ml.com/
- AT&T selects Adaptive Engine (text-to-SQL "AskData" precedent) — https://www.adaptive-ml.com/post/att-selects-adaptive-engine
- Adaptive ML profile / funding ($20M seed; Index Ventures, ICONIQ, **Databricks Ventures**) — https://en.wikipedia.org/wiki/Adaptive_ML · https://sifted.eu/articles/gen-ai-startup-adaptive-20m-seed
- RFT best practice (DPO/ORPO/RLVR; online iterative RLHF / RLTHF) — Raschka, "State of LLMs 2025" https://magazine.sebastianraschka.com/p/state-of-llms-2025 ; reward-model survey arXiv:2505.02686
- Tuned open model vs frontier (cost/latency/data-residency) — https://www.truefoundry.com/blog/llm-deployment-in-regulated-industries-hipaa-soc2-and-gdpr-playbook-for-2026

## Akka
- Manulife selects Akka (announcement) — https://akka.io/blog/manulife-selects-akka-to-operationalize-agentic-ai *(manulife.com original returned HTTP 403)*
- Akka SDK — Agents (sdk/agents.html), Autonomous Agents (sdk/autonomous-agents.html), Workflows (sdk/workflows.html)
- Akka concepts — architecture-model.html, state-model.html, inter-agent-comms.html, ai-agents.html (all under https://doc.akka.io)

## Evaluation & cost
- scikit-learn model evaluation — https://scikit-learn.org/stable/modules/model_evaluation.html
- PR vs ROC under imbalance — Saito & Rehmsmeier, PLoS ONE 2015, https://doi.org/10.1371/journal.pone.0118432
- Point-adjustment critique — Kim et al., 2021, https://arxiv.org/abs/2109.05257
- Range-based precision/recall — Tatbul et al., NeurIPS 2018, https://arxiv.org/abs/1803.03639
- Google SRE — monitoring & alerting — https://sre.google/sre-book/monitoring-distributed-systems/
- Event Hubs scalability / pricing — https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-scalability · https://azure.microsoft.com/en-us/pricing/details/event-hubs/
- Databricks pricing (DBU) — https://azure.microsoft.com/en-us/pricing/details/databricks/
- Azure OpenAI Batch API — https://learn.microsoft.com/en-us/azure/ai-foundry/openai/how-to/batch · prompt caching …/how-to/prompt-caching

## Data profiling — primary evidence (2026-07-02; see [10-data-profile-alignment.md](10-data-profile-alignment.md))
- `new_data/README.md` — GMAI-Pulse project README with both domains' Azure landing points
- `new_data/data_inventory.md` — narrative inventory of the profiled corpus
- `new_data/data_profiling_report.md` — deep column-level profile (human-readable)
- `new_data/data_profile_summary.json` — machine-readable summary: per-file readiness, **joinability graph**, grain hints, metric/dimension shortlists (primary input for `metric-registry.yaml`)
- `new_data/generated_data_profile.json` (+ `.md`) — full aggregate raw profile (every column, every stat)

## Prior internal research (synthesized & critiqued)
- `research/gemini/intial_gemini_research.md`
- `research/perplexity/intial_perplexity_research.md`, `perplexity_lucid_diagram.md`
- `forresearchpurposes/README.md` (GMAI–Pulse), data dictionary + field-profile JSON/MD — *superseded by `new_data/` above where they conflict*
- `research/perplexity/perplexity_extending_GMAI.md`, `research/gemini/gemini_extending_GMAI.md` —
  privacy/identity extension research; verdicts in [11 §1](11-privacy-identity-governance.md).
  **Caveats:** Gemini's "4.7× real-time value multiplier" is weakly sourced (ResearchGate/blog chain);
  its PySpark stitching sample references columns (`customer_id`) absent from the profiled schema —
  treat both docs as direction, not evidence.
