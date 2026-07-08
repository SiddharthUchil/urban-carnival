<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# All the files attached have the context for "Anomaly Detection" use case for Manulife enterprise use case. The two website for which we need to give this solution are

Manulife Canada Group Benefits -
[https://www.manulife.com/ca/en/personal/group-plans/group-retirement/overview](https://www.manulife.com/ca/en/personal/group-plans/group-retirement/overview)

and

Manulife CoverMe -
[https://www.coverme.com/health-insurance?cid=CA-EN_ML_IS_PS_GOOGLEADS_BrandedRestructureManulife_COVERMEHD_BR_TEXT_-_-_B2C_AQCTR_23656086405_-\&utm_source=GOOGLEADS\&utm_medium=PS\&utm_campaign=23656086405_BrandedRestructureManulife\&utm_content=B2C_-\&utm_term=ML_IS_CA-EN_COVERMEHD_BR_TEXT_-_-_AQCTR\&MKT=-\&gclsrc=aw.ds\&gad_source=1\&gad_campaignid=23656086405\&gbraid=0AAAAADEaixHEJ5yjKmJUB-gGWB57WIQnG\&gclid=CjwKCAjw0o3SBhBVEiwAh28-jb_P0W2PVeJTfYgtogt7VuqsCOEEf_m6PVLHAb0KiPyBkCaKX07HKhoCKckQAvD_BwE\&province=ON\&agecode=0](https://www.coverme.com/health-insurance?cid=CA-EN_ML_IS_PS_GOOGLEADS_BrandedRestructureManulife_COVERMEHD_BR_TEXT_-_-_B2C_AQCTR_23656086405_-&utm_source=GOOGLEADS&utm_medium=PS&utm_campaign=23656086405_BrandedRestructureManulife&utm_content=B2C_-&utm_term=ML_IS_CA-EN_COVERMEHD_BR_TEXT_-_-_AQCTR&MKT=-&gclsrc=aw.ds&gad_source=1&gad_campaignid=23656086405&gbraid=0AAAAADEaixHEJ5yjKmJUB-gGWB57WIQnG&gclid=CjwKCAjw0o3SBhBVEiwAh28-jb_P0W2PVeJTfYgtogt7VuqsCOEEf_m6PVLHAb0KiPyBkCaKX07HKhoCKckQAvD_BwE&province=ON&agecode=0)

In the attched references we have the information  requried for Manulife CoverMe, so we can focus on solutioning for this site

More information about the objective of the project refer to the "README.md". Manulife CoverMe context can be found in "CoverMe_Adobe_Analytics_Data_Dictionary_Web_-_Data_Feed.xlsx.json". The other 2 files are supplmentary

The project deliverables are divided into 2 Phases

Phase 1: Anomaly Detection
Phase 2: Investigation and Insights
Identify potential causes (deployments, system upgrades, outages, business trends, etc.)
Provide recommendations and actionable insights.(MORE DETAILS IS GIVEN IN THE README.md)

### FOR TECH STACK

An enterprise leveraging an all-Azure and Databricks ecosystem can build a highly scalable, robust anomaly detection architecture. By combining Microsoft's cloud infrastructure with Databricks' unified data platform, you can cover everything from ingestion to real-time alerts

### CAN YOU DO DEEP RESEARCH ON HOW TO SOLUTION FOR THE ABOVE USE CASE. FOR THE 2 PHASES WE NEED AN ONLINE(REAL TIME) AND OFFLINE SOLUTION FOR BOTH PHASES.

### FOR REFERENCE I AM ADDING GITHUB REPO LINKS WHICH ARE SUPPLEMENTARY LINKS TO REFER ALONG WITH YOUR FINDINGS (FROM DEEP RESEARCH)

[https://github.com/yzhao062/pyod](https://github.com/yzhao062/pyod)

[https://github.com/unit8co/darts](https://github.com/unit8co/darts)

[https://github.com/yzhao062/anomaly-detection-resources](https://github.com/yzhao062/anomaly-detection-resources)

[https://github.com/business-science/ai-data-science-team](https://github.com/business-science/ai-data-science-team)

### ALSO HELP ME WITH LUCID DIAGRAMS SOLUTIONS FOR THE 2 PHASES (FOR BOTH ONLINE AND OFFLINE FOR EACH PHASES)

### MAKE SURE TO LEVERAGE GEN-AI ALSO INTO OUR SOLUTION(HOW WE CAN LEVERAGE GEN-AI)

another additional piece of information is the akka platform integration in later stages of development. Once the solution is finalized then the final migration would be to akka.

[https://www.manulife.com/ca/en/about-us/news/manulife-selects-akka-to-operationalize-agentic-ai](https://www.manulife.com/ca/en/about-us/news/manulife-selects-akka-to-operationalize-agentic-ai)

Here’s a structured architecture and design you can use as a starting point for solutioning, plus how to turn it into Lucid diagrams and where Gen‑AI and Akka fit in. For full narrative detail and citations, refer to the attached markdown report (GMAI-Pulse CoverMe Anomaly Architecture).[^1][^2][^3]

***

## Overall framing for CoverMe

For CoverMe, think of GMAI‑Pulse as a **two‑phase, two‑mode system** sitting on top of Adobe Analytics data feeds and other enterprise signals:

- Phase 1: Detection
    - Offline (batch / daily–hourly)
    - Online (near‑real‑time / streaming)
- Phase 2: Investigation \& Insights
    - Offline (heavier correlation \& explanation)
    - Online (lightweight, real‑time context and narratives)

Everything lives on **Azure + Databricks** for data and ML, with **Gen‑AI agents** providing explanations and recommendations, and a **future Akka runtime** hosting those agents in production.[^3][^4][^1]

***

## Data and feature layer for CoverMe (common to all phases)

### Sources

- Adobe Analytics data feeds for the CoverMe report suite:
    - `data_feed_columns` (raw feed columns, post_ versions)
    - `post_eVar` (Language, User Type, Visit Number, New/Repeat, etc., with “Anomaly Detection” notes)
    - `post_prop` (Product Category, Product ID, Sponsor/Distributor, Sub-Line of Business, Language, etc.)
    - `post_event_list` (Purchase, Product View, Cart events, Checkout, etc., with Status and “Anomaly Detection” notes)[^2]
- Optional enrichments:
    - Deployment / release logs (Azure DevOps/Jira)
    - CMS / content change logs
    - Campaign metadata (UTM tags from Adobe, ad platforms)
    - Tagging configuration / consent flags
    - Later: VOICE / call‑center signals, social listening, other sites per README vision[^1]


### Core KPIs \& anomaly surfaces

- Traffic: sessions, page views, unique visitors, time on site.
- Funnel: Product View → Quote Start → Quote Complete → Purchase per product / line of business / language.[^2][^1]
- Segment mix: device, geo, new vs repeat, channel, language.
- Data integrity: completeness of required eVars/props, tag status (enabled/disabled), feed freshness.[^5][^2]


### Azure + Databricks data layer

- ADLS Gen2:
    - Raw zone: direct dumps of Adobe feeds.
    - Curated zone: cleaned event/session/funnel tables.
- Databricks + Unity Catalog:
    - `coverme_raw_*` (raw feeds)
    - `coverme_events`, `coverme_sessions`, `coverme_funnel_kpi_ts`
    - `coverme_anomalies_batch`, `coverme_anomalies_rt`
    - `coverme_anomaly_insights`
- Data Quality Monitoring:
    - Freshness \& completeness anomaly detection on critical tables (to catch “pipeline anomalies”).[^6][^5]

Lucid diagram tip:
Create a **“Data \& Signals for CoverMe”** diagram with swimlanes:

- Left: Adobe Analytics feeds and other sources.
- Middle: ADLS Raw → ETL (DLT / notebooks) → Delta tables.
- Right: Downstream consumers (Phase 1 \& Phase 2 components).

***

## Phase 1 – Detection (Offline / Batch)

### Objectives

- Daily/hourly jobs to:
    - Build baselines with seasonality.
    - Detect anomalies in KPIs and segments.
    - Persist anomalies for investigation and trend analysis.[^4]


### Batch detection pipeline (Azure + Databricks)

1. **Ingest**
    - Azure Data Factory / Synapse copy Adobe feeds to ADLS Raw (SFTP / cloud export).[^4]
2. **Transform**
    - Databricks notebooks or Delta Live Tables in **Triggered mode**:
        - Parse and normalize post_eVar, post_prop, post_event_list.
        - Derive event and session tables (e.g., `coverme_events`).
        - Aggregate KPIs per day/hour, product, language, channel.[^2][^4]
3. **Model training (batch)**
    - On `coverme_funnel_kpi_ts` and segment aggregates:
        - **Time-series models (darts)**:
            - ARIMA/Prophet-like, RNN/TCN, etc.
            - Train per metric/segment and compute forecast + residuals.
        - **Tabular anomaly models (pyod)**:
            - Isolation Forest, COPOD, autoencoders on multi-feature vectors (product-day, campaign-day, etc.).
        - Use Databricks MLflow to track models and versions.
4. **Batch scoring**
    - Periodic jobs:
        - For each KPI series, flag anomalies where residual > threshold.
        - For tabular features, compute anomaly scores and classify top‑N anomalous rows.
        - Persist to `coverme_anomalies_batch` with:
            - Metric, segment, time, expected value, actual value, severity.[^7][^8]
5. **Data quality anomalies**
    - Enable Unity Catalog data-quality monitoring to detect:
        - Freshness anomalies (missing or delayed feeds).
        - Completeness anomalies (row counts, field null rates).
    - Store those in a separate “pipeline anomalies” table.[^6][^5]

### Batch detection – Lucid diagram structure

Title: **“Phase 1 – Offline Detection (CoverMe)”**

Columns:

- **Source layer**: Adobe feeds.
- **Ingestion layer**: ADF → ADLS Raw.
- **Curated layer**: Databricks ETL / DLT → `coverme_*` tables.
- **Modeling layer**: pyod + darts training and scoring on Databricks.
- **Output layer**: `coverme_anomalies_batch`, data‑quality anomalies.

Use icons for:

- Storage (ADLS), compute (Databricks), ML (model blocks), and anomalies DB.

***

## Phase 1 – Detection (Online / Near‑Real‑Time)

### Objectives

- Detect high‑impact anomalies in **near‑real‑time** (5–15 minutes).
- Support operational alerting for broken flows / tags / campaigns.[^4]


### Streaming detection pipeline

1. **Streaming ingestion**
    - Options:
        - Adobe real‑time APIs into **Azure Event Hubs / Kafka**.
        - Adobe feeds ingested via Autoloader + micro‑batch (continuous mode).[^4]
2. **Streaming ETL**
    - Databricks **Structured Streaming** or DLT in **Continuous mode**:
        - Read from Event Hubs/Kafka.
        - Normalize into events table `coverme_events_rt` (subset of fields needed for realtime KPIs).
3. **Realtime detection models**
    - Load pre‑trained models from batch phase:
        - Lightweight time-series predictors with rolling windows.
        - Threshold/z‑score logic over sliding windows when latency is critical.
    - Compute metrics per 5–15 minute window and flag anomalies:
        - `coverme_anomalies_rt` table with metric, window, severity.[^9][^4]
4. **Alerting**
    - Databricks SQL Alerts over anomaly tables:
        - Schedule frequent checks (e.g., every 5 minutes).
        - Conditions like “checkout events < expected – 3σ”, “pipeline freshness unhealthy”.[^5][^6]
    - Integrate alerts with:
        - Azure Logic Apps / Functions → email, SMS, Teams, Slack, PagerDuty.

### Online detection – Lucid diagram structure

Title: **“Phase 1 – Online Detection (CoverMe)”**

Swimlanes:

- Stream source: Adobe RT APIs / tag manager → Event Hubs.
- Databricks streaming: Autoloader / Structured Streaming → Delta.
- Anomaly scoring service: streaming job writing `coverme_anomalies_rt`.
- Alerting: Databricks SQL Alerts + Azure Logic Apps.

***

## Phase 2 – Investigation \& Insights (Offline / Batch)

### Objectives

- Explain anomalies, identify likely causes, and produce **insight narratives + recommendations**.[^10][^1]
- Build institutional memory: anomaly → cause → resolution.[^1]


### Batch investigation pipeline

1. **Inputs**
    - `coverme_anomalies_batch` (from Phase 1 offline).
    - Pipeline anomaly tables (freshness/completeness).
    - Reference tables:
        - Deployment logs (time, component, environment).
        - CMS/content changes.
        - Campaign launches / changes.
        - Tag configuration / consent versions.[^1]
2. **Correlation \& contribution analysis**
    - For each anomaly:
        - Time‑window join with deployments, CMS changes, campaigns.
        - Use feature attribution / contribution analysis ideas:
            - SHAP on tabular models.
            - Dimension scanning like Adobe’s contribution analysis to find segments that “explain” the anomaly (e.g., product = Travel, device = mobile, language = FR).[^10]
3. **Gen‑AI Insights Agent (batch)**
    - Pipeline:
        - Build structured JSON bundles: metrics, deltas vs expected, affected segments, candidate causes, business impact estimates.
        - Call an LLM (Azure OpenAI, etc.) with specialized prompts:
            - “Summarize what changed, where, when; hypothesize why; suggest who should act and what actions.”
        - Store generated narratives in `coverme_anomaly_insights` (Markdown / HTML) + metadata (owner function, severity).[^1]
    - Patterns to borrow:
        - From the **ai-data-science-team** repo: “AI analysts” that turn raw data into reports, documentation, and action plans.
4. **Consumption**
    - Dashboards:
        - Databricks SQL / Power BI showing anomalies and insights.
    - Routing:
        - Email digests by function (Demand Gen, SEO, MarTech).
        - Ticket creation (ServiceNow/Jira) with AI‑generated descriptions and suggested steps.

### Offline investigation – Lucid diagram structure

Title: **“Phase 2 – Offline Investigation \& Insights”**

Blocks:

- Anomalies input.
- Correlation engine (joins \& feature attribution).
- Gen‑AI Insights Agent:
    - Retrieval → LLM → narrative storage.
- Consumers:
    - Dashboards.
    - Ticketing / email digests.

***

## Phase 2 – Investigation \& Insights (Online / Near‑Real‑Time)

### Objectives

- Provide **contextual explanations and recommended actions** in minutes for severe real‑time anomalies.
- Enable ChatOps style interaction with anomalies.[^3][^6]


### Real-time investigation pipeline

1. **Inputs**
    - `coverme_anomalies_rt` from online detection.
    - Static reference Delta tables:
        - Deployments (recent).
        - Active campaigns.
        - Tag / config versions for CoverMe.[^5][^4]
2. **Fast correlation**
    - Structured streaming or fast queries:
        - Check for deployments within a recent timeframe around anomaly.
        - Identify which product/segment is most impacted (simple contribution analysis on streaming metrics).[^10]
3. **Gen‑AI alert narratives**
    - On severe anomalies:
        - Trigger an LLM service with a focused prompt:
            - “Explain this anomaly in one paragraph” + “list 3 likely causes given these deployment/campaign logs” + “suggest top 3 actions.”
        - Return narrative as part of alert payload (email, Teams message, Slack post).
4. **ChatOps / agent surface**
    - In Teams/Slack:
        - Bot that surfaces anomaly cards (metric, delta, segments, narrative).
        - Allow follow‑ups: “Show related anomalies”, “Compare to last week”, “What changed in campaigns?”
        - This bot is a natural candidate to run on **Akka** later as an agent orchestrating data queries and LLM calls.[^11][^3]

### Online investigation – Lucid diagram structure

Title: **“Phase 2 – Online Investigation \& Insights”**

Swimlanes:

- Real‑time anomaly input.
- Context enrichment (joins with deployments/campaigns).
- Gen‑AI narrative service.
- ChatOps channels (Teams/Slack bot, email alert templates).

***

## How to leverage Gen‑AI systematically

You can design explicit **Gen‑AI “roles”**:

1. **Anomaly Explainer Agent**
    - Input: anomaly row + KPI context.
    - Output: stakeholder‑specific summary (“for Demand Gen”, “for MarTech”, etc.).
2. **Root‑Cause Hypothesis Agent**
    - Input: anomalies + recent changes.
    - Output: ranked hypothesis list + confidence rationale.
3. **Runbook Advisor Agent**
    - Input: anomaly type + history of past incidents.
    - Output: recommended actions / playbook steps.
4. **Knowledge Base Agent**
    - Input: current anomaly.
    - Output: similar past anomalies, causes, resolutions using retrieval over historical anomaly corpus and docs.[^1]

All of these can initially be implemented as services called from Databricks / Azure Functions, then **re‑hosted on Akka** as durable agents that:

- Monitor anomaly tables and events.
- Invoke LLMs.
- Manage workflows, logging, and governance.[^11][^3]

***

## Akka migration path

To be Akka‑ready:

- Treat data/ML layer (Azure + Databricks) as **source of truth** and feature store.
- Expose anomaly tables and insights via APIs that Akka agents call.
- Model Gen‑AI components as independent agents in Akka:
    - Detection watchdog agent (subscribed to anomalies).
    - Insights generation agent.
    - Routing / workflow agent (integrates with Teams/Slack/ServiceNow).
- Use Akka’s governance features for:
    - Policy enforcement (Responsible AI, PII handling).
    - Audit logging of decisions and actions.[^3][^11]

***

## Detection algorithm recommendations (for piloting)

For CoverMe, a pragmatic starting set:

- **Offline**
    - Time-series: darts forecasting per KPI/segment, anomalies via residual thresholds.
    - Tabular: pyod Isolation Forest / COPOD on aggregated product‑day or campaign‑day features.
    - Data-quality: Databricks Unity anomaly detection on `system.data_quality_monitoring.table_results` for freshness/completeness.[^6][^5]
- **Online**
    - Rolling mean/std or EWMA‑based thresholds per KPI with dynamic bands.
    - Simple streaming residuals from a low‑complexity forecasting model (ARIMA / seasonal naive).
    - Severity model combining magnitude, duration, and business impact (e.g., expected revenue at risk).

***

## Governance and operating model

From the README and Akka announcement, build in:

- **Governance**
    - Mask/aggregate potentially sensitive Adobe fields; use post_ fields.
    - Unity Catalog access control over anomaly and insights tables.[^2][^5]
    - Responsible AI checks for Gen‑AI narratives (hallucination mitigation via grounding, RAG, and human review for high‑severity alerts).[^3]
- **Operating model**
    - Clear ownership:
        - Demand Gen: conversion anomalies.
        - SEO/GEO: traffic / ranking anomalies.
        - MarTech: tagging / data anomalies.[^1]
    - SLAs: detection latency, triage response times, resolution expectations.
    - Feedback loop into model tuning (precision/recall, MTTR improvements).[^1]

***

## Lucid diagrams: practical guidance

To implement in Lucidchart/Lucidscale:

1. **Create four core diagrams**:
    - Phase 1 Offline Detection.
    - Phase 1 Online Detection.
    - Phase 2 Offline Investigation \& Insights.
    - Phase 2 Online Investigation \& Insights.
2. **Use standard shapes**:
    - Cylinders for data stores (ADLS, Delta tables).
    - Rectangles for services (Data Factory, Databricks jobs, LLM services).
    - Lightning bolts for alerts.
    - Cloud icons for Azure, Databricks, Akka.
3. **Add swimlanes by concern**:
    - Data ingestion.
    - Modeling / detection.
    - Gen‑AI / insights.
    - Routing / stakeholders.
4. **Annotate each anomaly flow**:
    - Metric → detection → anomaly record → correlation → Gen‑AI narrative → stakeholder channel.

If you’d like, next step can be: I can help you draft concrete Lucid diagram text specs (node labels + connections) that you can paste directly into Lucid’s auto‑layout tools, and also sketch prompts/flows for the Gen‑AI agents that will eventually be hosted on Akka.
<span style="display:none">[^12][^13][^14][^15][^16][^17][^18][^19]</span>

<div align="center">⁂</div>

[^1]: README.md

[^2]: https://akka.io/blog/manulife-selects-akka-to-operationalize-agentic-ai

[^3]: https://www.prnewswire.com/apac/news-releases/manulife-selects-akka-to-operationalize-agentic-ai-within-its-enterprise-ai-platform-302707356.html

[^4]: https://www.databricks.com/blog/near-real-time-anomaly-detection-delta-live-tables-and-databricks-machine-learning

[^5]: CoverMe_Adobe_Analytics_Data_Dictionary_Web_-_Data_Feed.xlsx.json

[^6]: https://www.pedromonjo.com/2018/04/anomaly-detection-contribution-analysis.html

[^7]: https://www.bl.ink/blog/adobe-analytics-everything-you-need-to-know

[^8]: https://experienceleague.adobe.com/en/docs/analytics/analyze/analysis-workspace/anomaly-detection/anomaly-detection

[^9]: https://experienceleague.adobe.com/en/docs/analytics/analyze/legacy-report-builder/layout/anomaly-detection

[^10]: https://www.adexchanger.com/analytics/adobe-analytics-adds-real-time-visualization-and-anomaly-detection/

[^11]: https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-quality-monitoring/anomaly-detection/results

[^12]: https://github.com/devlace/azure-databricks-anomaly

[^13]: https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-quality-monitoring/anomaly-detection/alerts

[^14]: https://www.youtube.com/watch?v=-a-8W6GQZnU

[^15]: internal_excel_field_profile.json

[^16]: shareable_excel_field_analysis.md

[^17]: https://www.youtube.com/watch?v=krXyQCjXoeU

[^18]: https://www.cmswire.com/cms/customer-experience/adobe-analytics-adds-anomaly-detection-statistics-more-022783.php

[^19]: https://www.newswire.ca/news-releases/manulife-selects-akka-to-operationalize-agentic-ai-within-its-enterprise-ai-platform-804452481.html

