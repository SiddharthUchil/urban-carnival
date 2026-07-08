<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# The GMAI - Pulse project attached provides a solid framework for an enterprise anomaly detection pipeline, but requires specific refinements for the Manulife/Azure ecosystem to fully cover its scope. Key additions include implementing data-stitching for Adobe Analytics' login boundaries, defining data latency tiers for Azure ingestion, and strengthening governance with PII masking to comply with Canadian regulations.

To ensure comprehensive coverage, update the proposal to explicitly address identity resolution, data masking in Azure Synapse, and external environment signal integration. The architecture should be updated to include an identity and privacy masking layer within the ingestion phase.
Could you do a deep research and ensure comprehensive coverage

The proposal has been updated to include an explicit identity‑and‑privacy layer in the ingestion phase, Azure Synapse data masking and governance aligned to Canadian regulations, data latency tiers, and external environment signal integration, all tailored to the Manulife/Azure ecosystem. A Markdown architecture addendum has been produced that you can drop alongside the current README to give full, enterprise‑grade coverage of these topics.[^1][^2][^3]

## What changed in the proposal

The original GMAI – Pulse README defines signal sources, ingestion \& normalization, detection \& correlation, and an Insights Agent, but keeps governance, identity handling, and Azure implementation details deliberately open. The addendum narrows those gaps for Manulife by specifying identity resolution across Adobe login boundaries, data masking and pseudonymization in Synapse/Databricks, explicit latency tiers, and how external environment signals are brought into correlation.[^2][^1]

## Identity resolution \& Adobe login boundaries

Adobe’s identity stitching capabilities are used as the basis for resolving anonymous and authenticated sessions to a single person‑level identifier, by rekeying events to a common person ID across cookies, devices, and channels. The addendum recommends field‑based stitching where a login ID is captured in an eVar/prop at authentication, and a replay window is used so pre‑login events are stitched to the same person ID as post‑login events, with persistent IDs for wide coverage and person IDs for known users.[^4][^5][^6]

## Identity \& privacy masking layer in ingestion

Within the “Ingestion \& Normalization” stage, the architecture now explicitly inserts an **Identity \& Privacy Layer** between Connectors and the Schema/Metric Catalog. This layer ingests stitched person IDs from Adobe, performs pseudonymization/tokenization of identities, and applies Azure Synapse Dynamic Data Masking and column‑level security so PII (emails, login IDs, customer IDs) is masked for non‑privileged roles while remaining available to tightly controlled admin roles.[^7][^1][^2]

## Azure Synapse data masking and governance

Azure Synapse supports Dynamic Data Masking at schema, table, and column level, with built‑in masking functions for common PII types and the ability to define custom rules and role‑based visibility. The addendum prescribes defining roles such as `pulse_reader` and `pulse_admin`, applying column‑level masking to PII in Synapse views that feed Power BI, and combining this with column‑level security so broad reporting audiences only see non‑sensitive aggregates and masked fields.[^8][^2][^7]

## Canadian PII and compliance (PIPEDA)

Under PIPEDA, organizations must obtain consent, limit collection and use to identified purposes, safeguard personal information, and be transparent about cross‑border processing, including use of cloud services. The addendum frames GMAI – Pulse’s purposes explicitly as operational anomaly detection and customer‑experience monitoring, recommends pseudonymization for model inputs wherever possible, and calls for contractual and technical safeguards when data is processed in non‑Canadian Azure regions to ensure equivalent protection.[^9][^3]

## Data latency tiers for Azure ingestion

The base README already distinguishes batch feeds but not explicit freshness tiers. The addendum introduces three latency tiers—near‑real‑time (streaming for high‑impact operational anomalies), hourly/intraday (scheduled loads for KPI and traffic anomalies), and daily (end‑of‑day aggregates for executive narratives)—each with clear expectations for ingestion schedules and anomaly detection windows so stakeholders understand alert timeliness.[^1]

## External environment signal integration

Beyond the existing social listening and VOICE/contact‑center sources, the addendum adds “external environment signals” as a first‑class source set (e.g., macroeconomic indicators, regulatory or policy events) aligned on the same time axis as deployments, campaigns, and web analytics. These signals are treated as features in the correlation and root‑cause hinting layer to distinguish platform or tagging issues from market‑wide shifts that might otherwise look like anomalies.[^1]

## Updated ingestion‑phase architecture

Textually, the updated high‑level flow is: Signal Sources (including Adobe, MarTech, VOICE, social, SEO/GEO, external environment) → Connectors → **Identity \& Privacy Layer** (stitching ingestion, pseudonymization, Synapse masking policies) → Schema/Metric Catalog → Detection \& Correlation → Insights Agent → Consumer functions. This keeps the original conceptual architecture intact while making identity resolution, privacy controls, and Azure‑specific governance explicit and solutionable.[^1]

## How to use the addendum

You can treat the generated Markdown addendum as an ADR‑style companion to the README: link it from the “Open Decisions / Governance” section and use it as the basis for detailed Azure implementation and security reviews. From there, subsequent ADRs can refine choices such as Adobe stitching method, exact masking/tokens strategy, and latency SLAs, all while staying anchored to the Manulife/Azure and Canadian regulatory context outlined in the addendum.[^3][^4][^1]
<span style="display:none">[^10][^11][^12][^13][^14][^15][^16]</span>

<div align="center">⁂</div>

[^1]: README.md

[^2]: https://experienceleague.adobe.com/en/docs/analytics-platform/using/stitching/overview

[^3]: https://experienceleague.adobe.com/en/docs/analytics-platform/using/stitching/use-stitching-ui

[^4]: https://adswerve.com/blog/how-identity-stitching-can-enhance-adobe-cja

[^5]: https://www.youtube.com/watch?v=47JS0G9LalA

[^6]: https://www.linkedin.com/pulse/from-clicks-customers-power-identity-stitching-adobe-madala--kssmc

[^7]: https://www.sqlshack.com/dynamic-data-masking-in-azure-synapse-analytics/

[^8]: https://www.ifse.ca/understanding-pipeda-privacy-requirements/

[^9]: https://www.youtube.com/watch?v=P7qET3D58CM

[^10]: https://www.youtube.com/watch?v=CqfZ2P5bUQk

[^11]: https://resourcehub.bakermckenzie.com/en/resources/cloud-compliance-center/na/canada/topics/data-privacy-and-security

[^12]: https://experienceleague.adobe.com/en/docs/analytics/components/cda/field-based-stitching

[^13]: https://www.youtube.com/watch?v=VxujRzmaDaU

[^14]: https://www.priv.gc.ca/en/privacy-topics/privacy-laws-in-canada/the-personal-information-protection-and-electronic-documents-act-pipeda/pipeda_brief/

[^15]: https://experienceleague.adobe.com/en/docs/analytics-platform/using/stitching/faq

[^16]: https://learn.microsoft.com/en-gb/answers/questions/1684607/synapse))power-bi

