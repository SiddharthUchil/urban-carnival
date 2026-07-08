# **Enterprise Anomaly Detection Architecture for the Manulife/Azure Ecosystem: Ingestion-Phase Privacy, Identity Resolution, and Multi-Tier Latency Optimization**

The transition of enterprise digital operations from reactive troubleshooting to proactive, automated diagnostics requires a robust data engineering framework capable of processing high-velocity marketing, operational, and customer-experience signals.1 The GMAI \- Pulse initiative aims to address existing operational inefficiencies by continuously monitoring digital assets, identifying multi-dimensional anomalies, and correlating these events with concrete root causes, such as codebase deployments or marketing campaign launches.1  
Deploying this capabilities-driven framework within a highly regulated financial services context like Manulife requires refining the conceptual architecture to support deterministic customer identity stitching, multi-tier data ingestion latencies, and rigorous compliance with Canadian data privacy laws.2 This architectural blueprint provides a comprehensive guide to implementing an inline privacy-masking and identity-resolution layer within the Azure and Adobe Analytics ecosystems.

## **Architectural Framework and Business Justification**

An enterprise anomaly detection pipeline must process signals of varying urgency without incurring unnecessary execution costs.4 Rather than forcing all telemetry into a single high-cost streaming pathway, the ingestion architecture is categorized into three distinct latency tiers.5 This design aligns with the economic principle that real-time event streaming delivers high analytical utility but demands significantly greater infrastructure investments.7  
The analytical utility of data decays over time, but the operational cost of maintaining a sub-second response loop is substantial.4 For instance, a 4.7x economic value multiplier exists for real-time data streaming over 24-hour batch synchronization, which justifies premium services like Azure Event Hubs only for highly critical, time-sensitive signals.7 To evaluate this mathematically, let ![][image1] represent the analytical value of real-time streaming data, and let ![][image2] represent the analytical value of 24-hour batch replication:  
![][image3]  
This framing, derived from cloud-native integration and dynamic valuation modeling, allows organizations to ground premium streaming infrastructure investments in quantifiable ROI rather than qualitative business-case arguments.7

## **Ingestion-Phase Dynamic Privacy and Inline Masking Layer**

To ensure compliance with Canadian regulations while maintaining the utility of downstream datasets, the architecture incorporates a dedicated privacy masking layer directly into the ingestion phase.1 Implementing data masking before the data lands in persistent storage mitigates security risks and ensures that subsequent analytical processes operate on sanitized datasets.8

### **Ingestion-Phase Masking Strategies**

Depending on the processing engine and data volume, organizations can implement inline masking using two main patterns:

* **Azure Data Factory (ADF) Scripted Pipelines:** Because ADF does not support native dynamic data masking within a basic Copy Activity, masking is implemented within a Data Flow Activity.8 This process uses an dynamic script (such as Python or.NET) that references a centralized control table.8 The script reads the incoming source payload, identifies sensitive columns (e.g., Social Insurance Numbers, credit card details, or email addresses), and applies SHA-256 hashing or tokenization before writing the records to the ADLS Gen2 landing zone.3  
* **Databricks Auto Loader Stream Processing:** For high-volume streams, Databricks Auto Loader captures raw payloads and passes them to a Spark Structured Streaming notebook.10 The notebook applies catalog-driven security rules to selectively mask or encrypt specific columns on write.12 This approach is highly scalable and integrates with Unity Catalog to maintain a single source of truth for schema validation and access control.12

| Masking Approach | Processing Stage | Tooling | Governance Integration | Performance Impact |
| :---- | :---- | :---- | :---- | :---- |
| **ADF Dynamic Data Flow** 8 | Pre-storage (Landing Zone) 8 | ADF Integration Runtime, Python/.NET 8 | Centralized Control Table 8 | Moderate (proportional to mapping complexity) |
| **Databricks Auto Loader** 11 | Ingestion to Bronze Delta 11 | Spark Structured Streaming, Scala/Python 10 | Unity Catalog Rules & Lineage 12 | Low (optimized parallel execution) |

## **Multi-Tiered Data Ingestion and Latency Framework**

An enterprise-grade anomaly detection pipeline must process signals of varying urgency without incurring unnecessary execution costs.4 The ingestion layer is segregated into three functional tiers to align operational needs with infrastructure costs 5:

* **Tier 1: Real-Time Event-Driven (Sub-Second to Seconds):** Designed for telemetry that requires immediate intervention, such as site outages, severe API failures, or abrupt drop-offs in transaction volumes.4 Data flows continuously via event brokers into streaming engines.4  
* **Tier 2: Near-Real-Time (NRT) Micro-Batch (5 to 15 Minutes):** Geared toward core operational KPIs, user journey anomalies, and tagging misfires.6 This balance captures changes rapidly enough for active marketing campaigns while reducing the compute footprint.4  
* **Tier 3: Scheduled Batch (Hourly, Daily, or Weekly):** Reserved for low-velocity contextual metadata, SEO/GEO search rankings, social listening indexes, and customer-satisfaction surveys.1

This multi-tier approach aligns with Hierarchical Storage Management (HSM) principles, allowing organizations to map latency requirements to appropriate storage and compute resources.14 Data initially ingested into high-performance Tier 0 (NVMe/SSD) storage for real-time processing is migrated to Tier 1 (standard cloud object storage) for active use, and eventually transitioned to Tier 2/3 (cold/archival storage) for long-term retention and regulatory compliance.14

| Source System | Latency Tier | Ingestion Technology | Target ADLS Gen2 Path | Data Format | HSM Storage Tier |
| :---- | :---- | :---- | :---- | :---- | :---- |
| **Site Telemetry & Tagging Misfires** 1 | Tier 1: Real-Time | Azure Event Hubs / Databricks Structured Streaming 7 | /mnt/telemetry/raw/tier1\_event/ | JSON / Avro | Tier 0 (Hot SSD) 14 |
| **Adobe Clickstream (CoverMe / GWAM)** 1 | Tier 2: Micro-Batch | Databricks Auto Loader / Synapse Pipelines 11 | /mnt/clickstream/raw/tier2\_micro/ | Parquet / Delta 1 | Tier 1 (Warm Delta) 10 |
| **Deployment / Git Logs** 1 | Tier 2: Micro-Batch | Webhook to Azure Functions 16 | /mnt/deployments/raw/tier2\_micro/ | JSON | Tier 1 (Warm Delta) 10 |
| **SEO / GEO Rankings & Share of Voice** 1 | Tier 3: Batch | Azure Data Factory Copy Activity 10 | /mnt/seo\_geo/raw/tier3\_batch/ | CSV / JSON 5 | Tier 2 (Cold Object) 14 |
| **Social Listening & Voice of Customer** 1 | Tier 3: Batch | Orchestrated REST API Pull via ADF 5 | /mnt/feedback/raw/tier3\_batch/ | JSON | Tier 2 (Cold Object) 14 |

## **Customer Identity Resolution and Adobe Clickstream Data Stitching**

A primary challenge in web analytics is the inflation of unique visitor counts and the fragmentation of customer journeys due to cookie expiration and cross-device browsing.17 Implementing a robust identity resolution layer ensures that anonymous pre-login actions are accurately stitched to authenticated post-login profiles.17

### **First-Party Device Identifier Mechanics**

The Adobe Experience Platform Edge Network leverages the First-Party Device Identifier (FPID) to mitigate the effects of browser-imposed cookie restrictions.17 Generated by an application server and maintained in a secure first-party database, the FPID serves as a stable seed for generating the Experience Cloud ID (ECID).17  
When a user initiates an unauthenticated session, the application server retrieves or generates the FPID, setting it as a secure, HTTP-only cookie.17 Upon reaching the Edge Network, this FPID is converted into a stable ECID, ensuring that browser-level cookie clearings do not artificially inflate unique user counts.17  
Because the FPID is bound to a specific browser or device, it does not natively solve the cross-device identity problem.17 If a user transitions from a mobile device to a desktop browser, distinct FPIDs are generated, which results in fragmented event paths.17 True cross-device journey visibility requires a secondary deterministic mapping layer.17  
To avoid visit fragmentation, organizations must configure the backend to prevent mid-session FPID switching.17 If a user logs in mid-session, changing the FPID immediately can split a single continuous flow into two separate visits.17 Instead, the recommended pattern is to let the FPID stabilize the device-level identity while utilizing an authenticated identifier (e.g., Customer ID) as the primary identity namespace for cross-device stitching.17

### **Deterministic Field-Based Stitching and Replay Mechanics**

To establish a continuous customer profile, Customer Journey Analytics (CJA) and Adobe's Cross-Device Analytics (CDA) utilize deterministic, field-based stitching.19 This process relies on two distinct keys: a persistent identifier, typically the ECID, and a transient person identifier, such as a validated CRM or login ID captured when the user authenticates.17 Field-based stitching operates through a two-pass pipeline:

* **Live Stitching:** As events enter the ingestion pipeline, the system evaluates the payload in real time.18 If a device has previously authenticated and its ECID is already mapped to a known person identifier in the identity database, the incoming hit is immediately stamped with that person identifier.18 If the ECID is unrecognized or unauthenticated, the event remains unstitched in the live stream.18  
* **Replay Stitching:** At scheduled intervals, a replay engine reprocessing historical records is executed.18 The engine scans the defined lookback window to identify instances where an anonymous ECID subsequently authenticated.18 Once a match is found, all prior anonymous events associated with that ECID within the window are re-keyed to the newly established person identifier.18

The replay frequency and lookback window are mathematically constrained to balance processing overhead against reporting accuracy.18 Common configuration options are structured as follows:

| Replay Profile | Lookback Window | Execution Frequency | Primary Operational Trade-off |
| :---- | :---- | :---- | :---- |
| **Daily Replay** 21 | 24 Hours 21 | Daily (approx. 3 A.M. customer time) 21 | Low latency; requires authentication to occur within the same day.18 |
| **Weekly Replay** 21 | 7 Days 21 | Weekly (Saturday night execution) 21 | Balanced cost; provides a longer window for user authentication.18 |
| **Biweekly Replay** 18 | 14 Days 18 | Weekly execution 18 | Highly lenient authentication window; high processing cost.18 |
| **Monthly Replay** 18 | 30 Days 18 | Weekly execution 18 | Captures extended purchase cycles; highest storage and compute cost.18 |

This processing architecture is subject to strict constraints 19:

1. Field-based stitching is strictly case-sensitive and does not transform the identifier used.19  
2. The process is applied post VISTA and processing rules.19  
3. The engine does not support multiple identifier variables simultaneously (e.g., evaluating both login ID and email ID in parallel).19  
4. It does not concatenate fields.19  
5. In cases where multiple hits occur with identical timestamps but conflicting identifier values, the engine applies alphabetical sorting to resolve the conflict.19

### **Azure Spark Pipeline Replication**

For the Manulife ecosystem, relying solely on Adobe's internal stitching engines creates a downstream dependency, particularly since the raw data feeds for Canada Retirement (GWAM) and CoverMe are landing in Azure Data Lake Storage (ADLS) Gen2.1 To build an independent, audit-compliant identity stitching pipeline, a custom Spark-based replay engine must be deployed over the Delta Lake Bronze and Silver layers.1  
The custom Spark pipeline replicates the dual-pass logic by maintaining an append-only transaction ledger in the Bronze layer and executing an idempotent upsert (MERGE) operation into the Silver layer.10 The following PySpark implementation outlines the deterministic replay mechanism:

Python  
from pyspark.sql import SparkSession  
from pyspark.sql.functions import col, first, when  
from datetime import datetime, timedelta

spark \= SparkSession.builder \\  
   .appName("GMAI-Pulse-Identity-Stitching") \\  
   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\  
   .config("spark.sql.catalog.spark\_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \\  
   .getOrCreate()

\# Configuration parameters  
lookback\_days \= 7  
cutoff\_date \= (datetime.now() \- timedelta(days=lookback\_days)).strftime('%Y-%m-%d')

\# Read raw hit data from the Bronze partition  
bronze\_df \= spark.read.table("gwam\_prod\_catalog.inv\_typed\_common.adobe\_hit\_data\_raw") \\  
   .filter(col("ingest\_date") \>= cutoff\_date)

\# Extract identity mapping: find the most recent non-null customer\_id for each ECID  
identity\_map\_df \= bronze\_df \\  
   .filter(col("customer\_id").isNotNull() & (col("customer\_id")\!= "")) \\  
   .groupBy("ecid") \\  
   .agg(first("customer\_id").alias("resolved\_customer\_id"))

\# Read the existing Silver Delta table  
silver\_table\_name \= "gwam\_prod\_catalog.inv\_typed\_common.adobe\_hit\_data"

\# Join bronze hits with resolved identities and structure the update  
stitched\_updates\_df \= bronze\_df.alias("bronze") \\  
   .join(identity\_map\_df.alias("ids"), col("bronze.ecid") \== col("ids.ecid"), "left") \\  
   .select(  
        col("bronze.hit\_id"),  
        col("bronze.ecid"),  
        when(col("ids.resolved\_customer\_id").isNotNull(), col("ids.resolved\_customer\_id"))  
           .otherwise(col("bronze.customer\_id")).alias("customer\_id"),  
        col("bronze.timestamp"),  
        col("bronze.page\_name"),  
        col("bronze.ingest\_date")  
    )

\# Idempotent MERGE into Silver Delta Lake  
from delta.tables import DeltaTable  
silver\_delta \= DeltaTable.forName(spark, silver\_table\_name)

silver\_delta.alias("target") \\  
   .merge(  
        source \= stitched\_updates\_df.alias("source"),  
        condition \= "target.hit\_id \= source.hit\_id AND target.ingest\_date \= source.ingest\_date"  
    ) \\  
   .whenMatchedUpdate(set \= {  
        "customer\_id": col("source.customer\_id")  
    }) \\  
   .whenNotMatchedInsert(values \= {  
        "hit\_id": col("source.hit\_id"),  
        "ecid": col("source.ecid"),  
        "customer\_id": col("source.customer\_id"),  
        "timestamp": col("source.timestamp"),  
        "page\_name": col("source.page\_name"),  
        "ingest\_date": col("source.ingest\_date")  
    }) \\  
   .execute()

The following table evaluates the trade-offs of using this Azure-native Spark pipeline versus Adobe's native Cross-Device Analytics (CDA) engine:

| Evaluation Metric | Adobe Cross-Device Analytics (CDA) | Azure-Native Spark Stitching |
| :---- | :---- | :---- |
| **Data Residency** | Shared across Adobe Experience Cloud nodes.25 | Strictly resides within Manulife's ADLS Gen2 tenant.1 |
| **Latency** | 8 to 12 hours processing latency.20 | Configurable (e.g., hourly execution using Databricks).6 |
| **Lookback Windows** | Rigid (1, 7, 14, or 30 days).18 | Fully customizable via Spark query filters. |
| **API Compatibility** | Deprecates older API versions; does not support 1.4 API.20 | Open compatibility via Databricks SQL and Spark.12 |
| **Calculated Metrics** | Supports segmented metrics at report run-time.26 | Managed programmatically during the Silver-to-Gold transition.23 |

## **Analytical Security and Custom Masking in Azure Synapse**

Deploying an enterprise data pipeline in Canada requires strict compliance with federal and provincial privacy legislation.2 This architecture is designed to align with the Personal Information Protection and Electronic Documents Act (PIPEDA), Quebec's Law 25, and the pending statutory mandates of Bill C-27.2

### **Canadian Regulatory Mandates and Data Governance**

PIPEDA mandates that organizations remain fully accountable for personal information throughout its lifecycle, including during cross-border transfers.3 This requires that any outbound transfer of data to third-party processors maintains equivalent levels of protection.3 Furthermore, Quebec’s Law 25 and the proposed framework under Bill C-27 impose substantial financial penalties—up to C$25 million or 5% of gross global revenue—for data governance failures.3  
To meet these legal obligations, the ingestion and storage layers must enforce several key requirements:

* **Strict Consent and Purpose Limitation:** Data can only be collected and processed for explicitly disclosed purposes, requiring granular consent tracking.3  
* **De-identification and Minimal Retention:** Personally Identifiable Information (PII), such as Social Insurance Numbers (SINs), financial account details, email addresses, and home coordinates, must be masked, hashed, or tokenized immediately upon ingestion.3  
* **Right to Erasure (Unstitching Compliance):** Upon receiving a consumer deletion request, the system must erase the individual's profile.3 To comply with 2025/2026 privacy guidelines, the unstitching process must replace the resolved person IDs with the original persistent anonymous IDs (ECID) rather than re-assigning them to other identities, avoiding potential legal or compliance issues under Quebec's Law 25\.3

### **Infrastructure Security and Access Control**

To prevent unauthorized access to raw data, the underlying storage layer must be thoroughly secured 9:

* **Disabling Shared Keys:** Access via account-level Shared Keys and Shared Access Signatures (SAS) is disabled, requiring all authorization to use Microsoft Entra ID.9  
* **Network Isolation:** Public network access is blocked, and communication is routed exclusively through Private Endpoints within a secured Azure Virtual Network.9  
* **Role-Based Access Control (RBAC) and Access Control Lists (ACLs):** Coarse-grained access is controlled using Entra ID security groups assigned to roles like *Storage Blob Data Reader*.9 Fine-grained directory and file-level permissions are managed using POSIX-compliant ACLs.9  
* **Microsoft Entra Pass-Through:** For interactive data exploration, Databricks and Synapse access the data lake using the user's Entra ID identity.28 Automated pipelines run under a designated Azure Service Principal or Managed Identity.9

### **Security Engineering in Azure Synapse and Databricks**

Data governance is enforced using different mechanisms depending on the query engine used:

* **Databricks Unity Catalog:** Unity Catalog centralizes cataloging, line-of-business lineage tracking, and fine-grained access control.12 In Databricks, Column-Level Security (CLS) and dynamic data masking are implemented directly at the catalog metadata layer, allowing security administrators to define masking policies using standard SQL functions and apply them to specific database columns.12  
* **Azure Synapse Dedicated SQL Pools:** Synapse Dedicated SQL Pools natively support Dynamic Data Masking (DDM) and Row-Level Security (RLS).28 Masking is configured on specific table columns using standard functions, such as partial(), email(), or default(), which prevent exposure to non-privileged users without altering the underlying physical storage.29  
* **Azure Synapse Serverless SQL Pools:** A critical gap exists in Azure's native security model: **Dynamic Data Masking and Row-Level Security are not natively supported in Synapse Serverless SQL Pools** when querying external tables or Delta lakes directly.9

Because the Canada Retirement (GWAM) and CoverMe datasets are accessed via serverless pools 1, a custom workaround must be implemented.9 The recommended approach is to completely restrict direct access to the underlying external tables and instead expose data through **secure custom views in a dedicated database schema**.9 In Synapse Serverless databases, the dbo schema is strictly reserved for system-generated lake tables and cannot contain user-defined objects.9 Therefore, custom schemas must be created to house the views, which apply conditional masking logic using functions like IS\_MEMBER().9  
The following SQL script demonstrates how to implement this secure custom view workaround in a Serverless SQL Pool, alongside the native RLS configuration used in Dedicated SQL Pools:

SQL  
\-- \=========================================================================  
\-- WORKAROUND FOR SYNAPSE SERVERLESS SQL POOLS (Using Custom Schema & Views)  
\-- \=========================================================================

\-- Create a distinct custom schema (the 'dbo' schema is restricted)  
CREATE SCHEMA secure\_reports;  
GO

\-- Create a secure view that applies inline dynamic masking logic  
CREATE OR ALTER VIEW secure\_reports.v\_coverme\_hit\_data AS  
SELECT   
    hit\_id,  
    ecid,  
    \-- Restrict viewing of customer\_id to members of the privileged 'HR-Admins' group  
    CASE   
        WHEN IS\_MEMBER('Manulife-HR-Admins') \= 1 THEN customer\_id  
        ELSE CONCAT(LEFT(customer\_id, 2), '-XXXX-XXXX-', RIGHT(customer\_id, 4))  
    END AS customer\_id,  
    timestamp,  
    page\_name,  
    \-- Mask the IP address column for unprivileged users  
    CASE   
        WHEN IS\_MEMBER('Manulife-Data-Engineers') \= 1 THEN ip\_address  
        ELSE '0.0.0.0'  
    END AS ip\_address,  
    ingest\_date  
FROM dbo.coverme\_hit\_data\_external; \-- Direct external table over ADLS Gen2 Parquet  
GO

\-- Grant access to business analysts strictly on the secure schema  
GRANT SELECT ON SCHEMA::secure\_reports TO \[Manulife\-Analysts\-Group\];  
GO

\-- \=========================================================================  
\-- NATIVE ROW-LEVEL SECURITY CONFIGURATION (Dedicated SQL Pools Only)  
\-- \=========================================================================

\-- Establish the Security Predicate Schema  
CREATE SCHEMA security\_filters;  
GO

\-- Define the Inline Table-Valued Function for row extraction  
CREATE FUNCTION security\_filters.region\_access\_predicate(@Region AS NVARCHAR(50))  
    RETURNS TABLE  
    WITH SCHEMABINDING  
AS  
    RETURN SELECT 1 AS access\_result  
    WHERE   
        \-- Executive roles can access all records  
        IS\_MEMBER('Manulife-Executives') \= 1   
        \-- Regional analysts are restricted to their designated geographic segment  
        OR (IS\_MEMBER('Manulife-Central-Analysts') \= 1 AND @Region \= 'Central Canada')  
        OR (IS\_MEMBER('Manulife-East-Analysts') \= 1 AND @Region \= 'East Canada');  
GO

\-- Bind the filter predicate to the target table via a Security Policy  
CREATE SECURITY POLICY RegionalDataFilterPolicy  
    ADD FILTER PREDICATE security\_filters.region\_access\_predicate(Region)  
    ON Sales.Region  
    WITH (STATE \= ON);  
GO

## **External Environment Signal Integration and Multi-Signal Correlation**

To provide accurate root-cause analysis, the GMAI \- Pulse detection engine must look beyond web analytics and integrate contextual operational signals from external environments.1 This correlation relies on a multi-signal detection paradigm that joins digital customer touchpoints with physical system telemetry.16

### **External Contextual Signals**

The detection engine ingests several classes of external data to build a unified timeline of events 1:

* **Codebase Deployments and Configuration Changes:** Ingested via CI/CD webhooks from Azure DevOps or GitHub.10 These parameters include the repository name, commit hash, deployment status, and the developer's identity.1  
* **Marketing Campaigns and Ad-Spend Adjustments:** Captured via REST API endpoints to track campaigns, tracking-code persistent periods, and budget shifts.25  
* **Customer Feedback & System Incidents:** Streamed from CRM platforms (such as Salesforce), telephony systems, and ITSM service tools (like ServiceNow) to capture spike rates in support tickets or customer sentiment shifts.1

### **Multi-Signal Correlation and Root-Cause Detection**

The diagnostic core of GMAI \- Pulse combines these disparate telemetry sources using the Azure AI Anomaly Detector and multivariate machine learning models.16 While univariate analysis monitors isolated metrics for spikes or dips, multivariate models analyze correlated metrics simultaneously—such as evaluating page load latency, checkout conversion rates, and recent CDN deployment logs in parallel.16

  UNIVARIATE ANALYSIS                             MULTIVARIATE CORRELATION  
┌─────────────────────┐                            ┌────────────────────────┐  
│ Page Load Latency  │ ──┐                         │  Azure AI Anomaly      │  
└─────────────────────┘   │                        │  Detector Engine       │  
                          ├─► \[Anomaly Flagged\] ──►│                        ├─► Natural Language  
┌─────────────────────┐   │                        │  Evaluates:            │   Diagnostic Summary  
│ Checkout Drop-off   │ ──┘                        │  \- System Traces       │  
└─────────────────────┘                            │  \- CDN Deployments     │  
                                                   │  \- Campaign Launches   │  
                                                   └────────────────────────┘

The correlation workflow is structured as follows:

1. **Distributed Tracing and Telemetry Enrichment:** Every ingested log is parsed, normalized, and enriched with structured metadata, including service topologies, host variables, and execution context.16 Distributed tracing propagates unique trace and span identifiers across all boundary points.34  
2. **Context Table Join Queries:** When an anomaly is flagged, the pipeline queries both the Silver-layer analytics data and out-of-the-box Azure Active Directory or Threat Intelligence Context Tables to map the event to active user segments or geographic regions.9  
3. **Multivariate ML Execution:** The system runs multivariate models to determine whether the deviation correlates with an external event.16 For example, if web traffic drops significantly while a server deployment is active, the system calculates the probability that the deployment caused the drop based on historical patterns.16  
4. **Natural Language Generation:** The AI Insights Agent summarizes the incident in natural language, describing *what changed, when, where, and the likely root cause*, then routes the alert to the appropriate operational team.1

## **Architectural Conclusions and Implementation Roadmap**

Deploying the GMAI \- Pulse anomaly detection framework within the Manulife/Azure ecosystem requires a progressive, three-phase roadmap to transition operations from foundation to optimization:

┌───────────────────────────┐      ┌───────────────────────────┐      ┌───────────────────────────┐  
│     PHASE 1: FOUNDATION   │ ───► │     PHASE 2: EXPANSION    │ ───► │   PHASE 3: OPTIMIZATION   │  
│  (Ingestion & Security)   │      │ (Stitching & Correlation) │      │  (Automation & Tuning)    │  
└───────────────────────────┘      └───────────────────────────┘      └───────────────────────────┘

### **Phase 1: Foundation (Months 1–3)**

The initial phase focuses on establishing a secure, compliant data foundation 35:

* Deploy the inline dynamic privacy and masking layer using Azure Data Factory or Databricks Auto Loader to sanitize raw streams before they reach ADLS Gen2.8  
* Configure storage accounts with private endpoints and active Azure AD authorization, completely retiring shared keys.9  
* Establish the secure schema and custom views in Synapse Serverless SQL Pools to support compliant, masked query access for downstream analysts.9

### **Phase 2: Expansion (Months 4–6)**

The second phase introduces advanced identity resolution and initial correlation workflows:

* Implement the custom PySpark identity stitching script to run daily micro-batches on the Canada Retirement (GWAM) dataset, resolving anonymous customer profiles.1  
* Integrate the 2025/2026 privacy unstitching workflows to ensure compliant profile erasure in the identity database.3  
* Set up real-time telemetry pipelines to ingest external DevOps change records, git logs, and active campaign tracking codes.1

### **Phase 3: Optimization (Months 7+)**

The final phase focuses on operational automation, model tuning, and long-term scaling:

* Deploy multivariate machine learning models using Azure AI Anomaly Detector to correlate site anomalies with system deployments and campaign launches.16  
* Configure OpenTelemetry and Azure Application Insights to track semantic traces and agent execution logic, ensuring comprehensive visibility across the analytical pipeline.34  
* Implement Databricks Lakehouse monitoring and time-travel querying to validate data freshness, completeness, and lineage across the Medallion layers.4

#### **Works cited**

1. README.md  
2. Microsoft 365 Implementation Services in Canada \- Gestisoft, accessed July 4, 2026, [https://www.gestisoft.com/en/blog/microsoft-365-implementation-services](https://www.gestisoft.com/en/blog/microsoft-365-implementation-services)  
3. A 2026 Guide to Canadian Data Privacy Laws | Concentric AI, accessed July 4, 2026, [https://concentric.ai/canadian-data-privacy-laws/](https://concentric.ai/canadian-data-privacy-laws/)  
4. 10 Best Data Integration Platforms in 2026 \- Domo, accessed July 4, 2026, [https://www.domo.com/learn/article/data-integration-platforms](https://www.domo.com/learn/article/data-integration-platforms)  
5. Data Pipelines: Ingesting Structured and Unstructured Data for Agents in Manufacturing and Logistics \- Auxiliobits, accessed July 4, 2026, [https://www.auxiliobits.com/blog/data-pipelines-ingesting-structured-and-unstructured-data-for-agents-in-manufacturing-and-logistics/](https://www.auxiliobits.com/blog/data-pipelines-ingesting-structured-and-unstructured-data-for-agents-in-manufacturing-and-logistics/)  
6. Data Pipeline & AI Integration Services \- EdgeFirm, accessed July 4, 2026, [https://www.edgefirm.io/services/data-pipeline-engineering](https://www.edgefirm.io/services/data-pipeline-engineering)  
7. (PDF) Cloud-Native Integration Strategies for SAP S/4HANA and Azure Data Platforms: Architectural Considerations and Best Practices \- ResearchGate, accessed July 4, 2026, [https://www.researchgate.net/publication/407952794\_Cloud-Native\_Integration\_Strategies\_for\_SAP\_S4HANA\_and\_Azure\_Data\_Platforms\_Architectural\_Considerations\_and\_Best\_Practices](https://www.researchgate.net/publication/407952794_Cloud-Native_Integration_Strategies_for_SAP_S4HANA_and_Azure_Data_Platforms_Architectural_Considerations_and_Best_Practices)  
8. Dynamic Data Masking with Azure Data Factory \- Microsoft Q\&A, accessed July 4, 2026, [https://learn.microsoft.com/en-us/answers/questions/1634978/dynamic-data-masking-with-azure-data-factory](https://learn.microsoft.com/en-us/answers/questions/1634978/dynamic-data-masking-with-azure-data-factory)  
9. Securing dataverse data via synapse link \- data access and reporting, accessed July 4, 2026, [https://community.dynamics.com/forums/thread/details/?threadid=788d2459-2c90-f011-b4cc-0022482c11b3](https://community.dynamics.com/forums/thread/details/?threadid=788d2459-2c90-f011-b4cc-0022482c11b3)  
10. Azure Data Factory baseline architecture in an Azure landing zone \- Microsoft Learn, accessed July 4, 2026, [https://learn.microsoft.com/en-us/azure/architecture/databases/architecture/azure-data-factory-on-azure-landing-zones-baseline](https://learn.microsoft.com/en-us/azure/architecture/databases/architecture/azure-data-factory-on-azure-landing-zones-baseline)  
11. Trace Midstream Case Study: Cloud-Based SCADA & Data Modernization \- Tiger Analytics, accessed July 4, 2026, [https://www.tigeranalytics.com/perspectives/case-study/trace-midstream-data-modernization/](https://www.tigeranalytics.com/perspectives/case-study/trace-midstream-data-modernization/)  
12. Databricks Data Pipeline Best Practices: Building Robust Bronze ..., accessed July 4, 2026, [https://hexacorp.com/databricks-data-pipeline-best-practices/](https://hexacorp.com/databricks-data-pipeline-best-practices/)  
13. Databricks Lakehouse Architecture: What's Changed in 2026 \- Kanerika, accessed July 4, 2026, [https://kanerika.com/blogs/databricks-lakehouse-architecture/](https://kanerika.com/blogs/databricks-lakehouse-architecture/)  
14. Hierarchical storage management \- Grokipedia, accessed July 4, 2026, [https://grokipedia.com/page/Hierarchical\_storage\_management](https://grokipedia.com/page/Hierarchical_storage_management)  
15. Azure Synapse Analytics: Complete Data Warehousing Guide \- Smartbridge, accessed July 4, 2026, [https://smartbridge.com/azure-synapse-analytics-complete-data-warehousing-guide/](https://smartbridge.com/azure-synapse-analytics-complete-data-warehousing-guide/)  
16. AI for Log Anomaly Detection Why It Matters, How It Works, and What Modern Organizations Need to Know \- DEV Community, accessed July 4, 2026, [https://dev.to/alexendrascott01/ai-for-log-anomaly-detection-why-it-matters-how-it-works-and-what-modern-organizations-need-to-4e1n](https://dev.to/alexendrascott01/ai-for-log-anomaly-detection-why-it-matters-how-it-works-and-what-modern-organizations-need-to-4e1n)  
17. Missing One of My Community Post After Experience League Migration, accessed July 4, 2026, [https://experienceleaguecommunities.adobe.com/adobe-analytics-3/missing-one-of-my-community-post-after-experience-league-migration-248121](https://experienceleaguecommunities.adobe.com/adobe-analytics-3/missing-one-of-my-community-post-after-experience-league-migration-248121)  
18. Field-based Stitching | Adobe Customer Journey Analytics, accessed July 4, 2026, [https://experienceleague.adobe.com/en/docs/analytics-platform/using/stitching/fbs](https://experienceleague.adobe.com/en/docs/analytics-platform/using/stitching/fbs)  
19. Field-based stitching | Adobe Analytics \- Experience League, accessed July 4, 2026, [https://experienceleague.adobe.com/en/docs/analytics/components/cda/field-based-stitching](https://experienceleague.adobe.com/en/docs/analytics/components/cda/field-based-stitching)  
20. Cross-Device Analytics \- Experience League, accessed July 4, 2026, [https://experienceleague.adobe.com/en/docs/analytics/components/cda/overview](https://experienceleague.adobe.com/en/docs/analytics/components/cda/overview)  
21. Stitching FAQ | Adobe Customer Journey Analytics \- Experience League, accessed July 4, 2026, [https://experienceleague.adobe.com/en/docs/analytics-platform/using/stitching/faq](https://experienceleague.adobe.com/en/docs/analytics-platform/using/stitching/faq)  
22. How replays work | Adobe Analytics \- Experience League, accessed July 4, 2026, [https://experienceleague.adobe.com/en/docs/analytics/components/cda/replay](https://experienceleague.adobe.com/en/docs/analytics/components/cda/replay)  
23. Data Lake Architecture for Data Engineering Interviews \- DEV Community, accessed July 4, 2026, [https://dev.to/gowthampotureddi/data-lake-architecture-for-data-engineering-interviews-32e1](https://dev.to/gowthampotureddi/data-lake-architecture-for-data-engineering-interviews-32e1)  
24. Data Quality Management With Databricks, accessed July 4, 2026, [https://www.databricks.com/discover/pages/data-quality-management](https://www.databricks.com/discover/pages/data-quality-management)  
25. What Is Adobe Analytics? A Complete Guide to Features & Reporting \- Improvado, accessed July 4, 2026, [https://improvado.io/blog/adobe-analytics](https://improvado.io/blog/adobe-analytics)  
26. Calculated Metrics Overview | Adobe Analytics \- Experience League, accessed July 4, 2026, [https://experienceleague.adobe.com/en/docs/analytics/components/calculated-metrics/cm-overview](https://experienceleague.adobe.com/en/docs/analytics/components/calculated-metrics/cm-overview)  
27. Azure Synapse Analytics Breaks Down Data Silos \- Alphavima Technologies, accessed July 4, 2026, [https://alphavima.com/blog/azure-synapse-analytics-data-platform/](https://alphavima.com/blog/azure-synapse-analytics-data-platform/)  
28. Dynamic Data Masking On Synapse Serverless SQL Database ..., accessed July 4, 2026, [https://learn.microsoft.com/en-us/answers/questions/2120635/dynamic-data-masking-on-synapse-serverless-sql-dat](https://learn.microsoft.com/en-us/answers/questions/2120635/dynamic-data-masking-on-synapse-serverless-sql-dat)  
29. Dynamic Data Masking \- Azure SQL Database & Azure SQL Managed Instance & Azure Synapse Analytics | Microsoft Learn, accessed July 4, 2026, [https://learn.microsoft.com/en-us/azure/azure-sql/database/dynamic-data-masking-overview?view=azuresql](https://learn.microsoft.com/en-us/azure/azure-sql/database/dynamic-data-masking-overview?view=azuresql)  
30. Azure Synapse Analytics security white paper: Access control, accessed July 4, 2026, [https://docs.azure.cn/en-us/synapse-analytics/guidance/security-white-paper-access-control](https://docs.azure.cn/en-us/synapse-analytics/guidance/security-white-paper-access-control)  
31. How Advanced Analytics Works | Exabeam Documentation Portal, accessed July 4, 2026, [https://docs.exabeam.com/en/cloud-delivered-advanced-analytics/all/administration-guide/understand-the-basics-of-advanced-analytics/how-advanced-analytics-works.html](https://docs.exabeam.com/en/cloud-delivered-advanced-analytics/all/administration-guide/understand-the-basics-of-advanced-analytics/how-advanced-analytics-works.html)  
32. Enterprise Channel Attribution Software: Complete Guide \- Cometly, accessed July 4, 2026, [https://www.cometly.com/post/enterprise-channel-attribution-software](https://www.cometly.com/post/enterprise-channel-attribution-software)  
33. Data export use cases | Adobe Customer Journey Analytics \- Experience League, accessed July 4, 2026, [https://experienceleague.adobe.com/en/docs/analytics-platform/using/cja-usecases/data-export/overview](https://experienceleague.adobe.com/en/docs/analytics-platform/using/cja-usecases/data-export/overview)  
34. Dynamic AI Agents at Scale Pattern \- Azure Architecture Center | Microsoft Learn, accessed July 4, 2026, [https://learn.microsoft.com/en-us/azure/architecture/solution-ideas/articles/ai-agents-at-scale](https://learn.microsoft.com/en-us/azure/architecture/solution-ideas/articles/ai-agents-at-scale)  
35. A Comprehensive Cloud Data Lakehouse Adoption Strategy for Scalable Enterprise Analytics, accessed July 4, 2026, [https://ijeret.org/index.php/ijeret/article/download/383/364](https://ijeret.org/index.php/ijeret/article/download/383/364)  
36. AI Services Canada \[2026\] \- Fusion Computing, accessed July 4, 2026, [https://fusioncomputing.ca/ai-services/](https://fusioncomputing.ca/ai-services/)

[image1]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABkAAAAaCAYAAABCfffNAAABLUlEQVR4Xu2UvUoDURCFT4gBQx5AJIoWgoVYBRFS2QiWIhYWPoC1Fj5BejvBzjJgZy/BZ9BSUBAsrK3EnzPOTeI92WXjXQvBfHAY9szAmb17E2DCX+WWOvumc2oumgDuqC6GMwdxe3w+4IF5PFLXav4UC3lRM2Bv9qZmChZiysLCl9RMIS9kkVpXM5W8kF85pj7vGA15plbEK8UDRkNK3yalBw9phOcTamrQLWaG2lNTOYSHtKgj6jJuF9KjptVU1uAhto3VStz+YptapqrwX75Vm9uFf1OrC4PpDGYxvGGb0jPmqS14/4q6gW9v1IM/Fja4r2agHepr5Dod6l7NPIq2sTO/UJM8UcdqpmIbN9WEL2cL7MBvWSnsnziL/gmcRm4iq2oEatSGmhP+MZ847ToKC4c6QAAAAABJRU5ErkJggg==>

[image2]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAC8AAAAaCAYAAAAnkAWyAAAB+klEQVR4Xu2WTygFURTGj1CKUlISCwklykIo2fgbCxaiJHsWViRbGwtWVhbKwtJaysLiZSdrJaWeUhbWysaf871zrzlznsl475WR+dXXu+87M/eee++5M0OUkvI/uWYdKB2xmkNXEN2xjim4ZiUc/n3eSSYSxQPrwppJAck/W9OBnXi1ZpJA8tBXYFJt1kwSUcm3sAasmTSikk90uXjeKD/5J1aX8RLJPeUnb58ufneqjV8Kzkn6HrOBOGQonNgeq+IzGmAnGIdLa0RwSgUuzBpJYr2sddZJOJyjibVtzW+YofirWcjC5OgjuXnB/ZaFwzk2We2sQ9aiic2z9o03x7oi6XPUxNDHLgXj4F2CcadZZ8qPRSMFNT1uYp4sa8u1V1mVro176lR7yrUBHgQanyTYIfn0AFgY+OXuf9b9xgY3L1nTgfrX27pMsko4G0PK19e0mv9A72qN8jFJlBhAHDv2I+xAGiSo4+i8x3l6ix9VG+cjq/7XUzjuqSLpx08GY00G4eLBKmdcu5aCiegJDbNmSQZGoojhsCIplBk8fLVqNkhKRk/K93mjvKLwZYOaxFu30/koG9T+CMl7AQf/1sVwHUoHX6QeeOijgfVC8mhE3eunGMYZZPUrryR0W4NkEH94MahGH17PBEmpeDooXHqYnI6npKT8ZT4AdYNo/dNVcuQAAAAASUVORK5CYII=>

[image3]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAmwAAAA9CAYAAAAQ2DVeAAADrklEQVR4Xu3cTchmYxgH8FtYyNeClEaRhZKFRIqyMWqIJBTFzoIFNopSFpKspjQNGtlIytfORpKNpYWNFamhZKkUhXzc15xzj/tcc97nOeM99da8v19dPef+eM45z7P6d5+PUgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADgTnVvr4Vr/1Pq+1n3T4XJkHHsw9e+FOI9NYnyu1hb/Rez3j3G79/Q49l7qBwDYtQgZb+fO6pla5+XOPfBB2R6+clCL+nsyYz2x7wi42Q21rsudAABr2CmAfJ079kCsAj5Utge2x1P759Re059l/nz+yh0AAGuZCyC7DTw3lWGfP4yfz06HTziaO2a0VbJ8fpvcVuvK3JnM/b4Ih5/lzhlfllPP59PUBgBY1RdlGkCuKEPo2Y2PUzv2Gce4ZGzfXraHnDe77RyQNvk9d+zg1257aVgLz5fp+ZxT66WuDQCwuufKNICsce9XC2ZZ3BcXx7o1DyRn1Xqiay8NbJ/UujF3bhChLcLa53lgg9h/fz799l2p/X+9VdbZDwBwhugDSNzgH2FpmyVh4v0yzDucB0b3545O3n9u72TpvObiMnxnyW9uLijDd2Jl7dEy7KO39By2zds2DgDsIxFWIhzcU+uq6VC5ptbZtV6odVGtS8vwKpCvah3s5mX5HrF4CjVehdHEMb/t2tssCS/XlmXzmg/Lf2Hr5loPdGPbxHHuLMMDEdlPZQirvdfL8D+G88vwSpAfa919ckYpr9W6Y9y+sNZHZXi1Sj8HANjHIoDkkBVuKcPY9WUIIuHlWgdOzph3ee4YxZOUsb+5p1I3yUEs2rkvX9rd5J1y6srY6YS2ueOHCFzt3rt2j1z7X9+tdfW4/UiZXrpt+2qfEdReGbf7e+0AgH1sLnw096b2prlri2AXT5q2au+Li5WtCEC99hLgJWKVa06sKC4Rx5m7jPpdtx1zjpdhVTLE07jNL932k2UIm73+dyz9TQDAPtXCRq8FiGOTXkL7byKExf1tOXhd1m2HuOwZIS/ui+u18XhxcTxI8VQ3BgAwkVexQlwafSx3csIbZVh5ayttr5bhKdTjtX6r9c3YH6tth8btuDT74vgZrxaJ1b8IciEuscbl5aWXagEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAOB3/Ah8AqOMY9HZEAAAAAElFTkSuQmCC>