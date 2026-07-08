# **Enterprise Anomaly Detection and Diagnostic Intelligence Blueprint: Manulife CoverMe**

## **Architectural Ingestion and Adobe Analytics Data Modeling**

To establish a resilient anomaly detection platform for the Manulife CoverMe digital portfolio, the ingestion framework must process, clean, and structure raw clickstream telemetry from Adobe Analytics.1 This clickstream data, delivered in daily or hourly batches, represents a sequence of raw digital actions that must be parsed to reconstruct historical patterns and live user trends.1 The raw data files are structured as tab-separated values (TSV) rather than standard comma-separated values (CSV) because escaped delimiters within search queries and URL parameters disrupt standard parsing engines.4 Consequently, an Apache Spark data source leveraging a high-performance TSV parser is utilized to prevent structural data corruption.4  
Reconstructing standard web analytics metrics from individual row-level hits is a prerequisite for downstream modeling.1 In Adobe Analytics, every server call corresponds to a single row.3 To compute unique visitors, visits (sessions), and occurrences, the system applies deterministic combinations of low-level data fields to handle scale and avoid mathematical collisions in high-volume environments.1 The mathematical formulations for these core session reconstruction fields are defined as follows:  
![][image1]  
![][image2]  
![][image3]  
The concatenation of the high and low 64-bit visitor identifiers yields a unique visitor token, representing the device-level footprint.3 To reconstruct a single Visit (session), the system concatenates the visitor identifiers with the session sequence counter (visit\_num).3 Incorporating the Greenwich Mean Time (GMT) visit start timestamp (visit\_start\_time\_gmt) is critical to prevent hash collisions arising from system resets or custom visitor ID overrides.1 An occurrence represents every recorded action or server call.3 It is modeled by appending the hit sequence depth counter (visit\_page\_num), which increments on every transactional event during that session.3  
To prepare the CoverMe metadata schemas for advanced machine learning, the data platform must ingest and align several critical metadata sheets from the CoverMe Adobe Analytics Data Dictionary.2 An audit of these sheets reveals structural anomalies and varying levels of data readiness:

| Metadata Worksheet | Inferred Business Purpose | Data Readiness & Quality Profiling | Recommended Tactical Actions |
| :---- | :---- | :---- | :---- |
| data\_feed\_columns | Canonical reference dictionary mapping raw clickstream schema headers to technical data types.2 | Highly complete; contains technical data types dominated by varchar(255).2 Notes column is 95.6% null but contains nine tagged "Anomaly Detection" rows.2 | Extract and normalize the preamble documentation text into clean schema column headers.2 Prioritize the nine tagged rows for core performance metrics.2 |
| post\_eVar | Custom persistent conversion dimensions capturing user journey context, campaigns, and attributes.2 | Status is clean and fully populated (only contains Enabled or Disabled).2 Name column has duplicated values, and descriptive fields are 100% null.2 | Disambiguate duplicate eVar business names through stakeholder interviews.2 Use the eight tagged "Anomaly Detection" rows for dimension-reduction steps.2 |
| post\_prop | Custom traffic and pathing dimensions documenting page-level details and custom pathing events.2 | Status is clean, but the friendly name column is 32% null.2 Custom descriptions and notes are 100% null.2 No active anomaly detection tags exist.2 | Backfill null friendly names.2 Preserve these dimensions as secondary factors for Phase 2 diagnostic segment breakdown.2 |
| post\_event\_list | Numeric custom events and metric slots capturing business-critical transactions (e.g., quotes, policy purchases).2 | Event-name is 100% distinct and complete.2 Friendly names are 79.6% null, and statuses contain inconsistent casing (Enabled, ENABLED, Disabled).2 | Normalize status column casing to uppercase strings.2 Prioritize the twelve rows explicitly tagged "Anomaly Detection" as the primary target metrics.2 |

Data governance is of paramount importance when processing health insurance clickstream data.6 Raw feeds containing visitor IP addresses, persistent browser cookie keys, customer account IDs, and query strings in URLs must be isolated and processed within a secure zone.2 The ingestion layer must apply one-way SHA-256 hashing to unique visitor tokens and mask query parameters on internal referrers prior to exposing data to any analytical workloads.2 This ensures that downstream machine learning models process zero personally identifiable information (PII) or protected health information (PHI), maintaining compliance with federal regulations and internal security standards.7

## **Phase 1: Anomaly Detection Architecture**

An enterprise anomaly detection system requires a bi-temporal approach: an offline training and batch processing pipeline to establish deep historical baselines, and an online streaming pipeline to detect deviations in near real-time.9 These workloads run on a unified Azure Databricks Lakehouse platform, leveraging Unity Catalog for centralized governance and MLflow for operational model tracking.9

### **Phase 1 Offline Solution: Scheduled Batch Modeling**

The offline pipeline operates on a scheduled batch frequency, executing once daily to retrain models and evaluate historical tables for structural or business anomalies.9 The pipeline follows the medallion architecture principles to progressively refine raw data.12  
During the extraction phase, raw Adobe Analytics log files stored in Azure Data Lake Storage (ADLS Gen2) are loaded into the Bronze Delta Lake layer.12 In the Silver layer, the system normalizes event names, filters out search-engine crawlers, and reconstructs sessions using the composite Visit ID formulas.1 The clean records are then aggregated into uniform hourly and daily intervals to create multi-dimensional time series targets.14 These targets are stored in the Feature Store.12  
For statistical and forecasting-based anomaly detection, the Python Darts library provides a robust wrapper around classic models such as Triple Exponential Smoothing (Holt-Winters) and modern deep learning models.16 The Holt-Winters model captures multiplicative seasonal patterns (![][image4]), modeling local trends (![][image5]) and seasonal variations (![][image6]) across the historical reporting periods 17:  
![][image7]  
where the level smoothing equation is:  
![][image8]  
the trend smoothing equation is:  
![][image9]  
and the seasonal smoothing equation is:  
![][image10]  
To capture complex, non-linear interactions across multivariate features without explicit seasonal tuning, the platform utilizes the PyOD library to train unsupervised Isolation Forest estimators.10 The Isolation Forest isolates anomalies by randomly partitioning the multidimensional feature space.19 Because anomalous transactions lie far from the dense clusters, they require fewer splits to isolate.19 The anomaly score (![][image11]) for a sample ![][image12] over a dataset size ![][image13] is calculated as:  
![][image14]  
where ![][image15] represents the expected path length (number of splits) across the ensemble of isolation trees, and ![][image16] is the average path length of an unsuccessful search in a binary search tree built on ![][image13] nodes, serving as the normalization factor 19:  
![][image17]  
When the anomaly score approaches ![][image18], the path length is highly compressed, signaling a definitive outlier.19 Trained model artifacts, hyperparameters, and scaler parameters are registered and versioned in the MLflow Model Registry, making them available for downstream production tasks.9

### **Phase 1 Online Solution: Real-Time Streaming Ingestion and Inference**

The online pipeline ingests live telemetry from CoverMe through Azure Event Hubs via its Kafka-compatible endpoint, processing messages inside Databricks Structured Streaming.13 This streaming data-flow utilizes Delta Live Tables (DLT) running in continuous mode with enhanced autoscaling to handle unpredictable web traffic spikes.9 Raw JSON payloads from the stream are processed by an ETL script that converts the binary payload body into a string schema 22:

Python  
\# Spark Structured Streaming schema extraction example  
import dlt  
from pyspark.sql.functions import col, from\_json  
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

schema \= StructType() \\  
   .add("post\_visid\_high", StringType()) \\  
   .add("post\_visid\_low", StringType()) \\  
   .add("visit\_num", IntegerType()) \\  
   .add("visit\_page\_num", IntegerType()) \\  
   .add("date\_time", StringType()) \\  
   .add("event\_list", StringType())

@dlt.table(  
    name="coverme\_silver\_streaming",  
    comment="Parsed and cleaned live telemetry from CoverMe"  
)  
def parse\_stream():  
    return (  
        spark.readStream  
       .format("kafka")  
       .option("kafka.bootstrap.servers", "eh-coverme-namespace.servicebus.windows.net:9093")  
       .option("subscribe", "coverme-clickstream")  
       .load()  
       .select(col("value").cast("string").alias("json\_payload"))  
       .select(from\_json(col("json\_payload"), schema).alias("data"))  
       .select("data.\*")  
    )

The parsed streaming data is aggregated into micro-batches using a rolling window with defined watermarks to handle out-of-order data arrival.23 To execute real-time anomaly detection, the pipeline loads the active MLflow-registered model as a PyFunc Spark User Defined Function (UDF).9 This UDF is applied directly to the incoming stream within the Gold Delta Live Table definition 9:

Python  
\# Streaming Inference utilizing MLflow UDF inside DLT  
import mlflow  
import dlt  
from pyspark.sql.functions import struct

run\_id \= "mlflow\_active\_run\_id"  
model\_name \= "coverme\_isolation\_forest"  
model\_uri \= f"runs:/{run\_id}/{model\_name}"

\# Register the model artifact as a scalable Spark UDF  
detect\_anomalies\_udf \= mlflow.pyfunc.spark\_udf(spark, model\_uri=model\_uri)

@dlt.table(  
    name="coverme\_gold\_anomalies",  
    comment="Real-time scored clickstream metrics with anomaly classification flags"  
)  
def stream\_inference():  
    features \= \["page\_views\_count", "session\_depth\_mean", "quote\_initiations\_sum"\]  
    return (  
        dlt.read\_stream("coverme\_silver\_streaming")  
       .withColumn("anomaly\_score", detect\_anomalies\_udf(struct(\*features)))  
       .withColumn("is\_anomaly", col("anomaly\_score") \> 0.65)  
    )

The system continuously scores incoming metric intervals.9 If the scored output exceeds the dynamic threshold set by the model's contamination parameter, an active anomaly flag is generated.19 This flag triggers immediate downstream routing to alerting endpoints, ensuring that operational teams are notified of deviations within seconds of occurrence.11

## **Phase 2: Root-Cause Investigation and Diagnostic Intelligence**

Once an anomaly is flagged, the system must determine the root cause, transitioning from detection to diagnostic investigation.15 This diagnostic process operates on both offline and online coordinates to isolate the technical or business drivers behind the statistical deviation.15

### **Phase 2 Offline Solution: Deep Batch Diagnostic Analysis**

The offline diagnostic framework replicates and extends advanced association techniques, such as Adobe's Contribution Analysis, to evaluate categorical dimensions (e.g., browser type, geographic region, referral URLs) and determine their statistical influence on the anomaly.14 When a daily batch metric deviates from the baseline, the system initiates a structured calculation across all associated metadata dimensions.14  
For each categorical dimension, the pipeline constructs a contingency table comparing metric distribution across historical (expected) and anomalous periods.14 The association strength of the nominal variable is measured using Cramer's V statistic 14:  
![][image19]  
where ![][image20] represents the Pearson chi-squared statistic computed from the observed and expected frequencies, ![][image21] is the total count of occurrences, ![][image22] is the number of dimension categories, and ![][image23] is the number of temporal periods (pre-anomaly vs. anomaly day).14 Cramer's V ranges from ![][image24] (no association) to ![][image25] (perfect association), helping isolate which categorical metadata fields (e.g., eVar\_12 or prop\_5) have shifted significantly.2  
To pinpoint the exact dimension value driving the shift, the pipeline calculates Pearson's Residuals with a finite sample correction for each unique dimension item 14:  
![][image26]  
where ![][image27] is the observed metric frequency on the anomalous day, ![][image28] is the expected frequency derived from the historical baseline, and ![][image29] is the marginal probability of that dimension item.14  
These residuals follow a standard normal distribution, allowing direct comparison across dimensions regardless of scale.14 The final score combines the Cramer's V value and the Pearson's Residuals to generate a normalized contribution score between ![][image24] and ![][image25], identifying the specific metadata items that contributed to the anomaly.14

### **Phase 2 Online Solution: High-Throughput Streaming Diagnostics**

For real-time streaming operations, computing exhaustive Cramer's V tables across millions of historical records introduces processing latency.14 The online diagnostic engine instead uses localized micro-batches to compute diagnostic features immediately after an anomaly is flagged.21  
When an anomaly is flagged in the streaming Gold table, the diagnostic engine extracts the anomalous records and the preceding sliding window (e.g., 15 minutes).21 The engine executes a lightweight Spark aggregation to compute localized residual shifts across high-cardinality dimension values.20 These dimensional shifts are ranked, and the top-ranking factors are packaged into a structured diagnostic context payload.14 This payload is immediately routed to the real-time agentic workflow layer, enabling rapid root-cause identification and automated operational mitigation.7

## **Generative AI Orchestration and Autonomous Agent Teams**

Integrating generative artificial intelligence into the anomaly detection and diagnostic pipelines transforms the system from passive alerting to active, natural language root-cause analysis.15 The platform leverages generative models and specialized multi-agent workflows to streamline diagnostics in both Phase 1 and Phase 2\.

### **Gen-AI Application in Phase 1: Configuration and Constraint Modeling**

In Phase 1, generative models automate data quality monitoring and validate pipeline configurations.26

* **Dynamic Data Quality Constraints:** Using metadata from the Unity Catalog, an LLM evaluates table lineage, freshness, and null distributions.9 It then generates appropriate expectation thresholds for Delta Live Tables.9  
* **Predictive Model Fine-Tuning:** Generative agents monitor execution logs and metric drift.11 When performance degrades, the agents suggest hyperparameter tuning schedules (e.g., adjusting the Isolation Forest contamination rate).9 This reduces false positive rates without manual intervention.15

### **Gen-AI Application in Phase 2: Multi-Agent Investigative Framework**

In Phase 2, generative AI is deployed using a multi-agent orchestration framework.32 The framework is modeled on the ai-data-science-team architecture, where specialized agents cooperate under a supervisor to resolve diagnostic queries.30

1. **Supervisor Orchestrator:** Manages the diagnostic lifecycle.33 Upon receiving an anomaly trigger, it decomposes the diagnostic task, delegates to specialized sub-agents, and synthesizes their outputs into a final root-cause report.33  
2. **SQL Database Agent:** Formulates and executes analytical queries against Silver and Gold Delta tables using natural language.30 It calculates localized Cramer's V values and Pearson's Residuals 14, identifying which custom eVars or event codes shifted during the anomaly window.2  
3. **System Status Agent:** Connects to Azure platform logs, application deployment schedules, and API gateway health feeds.11 It checks for system upgrades, network outages, or application deployments that coincide with the anomaly timestamp.11  
4. **Interpretability Agent:** Consumes the quantitative outputs from the SQL Database Agent, the timeline of system changes from the System Status Agent, and the historical baselines.30 It translates these data points into a natural language report explaining both the *what* and the *why* of the anomaly.15

INCIDENT SUMMARY REPORT: MANULIFE COVERME  
Incident Reference: INC-2026-89472  
Target Metric: Quote Initiations DLT (post\_event\_list: event\_14)   
Detection Timestamp: June 30, 2026, 14:15:00 UTC \[1\]  
Deviation Amplitude: \-42% relative to historical 14-day baseline \[17, 26\]

ROOT CAUSE ANALYSIS:  
The SQL Database Agent identified a significant deviation in Quote Initiations. Contribution analysis indicates high dimensional association with Safari v17.4 on iOS (Cramer's V: 0.81, Pearson's Residual: \+14.2). 

The System Status Agent found a correlating deployment to the CoverMe Ontario landing pages at 14:02:11 UTC.\[2, 11\] Dynamic testing of the deployment branch shows a JavaScript parsing error under Safari browser rendering contexts. This error prevented form-validation handlers from firing, causing user-facing errors and reducing submissions.\[26, 27\]

RECOMMENDED ACTIONS:  
1\. Revert the frontend commit 'prod-on-v2.4.1' deployed to CoverMe Ontario endpoints.  
2\. Flush edge network CDN caches to restore the stable fallback state.

This multi-agent process accelerates incident resolution, translating complex system telemetry into actionable business insights.15

## **Lucid Diagram Specifications for Architectural Pipelines**

To implement the end-to-end anomaly detection and diagnostics pipelines in Lucidchart, the architecture is divided into clear operational sections. The following diagrams provide the structural layouts and processing logic for both phases.

### **Phase 1: Anomaly Detection Architecture**

#### **Offline Scheduled Batch Modeling Pipeline**

This pipeline executes daily on a batch schedule to ingest historical clickstream data, normalize features, train PyOD and Darts models, and register verified model versions in the MLflow Registry.

Code snippet  
graph TD  
    %% Define Node Styles  
    style A fill:\#f9f,stroke:\#333,stroke-width:2px  
    style B fill:\#bdf,stroke:\#333,stroke-width:1px  
    style C fill:\#bdf,stroke:\#333,stroke-width:1px  
    style D fill:\#bdf,stroke:\#333,stroke-width:1px  
    style E fill:\#fff,stroke:\#333,stroke-width:1px  
    style F fill:\#f96,stroke:\#333,stroke-width:2px  
    style G fill:\#f96,stroke:\#333,stroke-width:2px  
    style H fill:\#9f9,stroke:\#333,stroke-width:2px  
    style I fill:\#fff,stroke:\#333,stroke-width:1px

    %% Flow Structure  
    A \--\>|Hourly/Daily Exports| B(Bronze Delta Lake: Raw Ingestion)  
    B \--\>|Schema Mapping & TSV Parsing| C(Silver Delta Lake: Session Reconstruction)  
    C \--\>|Aggregations & Feature Store| D(Gold Delta Lake: Time-Series Vectors)  
      
    subgraph Machine Learning Training Runtime  
        D \--\>|Feature Arrays| E  
        E \--\>|Subsequences W x D| F  
        E \--\>|Target Series| G  
        F \--\>|Anomaly Scores| H{MLflow Model Registry}  
        G \--\>|Expected Confidence Bands| H  
    end  
      
    H \--\>|Registered Model Run ID| I

#### **Online Real-Time Streaming Pipeline**

This pipeline operates continuously to ingest live telemetry from CoverMe web clients, parse incoming JSON events, execute low-latency model inference, and output dynamic anomaly flags.

Code snippet  
graph TD  
    %% Define Node Styles  
    style A fill:\#f9f,stroke:\#333,stroke-width:2px  
    style B fill:\#bdf,stroke:\#333,stroke-width:1px  
    style C fill:\#bdf,stroke:\#333,stroke-width:1px  
    style D fill:\#fff,stroke:\#333,stroke-width:1px  
    style E fill:\#f96,stroke:\#333,stroke-width:2px  
    style F fill:\#bdf,stroke:\#333,stroke-width:1px  
    style G fill:\#9f9,stroke:\#333,stroke-width:2px

    %% Flow Structure  
    A \--\>|Direct Real-Time POST| B(Azure Event Hubs Kafka Endpoint)  
    B \--\>|Structured Streaming Read| C(Bronze Streaming Delta Table)  
    C \--\>|Micro-Batch Watermarking| D  
      
    subgraph Real-Time Inference Container  
        E \--\>|Dynamic Model Load| D  
        D \--\>|Inference Score Computation| F(Gold Streaming Delta Table)  
    end  
      
    F \--\>|Contamination Threshold Check| G{Anomaly Alert Routing Hub}

### **Phase 2: Root-Cause Investigation and Diagnostics**

#### **Offline Scheduled Batch Investigation Pipeline**

This pipeline triggers daily to evaluate flagged historical anomalies, calculate categorical association statistics across CoverMe dimensions, and generate structured diagnostic reports.

Code snippet  
graph TD  
    %% Define Node Styles  
    style A fill:\#bdf,stroke:\#333,stroke-width:1px  
    style B fill:\#fff,stroke:\#333,stroke-width:1px  
    style C fill:\#f96,stroke:\#333,stroke-width:2px  
    style D fill:\#f96,stroke:\#333,stroke-width:2px  
    style E fill:\#fff,stroke:\#333,stroke-width:1px  
    style F fill:\#9f9,stroke:\#333,stroke-width:2px

    %% Flow Structure  
    A(Gold Delta Table: Flagged Anomalies) \--\>|Select Target Timestamp| B  
    B \--\>|Contingency Tables| C\[Cramer's V Association Processor\]  
    B \--\>|Observed vs Expected Matrices| D  
      
    subgraph Diagnostics Synthesis Engine  
        C \--\>|Feature Rankings| E  
        D \--\>|Residual Directionality| E  
        E \--\>|Top Contributing Items| F{Batch Diagnostics Dashboard}  
    end

#### **Online Real-Time Investigation Pipeline**

This pipeline processes streaming anomaly flags, joins them with platform events and system logs, and coordinates a multi-agent team to produce real-time diagnostic reports.

Code snippet  
graph TD  
    %% Define Node Styles  
    style A fill:\#bdf,stroke:\#333,stroke-width:1px  
    style B fill:\#fff,stroke:\#333,stroke-width:1px  
    style C fill:\#f9f,stroke:\#333,stroke-width:2px  
    style D fill:\#fff,stroke:\#333,stroke-width:1px  
    style E fill:\#fff,stroke:\#333,stroke-width:1px  
    style F fill:\#fff,stroke:\#333,stroke-width:1px  
    style G fill:\#fff,stroke:\#333,stroke-width:1px  
    style H fill:\#9f9,stroke:\#333,stroke-width:2px

    %% Flow Structure  
    A(Gold Streaming Delta Table: Active Flag) \--\>|Extract 15-Min Sliding Window| B  
    B \--\>|Incident Context| C  
      
    subgraph Multi-Agent Investigative Team  
        C \<--\>|Query Commands| D  
        C \<--\>|Deployment Log Scrape| E  
        C \<--\>|Synthesis Prompt| F\[Interpretability Agent\]  
    end  
      
    D \--\>|Dimensional Shifts| G\[Unified Context Assembler\]  
    E \--\>|Outage Correlations| G  
    F \--\>|Natural Language Report| G  
    G \--\>|Structured Payload| H{Actionable Operations Dashboard}

### **Lucidchart Layout Mapping Tables**

To construct these diagrams in Lucidchart, developers can utilize the following structural mapping guides to coordinate shapes, connections, and system actions:

#### **Lucidchart Layout Guide: Phase 1 (Anomaly Detection)**

| Source Component | Target Component | Lucidchart Shape | Connector Label | Business/Technical Process |
| :---- | :---- | :---- | :---- | :---- |
| ADLS Gen2 Log Storage | Bronze Delta Table | Cylinder (Database) | Scheduled Spark Job | Extracts raw hourly/daily exported clickstream logs from cloud storage.12 |
| Bronze Delta Table | Silver Delta Table | Process Block | Schema Normalization | Reconstructs visitor, session, and occurrence IDs; applies hashing.1 |
| Silver Delta Table | Gold Delta Table | Process Block | Metric Aggregation | Groups metrics into fixed temporal windows and registers them in the Feature Store.12 |
| Gold Delta Table | PyOD / Darts Training | Process Block | Train Feature Array | Feeds aggregated metric vectors into the model training runtimes.9 |
| PyOD / Darts Training | MLflow Registry | Document Shape | Register Run ID | Registers completed model run details, hyperparameters, and scaler matrices.9 |
| CoverMe Client SDK | Event Hubs | Cloud Shape | Secure HTTPS POST | Direct streaming ingestion of live user activity from CoverMe digital properties.20 |
| Event Hubs | Spark Streaming | Cylinder (Database) | Structured Streaming | Continuous consumption of payload arrays using the Kafka endpoint.13 |
| Spark Streaming | Gold Streaming Table | Decision Diamond | PyFunc UDF Inference | Applies the loaded Spark model UDF to evaluate sliding metrics.9 |
| Gold Streaming Table | Alerting Hub | Process Block | Alert Dispatch | Routes classified alerts to operations teams when thresholds are exceeded.11 |

#### **Lucidchart Layout Guide: Phase 2 (Diagnostics & Root-Cause)**

| Source Component | Target Component | Lucidchart Shape | Connector Label | Business/Technical Process |
| :---- | :---- | :---- | :---- | :---- |
| Flagged Anomalies | Dimension Extractor | Process Block | Trigger Target Query | Retrieves historical context data surrounding the flagged timestamp.15 |
| Dimension Extractor | Cramer's V Engine | Process Block | Nominal Variables | Calculates the overall statistical association for categorical dimensions.14 |
| Dimension Extractor | Pearson Residuals | Process Block | Value Cardinality | Isolates the direction and strength of shifts for individual dimension items.14 |
| Cramer / Pearson | Synthesis Engine | Process Block | Rank Coordinates | Combines associations and residuals into a normalized contribution score.14 |
| Streaming Gold Table | Diagnostics Prep | Process Block | Localized Capture | Extracts preceding 15-minute diagnostic values during active alert events.21 |
| Diagnostics Prep | Supervisor Agent | Process Block | Orchestrate Team | Delegates query, system tracking, and reporting tasks to sub-agents.33 |
| Supervisor Agent | Sub-Agents | Process Block | Dual-Path Message | Coordinates parallel execution across database, status, and reporting agents.30 |
| Synthesis / Sub-Agents | Operations Dashboard | Document Shape | Diagnostic Report | Delivers actionable reports, combining metric data with system context.11 |

## **Enterprise Platform Migration to Akka Runtimes**

As Manulife scale its enterprise generative AI capabilities to meet its $1 billion+ value-creation goals, the underlying data and model orchestration systems must evolve.7 While a Databricks and Spark streaming architecture provides a powerful data lakehouse foundation 9, high-volume, real-time agentic workflows require a runtime designed for stateful, low-latency execution.7 Manulife's selection of the Akka platform provides this robust, durable runtime foundation to support critical agentic AI applications at scale.7

### **Architectural Drivers for Stateful Akka Migration**

Migrating from a stateless, poll-and-query streaming architecture to the Akka platform introduces several key capabilities:

* **Durable Stateful Actors as Autonomous Agents:** In Akka, agents are deployed as stateful actors.36 Each actor encapsulates its own private state, such as visitor context, rolling metric counts, and localized baseline models.36 This state is isolated and secure, eliminating the risk of race conditions or resource contention.36 Memory and goals are kept in-memory with ultra-low latency, removing the need for frequent database lookups.29  
* **Akka Memory and Sharded State:** The Akka Memory layer provides a fast, sharded, and in-memory store with built-in durability.29 Agent states and session profiles are distributed across the cluster.29 In the event of system crashes or infrastructure failures, Akka's durable execution engine recovers states in milliseconds using event sourcing and automatic snapshotting.37  
* **Akka Streaming and Flow Control:** The Akka Streaming engine ingest and process high-volume telemetry feeds from the CoverMe web tag with precise backpressure control.29 This prevents downstream models and API gateways from being overwhelmed during peak traffic periods.29  
* **Akka Orchestration and Durable Workflows:** Akka Orchestration manages long-running, multi-step agentic tasks across distributed clusters.29 It ensures exactly-once execution of critical actions, such as scoring a model or triggering an alert, preventing duplicate side-effects.37 This provides consistent performance and reliable execution in highly regulated insurance environments.7

### **Target State Akka Agentic Architecture**

In the target state, the analytics ingestion, real-time anomaly detection, and Phase 2 investigative agents are fully integrated into Akka's actor-driven environment.29

* **Ingest and Stream Parsing:** Clickstream data from CoverMe web clients is streamed directly to Akka Ingestion Actors.29 These actors parse payloads, apply hashing policies to sensitive fields, and manage session boundaries in memory, reducing processing overhead.2  
* **Stateful Agent Actors:** Each distinct user session is assigned an active Session Actor.36 This actor tracks real-time occurrences, page depths, and conversion steps, comparing them dynamically against baseline models loaded via the actor's configuration.3  
* **Durable Event Sourcing:** State changes within the actors are persisted using event sourcing.38 Akka continuously logs these state mutations to a durable storage backplane, enabling millisecond recovery of in-memory states during system restarts.38  
* **Active-Active Replication:** Session states and metric baselines are replicated across cloud regions using active-active replication, ensuring high availability and supporting disaster recovery strategies.6  
* **Integrated Multi-Agent Diagnostics:** The supervisor and investigative agents (SQL Database, System Status, and Interpretability agents) are deployed as orchestrator actors.29 They handle diagnostics, coordinate tool executions, and generate reports under Akka's supervision tree, ensuring resilient operation.36

### **Implementation and Migration Pathway**

To ensure system reliability and avoid operational downtime, the migration from the Databricks streaming architecture to the Akka platform follows a structured path.6

* **Phase 1: Dual-Writing and Shadow Processing:** Deploy the Akka Streaming ingest gateway in parallel with Azure Event Hubs.20 The gateway ingests CoverMe clickstream payloads and routes them to both the existing Databricks Bronze layer and the new Akka cluster, establishing a dual-writing phase.6  
* **Phase 2: Model Porting and Actor Integration:** Package trained anomaly detection models from MLflow into standardized, containerized environments.9 Integrate these models with Akka's orchestration runtime to support low-latency inference calls.29  
* **Phase 3: Stateful Migration and Event Sourcing:** Configure Akka Persistence to store actor state mutations to a durable database backplane, enabling stateful recovery and event-sourced tracking across the cluster.38  
* **Phase 4: Validation and Cutover:** Validate Akka's anomaly alerts, model performance, and diagnostic report outputs against the Databricks baseline.9 Once the validation criteria are met, route primary production traffic directly to the Akka system and decommission the intermediate Databricks streaming resources.6

For Maven-based Java development runtimes, the project imports the canonical Akka Parent BOM to coordinate and pin compatible SDK dependencies across all target microservices 36:

XML  
\<parent\>  
    \<groupId\>io.akka\</groupId\>  
    \<artifactId\>akka-javasdk-parent\</artifactId\>  
    \<version\>3.5.2\</version\>  
\</parent\>

\<dependencies\>  
    \<dependency\>  
        \<groupId\>io.akka\</groupId\>  
        \<artifactId\>akka-javasdk-client\</artifactId\>  
    \</dependency\>  
\</dependencies\>

This migration path allows Manulife to transition from a stateless data lakehouse model to a stateful, low-latency agentic platform, ensuring long-term scalability and robust operational reliability.7

## **Strategic Recommendations and Implementation Timeline**

The deployment, scaling, and eventual platform migration of the anomaly detection and diagnostics system is structured into a logical 12-month implementation schedule.

Month 1-3: Phase 1 Offline Baselines & Metadata Preparation  
\[=================================================\>\]  
  \- Normalization of CoverMe Data Dictionary & Schemas.  
  \- Setup of MLflow Model Tracking Infrastructure.  
  \- PyOD & Darts batch baseline model training.

Month 4-6: Phase 1 Online Streaming & Phase 2 Diagnostic Routines  
\[=================================================\>\]  
  \- Structured Streaming via Event Hubs and DLT.\[20, 21\]  
  \- Serving of real-time MLflow model UDFs.  
  \- Integration of Gen-AI Multi-Agent Diagnostic teams.

Month 7-9: Parallel Verification & Akka Actor Prototyping  
\[=================================================\>\]  
  \- Shadow validation of streaming alerts and reports.  
  \- JVM environment preparation and Akka SDK modeling.  
  \- Porting baseline model configurations into Actor structures.\[36, 40\]

Month 10-12: Complete Stateful Akka Migration & Demobilization  
\[=================================================\>\]  
  \- Activation of active-active sharded actor memory clusters.  
  \- Validation of durable recovery and event-sourcing.  
  \- Cutover of production traffic to the stateful Akka engine.

To coordinate this timeline, project teams must track the following key milestones and technical check-points:

| Operational Phase | Technical Milestones | Key Technologies | Governance & Quality Controls |
| :---- | :---- | :---- | :---- |
| **Phase 1 Offline** (Months 1–3) | \- Normalize data dictionary schemas.2 \- Train historical PyOD/Darts baselines.16 \- Setup MLflow model registry.9 | \- Azure Databricks (ML Runtime) \- PyOD & Darts libraries 16 \- Delta Lake, MLflow 9 | \- Verify PII masking on source paths.2 \- Validate model MAPE bounds (\< 15%).14 \- Confirm schema consistency.43 |
| **Phase 1 & 2 Online** (Months 4–6) | \- Implement Event Hubs ingestion.13 \- Deploy streaming DLT pipelines.9 \- Setup Gen-AI diagnostic agents.30 | \- Azure Event Hubs 13 \- Databricks Structured Streaming \- Gen-AI LLM APIs 30 | \- Enforce strict watermarking limits.23 \- Review database access permissions.33 \- Monitor system API and token quotas.6 |
| **Akka Parallel** (Months 7–9) | \- Establish parallel streaming feeds.6 \- Build prototype JVM actor structures.36 \- Integrate dynamic inference models.29 | \- Akka SDK Runtime 36 \- Java Development Kit (JDK 17\) \- Docker container runtimes 41 | \- Verify message latency metrics (\< 50ms). \- Audit mTLS and zero-trust certificates.38 \- Validate shadow alert alignment.15 |
| **Akka Target State** (Months 10–12) | \- Enable sharded Akka memory.29 \- Configure event persistence.38 \- Execute active production cutover.6 | \- Akka Memory & Persistence 29 \- Akka Orchestration workflow engine 39 \- Azure AKS (Kubernetes) 6 | \- Confirm multi-region replication.6 \- Validate stateful recovery behaviors.38 \- Confirm compliance with EU AI Act and GDPR.6 |

By executing this strategic migration path, Manulife builds a secure, scalable, and highly available anomaly detection platform.7 This architecture protects system performance, enhances the customer experience, and supports the enterprise's long-term AI-driven goals.7

#### **Works cited**

1. Adobe Analytics — Data Feed\! \- Sainath Revankar, accessed June 30, 2026, [https://sainathrevankar.medium.com/adobe-analytics-data-feed-f0bda405aae2](https://sainathrevankar.medium.com/adobe-analytics-data-feed-f0bda405aae2)  
2. shareable\_excel\_field\_analysis.md  
3. From data to insights: Navigating Adobe Analytics feeds \- Experience League, accessed June 30, 2026, [https://experienceleague.adobe.com/en/perspectives/from-data-to-insights-navigating-adobe-analytics-feeds](https://experienceleague.adobe.com/en/perspectives/from-data-to-insights-navigating-adobe-analytics-feeds)  
4. timvw/adobe-analytics-datafeed-datasource: Apache Spark data source for Adobe Analytics Data Feed \- GitHub, accessed June 30, 2026, [https://github.com/timvw/adobe-analytics-datafeed-datasource](https://github.com/timvw/adobe-analytics-datafeed-datasource)  
5. Compare terminology for Analytics data passed through the Analytics source connector, accessed June 30, 2026, [https://experienceleague.adobe.com/en/docs/analytics-platform/using/compare-aa-cja/cja-aa-comparison/terminology](https://experienceleague.adobe.com/en/docs/analytics-platform/using/compare-aa-cja/cja-aa-comparison/terminology)  
6. Akka Automated Operations, accessed June 30, 2026, [https://akka.io/automated-operations](https://akka.io/automated-operations)  
7. Manulife Selects Akka to Operationalize Agentic AI within its ..., accessed June 30, 2026, [https://akka.io/blog/manulife-selects-akka-to-operationalize-agentic-ai](https://akka.io/blog/manulife-selects-akka-to-operationalize-agentic-ai)  
8. Manulife Selects Akka to Operationalize Agentic AI within its Enterprise AI Platform, accessed June 30, 2026, [https://www.prnewswire.com/apac/news-releases/manulife-selects-akka-to-operationalize-agentic-ai-within-its-enterprise-ai-platform-302707356.html](https://www.prnewswire.com/apac/news-releases/manulife-selects-akka-to-operationalize-agentic-ai-within-its-enterprise-ai-platform-302707356.html)  
9. Databricks for Anomaly Detection in Data Pipelines \- DataExpert.io, accessed June 30, 2026, [https://www.dataexpert.io/blog/databricks-anomaly-detection-data-pipelines](https://www.dataexpert.io/blog/databricks-anomaly-detection-data-pipelines)  
10. Near Real-Time Anomaly Detection with Delta Live Tables and Databricks Machine Learning, accessed June 30, 2026, [https://www.databricks.com/blog/near-real-time-anomaly-detection-delta-live-tables-and-databricks-machine-learning](https://www.databricks.com/blog/near-real-time-anomaly-detection-delta-live-tables-and-databricks-machine-learning)  
11. Streaming anomaly detection in oil pipelines using... \- Databricks Community \- 131369, accessed June 30, 2026, [https://community.databricks.com/t5/get-started-discussions/streaming-anomaly-detection-in-oil-pipelines-using-ml-models-on/td-p/131369](https://community.databricks.com/t5/get-started-discussions/streaming-anomaly-detection-in-oil-pipelines-using-ml-models-on/td-p/131369)  
12. Data Ingestion Reference Architecture \- Databricks, accessed June 30, 2026, [https://www.databricks.com/resources/architectures/data-ingestion-reference-architecture](https://www.databricks.com/resources/architectures/data-ingestion-reference-architecture)  
13. Use Azure Event Hubs as a pipeline data source | Databricks on Google Cloud, accessed June 30, 2026, [https://docs.databricks.com/gcp/en/ldp/event-hubs](https://docs.databricks.com/gcp/en/ldp/event-hubs)  
14. Statistical Techniques Used In Anomaly Detection | Adobe Analytics \- Experience League, accessed June 30, 2026, [https://experienceleague.adobe.com/en/docs/analytics/analyze/analysis-workspace/anomaly-detection/statistics-anomaly-detection](https://experienceleague.adobe.com/en/docs/analytics/analyze/analysis-workspace/anomaly-detection/statistics-anomaly-detection)  
15. Anomaly Detection Overview | Adobe Analytics \- Experience League, accessed June 30, 2026, [https://experienceleague.adobe.com/en/docs/analytics/analyze/analysis-workspace/anomaly-detection/anomaly-detection](https://experienceleague.adobe.com/en/docs/analytics/analyze/analysis-workspace/anomaly-detection/anomaly-detection)  
16. Time Series Made Easy in Python — darts documentation \- GitHub Pages, accessed June 30, 2026, [https://unit8co.github.io/darts/](https://unit8co.github.io/darts/)  
17. How Anomaly Detection is used to automatically find trends | Adobe Analytics, accessed June 30, 2026, [https://experienceleague.adobe.com/en/docs/analytics/analyze/legacy-report-builder/layout/anomaly-detection](https://experienceleague.adobe.com/en/docs/analytics/analyze/legacy-report-builder/layout/anomaly-detection)  
18. pyod 3.6.1 documentation, accessed June 30, 2026, [https://pyod.readthedocs.io/](https://pyod.readthedocs.io/)  
19. Anomaly Detection in Python with Isolation Forest \- DigitalOcean, accessed June 30, 2026, [https://www.digitalocean.com/community/tutorials/anomaly-detection-isolation-forest](https://www.digitalocean.com/community/tutorials/anomaly-detection-isolation-forest)  
20. Stream Processing with Databricks \- Azure Architecture Center | Microsoft Learn, accessed June 30, 2026, [https://learn.microsoft.com/en-us/azure/architecture/reference-architectures/data/stream-processing-databricks](https://learn.microsoft.com/en-us/azure/architecture/reference-architectures/data/stream-processing-databricks)  
21. Low-latency Streaming Data Pipelines with Delta Live Tables and Apache Kafka, accessed June 30, 2026, [https://www.databricks.com/blog/2022/08/09/low-latency-streaming-data-pipelines-with-delta-live-tables-and-apache-kafka.html](https://www.databricks.com/blog/2022/08/09/low-latency-streaming-data-pipelines-with-delta-live-tables-and-apache-kafka.html)  
22. Ingest Azure Event Hub Telemetry Data with Apache PySpark Structured Streaming on Databricks., accessed June 30, 2026, [https://techcommunity.microsoft.com/blog/analyticsonazure/ingest-azure-event-hub-telemetry-data-with-apache-pyspark-structured-streaming-o/3440394](https://techcommunity.microsoft.com/blog/analyticsonazure/ingest-azure-event-hub-telemetry-data-with-apache-pyspark-structured-streaming-o/3440394)  
23. Run your first Structured Streaming workload | Databricks on AWS, accessed June 30, 2026, [https://docs.databricks.com/aws/en/structured-streaming/tutorial](https://docs.databricks.com/aws/en/structured-streaming/tutorial)  
24. Transform data with pipelines | Databricks on AWS, accessed June 30, 2026, [https://docs.databricks.com/aws/en/ldp/transform](https://docs.databricks.com/aws/en/ldp/transform)  
25. Transform data with pipelines \- Azure Databricks \- Microsoft Learn, accessed June 30, 2026, [https://learn.microsoft.com/en-us/azure/databricks/ldp/transform](https://learn.microsoft.com/en-us/azure/databricks/ldp/transform)  
26. Anomaly detection in Databricks acts as an extra set of eyes over our business tables | by Amit Dass | Medium, accessed June 30, 2026, [https://medium.com/@amitdassit/anomaly-detection-in-databricks-acts-as-an-extra-set-of-eyes-over-our-business-tables-e179b45dee48](https://medium.com/@amitdassit/anomaly-detection-in-databricks-acts-as-an-extra-set-of-eyes-over-our-business-tables-e179b45dee48)  
27. Frequently Asked Questions (FAQs) | Adobe Analytics, accessed June 30, 2026, [https://business.adobe.com/products/adobe-analytics/faq.html](https://business.adobe.com/products/adobe-analytics/faq.html)  
28. PyOD Scorer — darts documentation \- GitHub Pages, accessed June 30, 2026, [https://unit8co.github.io/darts/generated\_api/darts.ad.scorers.pyod\_scorer.html](https://unit8co.github.io/darts/generated_api/darts.ad.scorers.pyod_scorer.html)  
29. Akka \- Enterprise Agentic AI \- Microsoft Marketplace, accessed June 30, 2026, [https://marketplace.microsoft.com/en-us/product/lightbend.akka?tab=overview](https://marketplace.microsoft.com/en-us/product/lightbend.akka?tab=overview)  
30. ai-data-science-team 0.0.0.9014 \- PyPI, accessed June 30, 2026, [https://pypi.org/project/ai-data-science-team/0.0.0.9014/](https://pypi.org/project/ai-data-science-team/0.0.0.9014/)  
31. Anomaly detection \- Azure Databricks | Microsoft Learn, accessed June 30, 2026, [https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-quality-monitoring/anomaly-detection/](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-quality-monitoring/anomaly-detection/)  
32. AI Data Science Team download | SourceForge.net, accessed June 30, 2026, [https://sourceforge.net/projects/ai-data-science-team.mirror/](https://sourceforge.net/projects/ai-data-science-team.mirror/)  
33. Releases · business-science/ai-data-science-team \- GitHub, accessed June 30, 2026, [https://github.com/business-science/ai-data-science-team/releases](https://github.com/business-science/ai-data-science-team/releases)  
34. ai-data-science-team 0.0.0.9009 \- PyPI, accessed June 30, 2026, [https://pypi.org/project/ai-data-science-team/0.0.0.9009/](https://pypi.org/project/ai-data-science-team/0.0.0.9009/)  
35. Anomaly Detection Overview | Adobe Customer Journey Analytics \- Experience League, accessed June 30, 2026, [https://experienceleague.adobe.com/en/docs/analytics-platform/using/cja-workspace/anomaly-detection/anomaly-detection](https://experienceleague.adobe.com/en/docs/analytics-platform/using/cja-workspace/anomaly-detection/anomaly-detection)  
36. The Akka Actor Model: A Foundation for Concurrent AI Agents | Pradeep Loganathan's Blog, accessed June 30, 2026, [https://pradeepl.com/blog/agentic-ai/akka-actor-model-agentic-ai/](https://pradeepl.com/blog/agentic-ai/akka-actor-model-agentic-ai/)  
37. Akka Orchestration: Guide, moderate, and control agents, accessed June 30, 2026, [https://akka.io/blog/akka-orchestration-guide-moderate-and-control-agents](https://akka.io/blog/akka-orchestration-guide-moderate-and-control-agents)  
38. Akka Agentic AI Platform, accessed June 30, 2026, [https://akka.io/akka-agentic-ai-platform](https://akka.io/akka-agentic-ai-platform)  
39. Akka Orchestration, accessed June 30, 2026, [https://akka.io/akka-orchestration](https://akka.io/akka-orchestration)  
40. Akka Agents \- Overview \- YouTube, accessed June 30, 2026, [https://www.youtube.com/watch?v=MI6OliRt0zI](https://www.youtube.com/watch?v=MI6OliRt0zI)  
41. The 7 Layers of Agentic AI Stack \[2026\] \- AIMultiple, accessed June 30, 2026, [https://aimultiple.com/agentic-ai-stack](https://aimultiple.com/agentic-ai-stack)  
42. GitHub \- yzhao062/pyod: A Python library for anomaly detection across tabular, time series, graph, text, image, and audio data. 60+ detectors, benchmark-backed ADEngine orchestration, and an agentic workflow for AI agents., accessed June 30, 2026, [https://github.com/yzhao062/pyod](https://github.com/yzhao062/pyod)  
43. Delta Lake table streaming reads and writes | Databricks on AWS, accessed June 30, 2026, [https://docs.databricks.com/aws/en/structured-streaming/delta-lake](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)  
44. Introducing my new Machine Learning Agent (AI Data Science Team) \- YouTube, accessed June 30, 2026, [https://www.youtube.com/watch?v=GMUhdrQs8\_A](https://www.youtube.com/watch?v=GMUhdrQs8_A)  
45. Reliable AI \- Akka is the Agentic AI Platform for Enterprises, accessed June 30, 2026, [https://akka.io/](https://akka.io/)

[image1]: <data:image/png;base64,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>

[image2]: <data:image/png;base64,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>

[image3]: <data:image/png;base64,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>

[image4]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAbCAYAAAB1NA+iAAAA6klEQVR4XmNgGAWjgMqgEYgPAzEjmrgbFjEUwALEf6Hsf0D8FUkOBP4DsTSaGAqYBMQeUDZI8XUkOWOoGF7gicQGKbZE4s8H4udIfJBX9JH4KMCGAdO2b0DcisQHufY0Eh8FvGWAKIABEQaIgdxIYiA+yFtYAUjSFImfDhUDAV8oG4Y1YYqQASgGXJD4oJhB9xJ6DKEAUADBbFgBpZEDEBR40Uh8vAAUrSADQN6AgTlALIjEhwNQQoLZDAM/0fgg8AlKg9SjpEx+BohidSh/IxDvQUjDQRYQvwNiJ3QJEAAJwlxRhiY3CmgNAJYpMitw/e9iAAAAAElFTkSuQmCC>

[image5]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA8AAAAaCAYAAABozQZiAAAA1ElEQVR4XmNgGLnAG4ifAPF/IH6IJkc0AGn2RBckBogwQDRzo0sQA9IZIJrJAt+AuBVdkBjAyACxtQhK/4LSrMiKcAGYk/mRxK5CxQgCbArvYhHDCkCK0BWii+0A4kYkPhgoMUAUTcIihqwZxAZFJwowhkqYIolNgYqVA7EuEIdA+SAaxIcDcagEyBAYAPF/IvFB4DQaHw6mM0A0yDNAoskPVZohmgHVcAzAAcQRUBodgGwFpQWyACzgQLbrIEsQA0Ca+YB4O7oEMYAZiIXRBUcBiQAANGIvT/pEGj4AAAAASUVORK5CYII=>

[image6]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAaCAYAAAC+aNwHAAAAuUlEQVR4XmNgGAWjgAbACYj/A/EjKL0dVRo/YATif0j8twwQQ4gGmgwQDaxQ/gwg7kdIEwb8DBADYBjkHXQAcqECuiAyEAbiNQwIQ+ajSuP2UjEDqiTIGyD+VijfG4hDgPgplMYAMBthABSgIL4lkpgOEEcj8VHALiD+CMSSQOzCANEMClRkcJoBYjBOAHI2yHm+6BJQgNP/xAB9BoQBc5AliAXqQPwbiN2BOAhNjmgASiewRDYKqAkA60kj2x1DP0oAAAAASUVORK5CYII=>

[image7]: <data:image/png;base64,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>

[image8]: <data:image/png;base64,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>

[image9]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAmwAAAA9CAYAAAAQ2DVeAAAFVUlEQVR4Xu3dTah1UxgA4CURIb+Rn4G+gfITyt8EKYTkJz9RkmRgYmRARjIwMJMYMPn6BhKZKsngZkIZmBCJAZEkKUVJfvbbPvu767x3n3P3uffsc+93vuept3PWu/c9Z519bu33rr32uqUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA+9SZObFHDjXxRE5O/JcTlfdzYgXebeKYnNylN5o4LidH8nVO7IE4fm838WDK/5batXNzAgDW3QOlLYQeyRtW7P4mLpw8j4Iln7A/muT7/NXEyZNYla54jAJz2eYVpsvyUBM35GTj4Sa+y8mRPNPEGZPn8QfDl9W2S5t4rmqHG0v7e/FTygPAUWEVBcI8McoSBUQt9+nf1O780sSJTdySN4zs1cnjk1PZ5bisiXdyco5zcmKAfHyjSPukiT8mz8cWxfd1KZf7lNshcnfkJACsu1NK/4lxlX5I7ShY6hG2uPR4fdWudX2PUbZVeWvyGKNRY1nkO1n0EmEcyzimfVZVsOXv6/YyPcIWXkntsMhxAYC1ESfF30t7eerbtG1VNko7yhaXZ08oW0/Kud2JwunT0v7sfWnbmLr+RHEzllmfuc+iBduvTRzIyYlVFWwHmzi2tN95jLb1fd6zynShHu3Y75Imvi/tyCoAHBXiBFifLPtOnLPEBPnt4sXDe/eLAi2Kh+dLexJ+rIl/pvaY3afIXzx5XJWuoIzPNqZFiqZFC7Z5x2sVBVsUXqc38VrZ/M7znMXOe9XzN0vb9+5Gj/o5AKy1fNKL9klVe2zP5kRp+xCFUd3uE/kPc3JkT5e2qLk2b9iBK3OispETcxxpBdvBnCiz+1T3Jfa5O7Xj+wCAtVefKKNwq9uzTqLL1HczQbxvXYT09SOKysifmjeU/v078RnzKGCOyw/vvVVcNu57/SgwZ80Ly+I1Hm/irryhcignKrm/MfKUc/Neu6//nSEFW9xRnN8vxzx97x+5vrt8c8FWi3b3OePSfozaDRFFfp43CQD7Wn3iirXMooCKYijmFv3YxJ3V9ixOmENino3U7pvPlNshcn1zyOJGgM+auDlvWIKYMxXvm0e07i3tXLp471lLj2RRaMwrqhaZT5j7s52+49kZUrDtVi5GY0mPWX3aqJ7X+8SNKV27W5omHoca+zMCwFLVRU99QoxRlKuq9hguKFtP1NHOo2Z5n1snj3W+ex5z5s6v8sv0QmmX8eiOS6wN18l93M52Bdsir7dowfZ3mb0UyNgF2zVl66hqfNa+QjdG3OolPPL3Hb8/nb7ifZ4xPyMALN3LpT35/Znycefo2LoJ5fH+EV9V22qx3EPcXJDFpa2YtP5NlVuk0Nmprr+1KII6sSBt9CvHo9U+e1mwPVW2LpkRo151X2PktW9+4W59UTYvvUd8PL15Sn7/i8rmz8Udpp0ryvTCz/m4d1FTsAGwFrqCYei8rJ0YWpTEiNvQ1e2713x9KjuuA2WzuDi+3jDHvIItRr9iEduhFi3YwtBjv2yL/CEwtI/d0i5RuA2lYANgLcSI0W05uWTbLflRywutzhKF3az/RTqWm0pbtA2dyB5zrWKe3edN3JO2hXzJcAzxnxTOzskVGPov0OJy6Ac5OcPPZbG1+Lo5by+V/pFbAGAibmyol+4YYuiIy5Es/qfq1Tk5klUfz/NyYo5V9w0AWKK+yenrZNEidrdOy4l9YD/2CQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgOX4H8dLFLL9qmaJAAAAAElFTkSuQmCC>

[image10]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAmwAAABLCAYAAADNo9uCAAAFPUlEQVR4Xu3dTchlYxwA8Ecok49JyUeUIRsRiymlTFmSTMJCviLFzkZRshBZKiZZaGQlCxY2QimzFGuN7EyilJRiMRqcf/eeeZ/7v1/nzMy95973/f3q33ue5znvvee+76nzv895zvOUAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAbKvzmzjRxL5Ud2FVBgBgQO81cUUT/1V1sX24KgMAMLAvmjhelevkDQCADRAJ2v7x9qXjcuvfahsAgAFcVCYTtCNN/FmV9bYBAAzsmjI9fu2x8XY8iPBJ1QYAwEAiSbu1iZPj7dYbTVxblXebL3PFBri6iQdzJQBA66oy3du2W73YxIFc2Tiviedz5Zq93sT1uRIA2LsiKYtErd2+JbWFx6u63SIno1Fu46XUNoR8fADAHtaOWYuf16W2mDz3klS3ifomWB81cVeuHNuUhO2zXAEAsM36JliLeq82JWGLXs+DuRIA9oLoMfplvH1zE7dXbcvEigDL4oHTe0+rb7nVCcOpapsz0zfBGjJh63MefJ/Kh8pofFt4pYmjVRsA7Bpxm+nH8fajdcOKfZPKD5VRwnhDmVzDkzPTN8EaKmHrex78lcoxiXE8LBE+LdZ7BWCXigH1s3o3Qi6v2ndl9nvmHpi9Gs+W+fK+s+LuduckxuRF+zzRtixhyz2rs6KreedByAnbW2Xn851MbfNeAwC2TszqH6Jnor7AXd7EB1V5lhMd4u3Tey8X739nruSMLEuwskXJTZeE7VxadB78msox5UiIHrn8GXJyBwBb6Z8yeZGr18h8t4yStnXKF1zOXN8Ea9HffoiEbZ4Pq+3Yr76lWv9eTHrcrk6xTIyLi9f9eVyO14mng+P8f3VcBwCDeaGJj8toKaafmniyalt00VwVPSLnTt8Ea9b/O/4fUV9HnCurNu88uLFMrjJxvImXyyi5imOLFRFacVu17X2L3uOH50SI16y/rLR/ixhL91xVDwAbZ9YFfNW6rtF5LFdsoCtzxZr1TdjeaeKRXDmQeedBfkJ0kb7nb73/7+Of+fYrAGycuEW0v+z0Uqxa3L6KHpRl2gvr/RO1myOmMYljjDVHh3Rbruigb5KzCovOgz7H9/f4500TtbPFOM52Ut6YPPjweDveb+jEGwAWittB69SOH1okngi8p4wusOs8vpiwtY+40F+cK7fAHWU0rcaQ5p0H35Z+Xx7iy0bXKT7q172s2o6nZ/u8JwBQdnpYYhmldeozbisu8H16gjbNfbliA0SP1zYmwACwJ7WJ0LG6cg36JGwxSD2O86kymtYEAGDPiFugR8rOqgzr1Cdhi7FTdQ/bNve2AQD08lUTT5fR2KSzdSBXLNEnYYsELcY+1eW+Y+BabgMCAFslEp9Z46ui563rBKk/NPFaEwdzQ5KXVIonZnPdrKWWZo1fqxO4drqILt4v068FALDR5iUv9QSpXcT8ZMsStqxrD1u8bn2cMVj+bG6P9t0fAGAw95adebLCH9V236RmlQnbBWU6QWuTyS5rs2Z9PxsAwOB+K9NJTPSwtZ4o04vPRxyq9lllwhZiua84xlOp/miZXJs1H2MbtfxZAQC2Toxdi+Qrera6WnXCNk+bfL05UbuYhA0A2HqR/ESy9nlumCMW+45F7r8uo9us6xTJV8yi3/Up1zjW+J1nmtiX2gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAoJT/AYLXRjMEncRBAAAAAElFTkSuQmCC>

[image11]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAoAAAAZCAYAAAAIcL+IAAAAcElEQVR4XmNgGAW0AjFA/AiIdwAxM5ocHPwF4nVQtiIQ/0eSgwMRIP6GxP/KgEOhJgNE4hUQ86DJYQCQQmTMiCoNAWJI7GkMEIUHkMTAAOQJdPeA+K1oYmBBSyT+aSC+hcSHA24GhKkgnIsqPQqoBQD9LBoZ9edZ3gAAAABJRU5ErkJggg==>

[image12]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAwAAAAaCAYAAACD+r1hAAAAjklEQVR4XmNgGAVDHjgB8S8g/gulg6DsL0BshqQODFYBsTkS/z8QNyCxTyOkIOA3EpuRAaJIHMr/CMT8CGlMoM8A0UA0AFlPUIMxEMtC2SDF/5DkQEAXmWPJAFH0kwHibhD7AJJ8DBIbDBQZIMEnA6XLGCCa5BkgHg5HKEUAbiAOQeKDQiWAARJio2AQAgAV+RyQtBzpcgAAAABJRU5ErkJggg==>

[image13]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAwAAAAbCAYAAABIpm7EAAAAmElEQVR4XmNgGAVDGnAD8V0g1kISuwDEj4A4BEkMDMSBeAeU/R+IvwCxH0IaLAZSAwfLgVgTygZJTkCSg4kZIwt4QmlpBogkMuCBioGcjAGKgPgTmlg0A6YhcPAbiNPRxECK/6GJgQELA0RSBEkM5C+QWCuUDzIQDkwZMK2eBBUDGZIBxXCwGCqJDBiB+BdUXA1NbhQMNQAAhvQekANzHekAAAAASUVORK5CYII=>

[image14]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAmwAAABDCAYAAAAh8FnvAAAD8klEQVR4Xu3dz6vmUxgA8CMUDUlESt1YkCws/FiIHWJBQln4A1hYTBQaJWVly5QNTbLRpLAgC4uJ3SwksbQgkSQri0l+nKf3nO5zj++9733v+2Nc8/nU0/c5z33v/b4zq6dzvt9zSgEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgCW9WOPVGo/V+KrVLt7+cfkp5eHeYdxdlfIPUw4AwJKuqfH8UPs55b+nPJwYxtl7Kb885QAALKE3bB+l2psp/7tdH2jXW9q1z7S9UuPblufm7ljKAQBYwjjDFsuhfXxejevLbLbsklaLz3fxs/hM913K3045AABLuLbGky1/rcalNT5t4yfa9VSNF1reZ9pi5u10y/vza7+2a7gn5QAArFheEh39OBaSm9v1uh1VAIB9+mAsrMkXY+GQenYsJFtjoXo/5belHABgrnjGqj8UvylnxsJZ8HCZbdUxRhbLmusOAIC5zkbTEE3iJpq272vcXeP+Mvt3vrPzxwAAh8MfY2FD1t0oxnNiX6fx1WV2z8tSbVWiAb1wLDZbYwEAYBHRaPQ3IDctHtwfN6RdpQfLrEG7MtVivI57fjMWknXcDwD4H3quxustz8+rxRYUUzvu/1nj/Brv1nipxu013qhxY/7QhKfK7PeiMXq5xp1pPIoNZvM2F5sQ3+PWsXhAJ8v2yQV3tevRMnsZ4a0aN7TaBS0AAPaUG6acT23e+kjZ3uy1z1KFuObzMKf0nfzHpcephi02mp2qdxeV2Sa082K/Pit7328RTw/j/j1iWbTPqD3UrmGR7wkAnKOiUemRnRrGozjYPDaLXcRNZed9ovkb79vtVl+1rbLae30+jPusXTwnF41mLDPnhvVIygEAdhW7849N29QMWxafnVoy3Us0M7mhOV6mlz7nzbCtSjSMscS7annW7JN2zS8exFJwyDNtAACToinKzUVukuJopX4GZvdxjR9anj8bjVcXM0ZXpHEWvxOzbHkcb2z2v9nFkU/5MPR1+WUY9+fNVmm3/4vw6FgAABh9mfLYlywv1UWz1s++7KLBilp+5isOPI8XF7pxpi4b630cLyRkx8riy62L6t8zh+VJAOA/KXbwv28sNvGc2ijv+B+zcH15L+sHn49ik9osfjdeXhj9Vdb79uQz5d/N2thMAgAcCgdtYn4bCws66H0BAM458fzVibE4x1aNO8biAk6X3U8GAABgwuNlPUc2TYlGb5lmDwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA9ucfTFekgxgTLeYAAAAASUVORK5CYII=>

[image15]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEUAAAAaCAYAAADhVZELAAADCElEQVR4Xu2YTahNURTHl1DkKyUS9VImYiIZMJABSlJiQFGGjBkoIwamBpKBSJSUuZnBzUgoJTKQAQMGBkooycf+dfbKfv+31373vndfJudXq3PPf+2zP9bZe699rllPT89/Zn6y6yrOkM8qDMmFZAtVzGxK9knFFq+SvR/SIn6pkDhgnf4n2TvxRTxMNk/FEaCt6PkTySZUnI5v+Tooxcy1ZE9VzNCRHSpmTlnn36+OCm+S3VNxRDZa194CdWTwbVOxRSsoQIXKhWS/VSxgFvLcEnUIG6wrt1gdM4Dld1/FzNVkP1RsEQVld76+LcUMA7mkYgH+VtCcx8k+qjhDfHbWWGWxr0oUlHNyX0IDi1QsmC5oDuX2qDgLWgP/kmydihG1oDyyOCgsiVbjbHj4N+crGzVXMoGCvkzFDBkF/wfrMojX+9XiTMXsZFbUeGDdbBqKMig0ih21OCjbrZ1VfBofKbSDWSvxQdbYal1Gcrxf5e/a4EkKtFXjuE1dDSG1mXLD4qDQ6EDFAjbZJ6IRqJ+iLbU4KOwz+B3KeZC+J7tb+EoG1u5362VOohYUiKYajQ5ULGAAmooJFNO3ZK3FQVEoN8zec9vioJCSZx0UTqsOb8nviTiDrEHDOtCVWdO025opJbU6IwYWB2UsM8XxDc+hk1EavWVTB8AZwbXVyW4WPi3rLE+2N/+mTm3vhdw7HB+iPeWMxWOcwnRB4chOOnP8zdegnPq4d42T6xbxlXsHcCotnyGjsMc5p607PNZgLOtVzLC0zquoXLZutybd8VH3LF8xKqBTBOROsiv5GQefplLu0XXvoH46+9wmZySg/GHRAH2Xdc/tzPcTyS5avTx4MCPY6PUFjBUaJ8UpJ63+YbbP6oc9zhu1byvqOGT/9rE11tXNNYKNOAoKbUe+sXHMxtPICuvqaQ12WKiHpVWDz4mXKs4FfGBFnRgFPuJm22GWVO1vDIeAzenScfwtj4PWgIaBfpDZavC3wjhe3khE3yGjQmCi/0NavFah4KzFmaqnp6dnTvkLXW/Nv+j1IOEAAAAASUVORK5CYII=>

[image16]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACUAAAAaCAYAAAAwspV7AAABpklEQVR4Xu2WTytGQRTGj2JhIQsLKRbEQhYWyso3UBaivOUjWLFQtr6BlJKysGQnX4CltWxsSCklKwvkz3nMTB3PneG+c6+V+6vTvfOcOaczc8/M+4o0/ENe1HpZLMkpC3Uw7a0KjyxUYV7tg8UMltXeWcwFBa2wmAlyobjKIFEni5mcSA27NaV2wWIF+qVkK7TETbz1zxvj21PbMONAl7i5OJFrah1+jFg80Ycpfi0KSXbNGAE2CIVit5gwBz682xwDXkvxKm5OlE0pBmPlV2b8JPEE5/65pfZsHcqkFPNakDO20C94V2LAHysq8CCuMMu+2h1plmtJFBXunrDiFFjVOIueHnE58AyE3krFgOROrYoLXmcHgVXNsOiJXapLRsM1MmR8geTuD0u6qAPzfiTxOeBMikXhswUtdeHCjx2NMituwpjaoLgmv5fvAfgMOIExEMu9c+x1nMYd8oERKS4kCrZ8gUVDKsmiWjeL4vqlj0UPDkUqX1vgyG+zmAF6DAVNsCOHUalndfh1uGSxCuiR3D94gR8bPJdDtTkWS/LGQkPDX/MJx6xgPX8biesAAAAASUVORK5CYII=>

[image17]: <data:image/png;base64,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>

[image18]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABoAAAAZCAYAAAAv3j5gAAAAwUlEQVR4XmNgGAWjAAsQB+LdQPwfiBeiyeED3AwQPXxQvjsQv0dIowJfBohifyhNikVfgfgZmhjIjAloYhiAVItA6svRxJ5AxfECUiziYYCot0ETPwAVxwtIsUiTAaLeGE0cpJ+qFoEsGLUIDkixCJaHQNkDGRyAiuMFpFgEAiD1RWhiD6HieAEui/gZIHLoBoAyLHpJAFIzBU0MDCQZEIagYxhgwSIGAjAHiEH5UQyYFlMVPGWAWLgCXWIUjIJhDgABgkVo1G+mwwAAAABJRU5ErkJggg==>

[image19]: <data:image/png;base64,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>

[image20]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABUAAAAaCAYAAABYQRdDAAAA+klEQVR4XmNgGESAEYgbgXgaugQlYA+UlgHi/0DMjyRHFtBngBgEA5+A+CsSn2xQg8R+y0AlQ5EByNU86IKUgFlALIQuSAkoBuIGdEFKwHMGSLKCgdNIbDgAeeMLAyR8HqHJwYA8EIszQJIPSB0ynoSkDg4EoHQ5A/Z0B0qXZWhiRAOQl0CGzkcSA/liDhKfLLCDAZG4QZbsQ5IjG4DSHMjQFiB+jyZHEQAZ+hddkFIAi1WqAFYGiGGgdAeijVGlyQMgL4PSowcDxNATqNKkA/RI+cdAYRDcAuJgNLEcBoihKlD+YiQ5gmA6A+7cAgoOkMFdDBCLR8FwAgDTqzR/TPMCTQAAAABJRU5ErkJggg==>

[image21]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABMAAAAaCAYAAABVX2cEAAAA3klEQVR4XmNgGAWUAhkgfoSEd6JKg8E5IJ4LxLOguB5VGhP8A+L/UIwL3EUXwAV+A/EUBohhOmhyIMANxC7ogrjAGiDmZIAY9gRNDgRABnGgC2ID0kAsAmXnMEAM9EBIg8EDND5OUI7EZmSAGPYeSQwE8IUlCgCFFzKARUQElA/yIigYCAIWBkzDGhgghv2E8rcCsQ1cFg/AZivMqzCvgZINyFKC4AEDIvCRAciLIMPqGEhIX/gCFua6InQJXOABugASWM4AMQyby+HAjwFhKwyDUjg2gB45o2AUDG0AADnUMiAz4gvtAAAAAElFTkSuQmCC>

[image22]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAsAAAAbCAYAAACqenW9AAAArElEQVR4XmNgGNpAH4h/AfF/IP6KJocBhIF4EgNE8Ro0OaygigGi2AZdAhv4xwBRTBQAKXyALogLgBSXI/G9gfg0EDMiiYGBEgNEMQeUPx+IWaFiC2GKYAAUEiA3g8APKA3yKEixJZQPB8+B+AoQ/2bAYi06AJkAw+/R5FCAJwNqkOWg8VHAVgZUySAkPsgvKG5GjwxQ8D2EslFs4IYKlCELQsVAmKBnR8EAAgC3ySnuFEvKSgAAAABJRU5ErkJggg==>

[image23]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAoAAAAZCAYAAAAIcL+IAAAAZ0lEQVR4XmNgGAXUBJxAfBeItaD8MiB+BMSxcBVQ8B+IGaF0IxA7ATE3EP9DVjQFiC2hbJDCZiQ2CMOBJ5QWhErwIMlhBdEMaCbgAm+BuBVdEBsAmaaELogO/BiItHYOA5EKRwFlAABxJBMYXHlbOQAAAABJRU5ErkJggg==>

[image24]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAoAAAAaCAYAAACO5M0mAAAAiUlEQVR4XmNgGKaAE4j/A/ElKH0KVRoCGBkgkqxIYl+B+BYSHwyeM0AUIgMZLGJgAXRBHqiYEkxAEioAsgoZcEPFo2ECxlABdIUgABJfCONYQQUIKiTaRHGoALpCmGfgbgQBbL4WhIrBfQ0C2MJRH4sYPGb4kcT+MmCJGRCAhdszKI01rkcBZQAAkp0rxkDK6hYAAAAASUVORK5CYII=>

[image25]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAoAAAAaCAYAAACO5M0mAAAAOklEQVR4XmNgGOagB4gXogvCgC8Q/0fCOBUig1GFeAF1FWoyQBReBWIBNDkwkGSAKHiEhkFio4CKAADRKBtlymUYLAAAAABJRU5ErkJggg==>

[image26]: <data:image/png;base64,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>

[image27]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABYAAAAaCAYAAACzdqxAAAAA+ElEQVR4Xu2UPQrCQBCFn2ihKNgKNnoMQUFsbaw8g4WVFoKVxxAbsRJv4S1stLVTKyvxZ5ZJxDyXZEMEEfzgsWTe7AuzbAL8+SZt0Z3UDXTEpAUNKbEhnKFekY0o+tCNMzY8qlB/z0YUZtOJi4R/LM74Y0YRK7gGbV6yYSFW8AXanGWDqCNmsGvzGtp3o3qHnp+4Bvt9PUvdiktwBdozp3ooB0QHX/F+FdOiBdUCNKDBZrWxg/3FG1FBNGDjlTx08xZ6O1KisVcznzlTFmWgV9SsoZjRptCxj6Jh0LZimyQxE+hxmH9MLmglw9yQkWjFxidocuHPj/MAQAA/g+9IplQAAAAASUVORK5CYII=>

[image28]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABUAAAAaCAYAAABYQRdDAAAA7ElEQVR4XmNgGAW0BpJAPItIrAnVQzT4B8T/gdgGXQII+BggciQbCtIEwizoElAAkuNBFyQEQJp+owsiAZA8SUCaAaIpHU38KxIbn4VYQRUDxFARJDEVBlRDSQawSELH0ciKSAEcDBAD5qOJg8QE0cRggJMBeyqBAxcGiAHGaOLIEQNKEZZI/LtA/BqJjwGeMxCO2WfoAoQALPxwAUMgfo/ElwHiNCQ+VkDIUJCcB5QNMjABKgZio4AIqAQx+AlUDwjYQWlQaqEqAGWUVnRBSgEsfI+hiFIIQF4HlVbB6BKUAEYg5kcXHAVDBAAA8bw/MVjGXdsAAAAASUVORK5CYII=>

[image29]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAbCAYAAAB1NA+iAAAA40lEQVR4XmNgGAWjgIqAFYhvALEHlG8OxI+A+DBcBR7ACcRPoOz/UIwsh8zHClqB2AXKBimuRJKDiZmiiaEAZyBmAWIOBohiESQ5kDhIzBdJDKeLQK5AlzSGiukjiR1AYqMAUDjMRxO7CsT/0MRwAnS/MkLFXKF8eSCeDMS74CqQgAwDRHEEktgeqBgMXGSAyH9CEoODrQwQxSAvgGgQ1kJRAQEgcR10QRAA+RM9ALEBrGq4GSASoADDB8SBeAoDJGpRwHkGiAFTgVgXTQ4ZgAL1PRD3oEvMguKVQOyNJjcK6AEAxuQvZ7/WarwAAAAASUVORK5CYII=>