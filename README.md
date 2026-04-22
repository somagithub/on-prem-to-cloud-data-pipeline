# On-Prem to Cloud Data Pipeline: Medallion Architecture

## 🏗️ Architecture
![Project Architecture](e2e%20data%20architecture%20diagram.png)

## 📝 Project Overview
An end-to-end hybrid data pipeline migrating on-premise SQL Server data to an **Azure Lakehouse** using the Medallion Architecture. This project demonstrates secure data ingestion, automated cleaning, and business-level analytics.

### 💼 Business Context
Built to reinforce patterns applied across enterprise data migration programmes in insurance and financial services, including on-premises SQL Server to Azure cloud migrations in regulated environments. Demonstrates the full migration lifecycle: source profiling, transformation rules, data quality validation, and reconciliation — replicating real-world medallion architecture deployments.

### 🛠️ Tech Stack
*   **Orchestration:** Azure Data Factory (ADF)
*   **Connectivity:** Self-Hosted Integration Runtime (SHIR)
*   **Compute:** Azure Databricks (PySpark & SQL)
*   **Storage:** Azure Data Lake Storage (ADLS Gen2) & Delta Lake

### 🔄 Migration Approach
| Phase | Activity |
|---|---|
| **Source Profiling** | Row counts, null checks, duplicate analysis on SQL Server source |
| **Source-to-Target Mapping** | Column-level mapping with transformation and derivation rules |
| **Data Quality Rules** | Null, duplicate and referential integrity checks enforced at Silver layer |
| **Reconciliation** | Row count and checksum reconciliation between Bronze, Silver and Gold |
| **Cut-over Readiness** | Validation scripts confirming data completeness before Gold promotion |

### 🧊 Medallion Layers
1.  **Bronze:** Raw data ingestion from on-premise SQL Server sources using ADF via Self-Hosted Integration Runtime — preserving source fidelity with no transformation.
2.  **Silver:** Data cleaning, deduplication (Window Functions), schema enforcement and data quality rule application in Databricks — enforcing referential integrity and null constraints.
3.  **Gold:** Business-level aggregates and KPIs ready for stakeholder reporting — reconciled against Silver layer row counts for completeness assurance.

---

## 📊 Gold Layer: Business Analytics
The final stage transforms processed data into actionable insights for decision-makers.

![Gold Dashboard](Sales%20Analytics-Gold%20Layer.png)

### 📈 Key Insights & Business Value:
*   **Revenue Monitoring:** Real-time visibility into total revenue KPIs using aggregated sample data.
*   **Sales Distribution:** Visual breakdown of sales by product category and time range.
*   **Automation:** Replaced manual reporting with a scalable, automated cloud solution.

---

## 📁 Repository Structure
* [`01_load_sql_to_bronze.py`](01_load_sql_to_bronze.py): Ingestion logic from SQL Server source to Bronze layer.
* [`02_silver_transform.sql`](02_silver_transform.sql): Transformation and deduplication logic with schema enforcement.
* [`03_medallion_validation.py`](03_medallion_validation.py): Row count reconciliation, null checks, and duplicate integrity validation between pipeline layers.
* [`04_medallion_architecture_diagram.png`](04_medallion_architecture_diagram.png): Visual overview of the data pipeline.
* [`05_sales_analytics_gold_layer.png`](05_sales_analytics_gold_layer.png): Sales analytics visualization.
* [`06_sales_analytics_gold_layer.json`](06_sales_analytics_gold_layer.json): Full exported analytics report data.

---

## 🏆 Certifications & Badges
I hold the following industry-recognised certifications in the Modern Data Stack:

| Badge | Certification | Verification |
| :--- | :--- | :--- |
| ![dbt](https://shields.io) | **dbt Fundamentals** | [Verify Credential](https://credentials.getdbt.com/f660f49d-9069-43cc-9517-271e4308070d#acc.aioOebYA) |
| ![Databricks](https://shields.io) | **Databricks Fundamentals** | [Verify Credential](https://credentials.databricks.com/db325e6a-bde3-479a-a51f-593fbfd1d36f#acc.W0zRQ8Lt) |
