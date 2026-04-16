# On-Prem to Cloud Data Pipeline

## 🏗️ Architecture
![Project Architecture](e2e%20data%20architecture%20diagram.png)

## 📝 Project Overview
An end-to-end hybrid data pipeline migrating on-premise data to an **Azure Lakehouse** using the Medallion Architecture.

### 🛠️ Tech Stack
*   **Orchestration:** Azure Data Factory (ADF)
*   **Connectivity:** Self-Hosted Integration Runtime (SHIR)
*   **Compute:** Azure Databricks (PySpark)
*   **Storage:** Azure Data Lake Storage (ADLS Gen2) & Delta Lake

### 🧊 Medallion Layers
1.  **Bronze:** Raw data ingestion from on-prem SQL sources.
2.  **Silver:** Data cleaning, deduplication, and schema enforcement.
3.  **Gold:** Business-level aggregates ready for Power BI/Reporting.
