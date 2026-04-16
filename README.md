# On-Prem to Cloud Data Pipeline: Medallion Architecture

## 🏗️ Architecture
![Project Architecture](e2e%20data%20architecture%20diagram.png)

## 📝 Project Overview
An end-to-end hybrid data pipeline migrating on-premise SQL Server data to an **Azure Lakehouse** using the Medallion Architecture. This project demonstrates secure data ingestion, automated cleaning, and business-level analytics.

### 🛠️ Tech Stack
*   **Orchestration:** Azure Data Factory (ADF)
*   **Connectivity:** Self-Hosted Integration Runtime (SHIR)
*   **Compute:** Azure Databricks (PySpark & SQL)
*   **Storage:** Azure Data Lake Storage (ADLS Gen2) & Delta Lake

### 🧊 Medallion Layers
1.  **Bronze:** Raw data ingestion from on-premise sources using ADF.
2.  **Silver:** Data cleaning, deduplication (Window Functions), and schema enforcement in Databricks.
3.  **Gold:** Business-level aggregates and KPIs ready for stakeholder reporting.

---

## 📊 Gold Layer: Business Analytics
The final stage transforms processed data into actionable insights for decision-makers.

![Gold Dashboard](Sales%20Analytics-Gold%20Layer.png)

### 📈 Key Insights & Business Value:
*   **Revenue Monitoring:** Real-time visibility into the **$525.1M** total revenue.
*   **Sales Distribution:** Visual breakdown of sales by product category and time range.
*   **Automation:** Replaced manual reporting with a scalable, automated cloud solution.

---

## 📂 Repository Structure
*   `01_load_sql_to_bronze.py`: Ingestion logic from SQL sources.
*   `02_data_quality_tests.py`: Validation and data integrity scripts.
*   `Create Silver Table.sql`: Transformation and deduplication logic.
*   `Sales Analytics-Gold Layer.pdf`: Full exported analytics report.
