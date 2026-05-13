# SQL Server to Azure Cloud Data Platform — Medallion Architecture

> **End-to-end production-grade data pipeline** migrating on-premise SQL Server data to an Azure Lakehouse with Microsoft Purview governance, Microsoft Fabric integration, automated data quality validation and CI/CD.

---

## 🏗️ Architecture Overview

![Project Architecture](e2e%20data%20architecture%20diagram.png)

---

## 📝 Project Summary

This project demonstrates a complete **on-premises to cloud data platform migration**, covering every layer from raw SQL Server ingestion through to governed, business-ready Gold data — with Microsoft Purview providing end-to-end cataloguing and lineage, and Microsoft Fabric enabling cross-platform consumption.

Built to production standards used in regulated financial services environments, including automated reconciliation, data quality frameworks, governance metadata and CI/CD pipeline automation.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Source | SQL Server (on-premises via SOMA\smedi) |
| Orchestration | Azure Data Factory (ADF) |
| Connectivity | Self-Hosted Integration Runtime (SHIR) |
| Storage | Azure Data Lake Storage Gen2 (ADLS Gen2) |
| Compute | Azure Databricks (PySpark) |
| Table Format | Delta Lake |
| Transformation | dbt (Gold layer SQL models) |
| Data Quality | pytest / PySpark validation framework |
| Governance | Microsoft Purview (Data Map, Lineage, Glossary) |
| Consumption | Microsoft Fabric Lakehouse (via ADLS Gen2 shortcut) |
| CI/CD | GitHub Actions |

---

## 🧊 Medallion Architecture Layers

### 🥉 Bronze — Raw Ingestion
- ADF pipeline ingests raw SQL Server tables via SHIR into ADLS Gen2 Bronze container
- Parameterised, metadata-driven ForEach and Lookup patterns for scalable multi-table ingestion
- No transformation applied — full fidelity source data preserved

### 🥈 Silver — Cleansing and Transformation
- Databricks PySpark notebooks apply schema enforcement, deduplication and data type standardisation
- Window functions handle SCD logic and duplicate detection
- ZORDER, VACUUM and OPTIMIZE applied for Delta Lake performance
- Audit columns added (load timestamp, source system, record hash)

### 🥇 Gold — Business Analytics
- dbt SQL models build aggregated, business-ready datasets in the Gold layer
- Modular, documented and reusable transformation logic
- KPIs and analytics dimensions ready for reporting and consumption

![Gold Layer Dashboard](Sales%20Analytics-Gold%20Layer.png)

---

## ✅ Data Quality Framework

Automated validation runs at each layer transition using a **pytest / PySpark reconciliation framework**:

- Row count reconciliation between source and target
- Schema validation — column names, data types, nullability
- Null checks on mandatory fields
- Referential integrity checks across related tables
- Results logged and surfaced as pass / fail per pipeline run

```python
# Example: Row count reconciliation check
def test_row_count_bronze_vs_source(spark, source_conn, table_name):
    source_count = get_source_count(source_conn, table_name)
    bronze_count = spark.read.format("delta").load(f"{bronze_path}/{table_name}").count()
    assert source_count == bronze_count, f"Row count mismatch: source={source_count}, bronze={bronze_count}"
```

---

## 🔍 Microsoft Purview — Data Governance

Microsoft Purview is configured as the central **governance and discovery layer** across the full Medallion architecture.

### What is configured:
- **Data Map** — all ADLS Gen2 containers (Bronze, Silver, Gold) registered and scanned
- **Automated Lineage** — end-to-end lineage captured from SQL Server source through ADF pipelines to Databricks transformations across all three layers
- **Business Glossary** — governance terms defined and linked to scanned data assets
- **Metadata Classification** — classification rules applied to identify and tag sensitive data fields
- **Collections** — assets organised into logical domain collections aligned to business function
- **RBAC** — role-based access controls configured to restrict data access by layer and team

### Lineage view covers:
```
SQL Server (source) 
  → ADF Pipeline (ingestion) 
    → ADLS Gen2 Bronze 
      → Databricks Notebook (cleansing) 
        → ADLS Gen2 Silver 
          → Databricks Notebook (aggregation) 
            → ADLS Gen2 Gold 
              → Microsoft Fabric Lakehouse
```

---

## 🪡 Microsoft Fabric Integration

The Gold layer Delta tables are accessible directly within **Microsoft Fabric Lakehouse** via an ADLS Gen2 SAS shortcut — enabling consumption without data duplication or movement.

### Integration approach:
- SAS token generated on ADLS Gen2 storage account with Service, Container and Object permissions (Read + List)
- Shortcut configured in Fabric Lakehouse pointing to Gold container via `dfs.core.windows.net` endpoint
- Cross-tenant connectivity confirmed — Fabric workspace and ADLS Gen2 on separate Azure tenants
- Gold Delta tables queryable directly within Fabric using Spark notebooks or SQL analytics endpoint

### Why this matters:
This pattern allows a single governed Gold layer to serve both Databricks-based workloads and Fabric-based analytics teams — no duplication, single source of truth, unified lineage tracked in Purview.

---

## ⚙️ CI/CD — GitHub Actions

Automated test execution on every push to main:

```yaml
# .github/workflows/data_quality.yml
on:
  push:
    branches: [ main ]

jobs:
  run-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run data quality tests
        run: pytest tests/ -v --tb=short
```

- Tests run automatically on every commit
- Pipeline blocked if validation checks fail
- Results visible in GitHub Actions tab

---

## 📁 Repository Structure

```
on-prem-to-cloud-data-pipeline/
│
├── 01_load_sql_to_bronze.py          # ADF + SHIR ingestion from SQL Server to Bronze
├── 02_silver_transform.sql           # PySpark / SQL cleansing and deduplication
├── 03_medallion_validation.py        # pytest data quality and reconciliation framework
├── 04_medallion_architecture_diagram.png  # Full architecture diagram
├── 05_sales_analytics_gold_layer.png      # Gold layer dashboard screenshot
├── 06_sales_analytics_gold_layer.json     # Exported analytics report data
├── .github/
│   └── workflows/
│       └── data_quality.yml          # CI/CD — automated test execution on push
└── README.md
```

---

## 🏆 Certifications

| Certification | Issuer | Verification |
|---|---|---|
| **Microsoft Applied Skills: Govern data with Microsoft Purview** (In Progress) | Microsoft | — |
| **DP-700: Microsoft Fabric Data Engineer** (In Progress) | Microsoft | — |
| **Generative AI Fundamentals** | Databricks Academy | [Verify](https://credentials.databricks.com) |
| **dbt Fundamentals** | dbt Labs | [Verify](https://credentials.getdbt.com/f660f49d-9069-43cc-9517-271e4308070d#acc.aioOebYA) |
| **Databricks Fundamentals** | Databricks | [Verify](https://credentials.databricks.com/db325e6a-bde3-479a-a51f-593fbfd1d36f#acc.W0zRQ8Lt) |
| **PRINCE2 Foundation** | APMG International | — |

---

## 🔗 Connect

- **LinkedIn:** [linkedin.com/in/somasekhar-medisetti-72123318](https://www.linkedin.com/in/somasekhar-medisetti-72123318/)
- **GitHub:** [github.com/somagithub](https://github.com/somagithub)

---

## Topics

`azure-data-factory` `databricks` `pyspark` `delta-lake` `medallion-architecture` `microsoft-purview` `microsoft-fabric` `data-governance` `dbt` `pytest` `github-actions` `data-quality` `adls-gen2` `sql-server` `data-migration`
