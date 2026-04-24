# Reconciliation Strategy

## Overview

This document defines the reconciliation approach used across the On-Premises to Azure Cloud Data Migration Pipeline. It mirrors the reconciliation frameworks applied across enterprise data migration programmes in insurance and financial services.

The purpose of reconciliation is to provide confidence that data migrated from source systems is complete, accurate and consistent at every stage of the medallion pipeline - Bronze, Silver and Gold - before business sign-off and cut-over.

---

## Reconciliation Principles

- **Every layer is reconciled independently** - Bronze against source, Silver against Bronze, Gold against Silver
- **No data is promoted to the next layer until reconciliation passes** - this is the gate control
- **All reconciliation results are logged** - providing an audit trail for stakeholder sign-off
- **Failures are treated as blockers** - not warnings - until root cause is identified and resolved

---

## Reconciliation Approach by Layer

### Bronze Layer - Source vs Bronze

The Bronze layer preserves raw source data with no transformation. Reconciliation at this stage confirms that ingestion via ADF and SHIR was complete and no records were lost in transit.

| Check | Description | Pass Criteria |
|---|---|---|
| Row count | Total records in source SQL Server table vs Bronze Delta table | Counts must match exactly |
| Column count | Number of columns in source vs Bronze | Must match exactly |
| Null check | Null values in key columns (e.g. ID, date fields) | No unexpected nulls introduced during ingestion |
| Duplicate check | Duplicate primary key values in Bronze | Zero duplicates unless present in source |
| Load timestamp | All records carry a load timestamp | 100% populated |

**Tool:** `03_medallion_validation.py` - automated Python script executing all Bronze checks and logging results.

---

### Silver Layer - Bronze vs Silver

The Silver layer applies data cleansing, deduplication and schema enforcement. Reconciliation confirms that transformation rules have been applied correctly and no valid records have been dropped.

| Check | Description | Pass Criteria |
|---|---|---|
| Row count | Bronze row count vs Silver row count post-deduplication | Silver count ≤ Bronze count; variance explained by deduplication rules |
| Deduplication | Records removed by Window Function deduplication logic | All removed records logged with reason |
| Null enforcement | Null values in mandatory fields after cleansing | Zero nulls in mandatory columns |
| Referential integrity | Foreign key relationships between entities (e.g. order → customer) | Zero orphaned records |
| Schema enforcement | Column data types match defined Silver schema | 100% schema compliance |
| Business rule validation | Transformation rules applied correctly (e.g. date formatting, field derivations) | Spot-check sample of 10% of records |

**Tool:** `03_medallion_validation.py` - Silver validation section with logged exception report.

---

### Gold Layer - Silver vs Gold

The Gold layer produces aggregated, business-ready datasets. Reconciliation confirms that aggregations are mathematically correct and totals reconcile back to Silver-level detail.

| Check | Description | Pass Criteria |
|---|---|---|
| Aggregate reconciliation | SUM of revenue in Gold vs SUM of revenue in Silver | Values must match to 2 decimal places |
| Record count | Gold summary records map correctly to Silver source records | No missing categories or time periods |
| KPI validation | Key metrics (e.g. total revenue, product category totals) cross-checked manually | Spot-check 5 KPIs against Silver source |
| Completeness | All expected product categories and date ranges present in Gold | Zero missing segments |

**Tool:** Manual SQL validation queries against Gold Delta tables + visual check against Power BI dashboard.

---

## Exception Handling

When a reconciliation check fails, the following process applies:

1. **Log the failure** - record the check name, layer, table, expected value, actual value and timestamp
2. **Halt promotion** - do not promote data to the next layer until the failure is resolved
3. **Root cause analysis** - identify whether the issue is in source data, pipeline logic or transformation rules
4. **Fix and re-run** - apply the fix and re-execute the full reconciliation check for that layer
5. **Sign-off** - once all checks pass, record sign-off confirmation before promoting to next layer

---

## Reconciliation Sign-off Checklist

Before each layer promotion, the following must be confirmed:

- [ ] Row count check passed
- [ ] Null check passed
- [ ] Duplicate check passed
- [ ] Referential integrity check passed
- [ ] Schema enforcement passed
- [ ] Exception log reviewed and cleared
- [ ] Sign-off recorded with date and approver

---

## Lessons from Enterprise Programmes

This reconciliation framework reflects patterns applied across real-world migration programmes in insurance and financial services, including:

- **QBE Insurance** - claims data migration from Guidewire, Acturis, IRIS and Genius to Azure DataHub; automated reconciliation reduced manual validation effort by 60%
- **Direct Line Group** - Motor Claims Hub migration across four brands; row-count and SLA reconciliation across Salesforce, Acturis and Guidewire sources
- **MS Amlin** - CAT Exposure data migration; SQL-based financial reconciliation for actuarial sign-off aligned to Lloyd's CROF framework
- **Markerstudy** - Policy and Claims consolidation migration; source profiling and reconciliation prior to cut-over sign-off

---

## Related Documents

- [Source-to-Target Mapping](source-to-target-mapping.md)
- [README - Project Overview](../README.md)
- [Medallion Validation Script](../03_medallion_validation.py)
