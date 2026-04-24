# Source-to-Target Mapping

## Overview

This document defines the source-to-target mapping for the On-Premises to Azure Cloud Data Migration Pipeline. It captures column-level mapping between the on-premises SQL Server source and the Azure Lakehouse target across Bronze, Silver and Gold medallion layers.

This mapping document mirrors the artefacts produced on enterprise data migration programmes in insurance and financial services - providing the definitive reference for transformation rules, data quality rules and validation criteria agreed with business stakeholders prior to migration execution.

---

## Scope

| Item | Detail |
|---|---|
| Source system | On-premises SQL Server (AdventureWorksLT sample database) |
| Target system | Azure Data Lake Storage Gen2 (Delta Lake - Medallion Architecture) |
| Layers | Bronze (raw), Silver (cleansed), Gold (aggregated) |
| Migration pattern | Full load - Bronze; Cleanse and deduplicate - Silver; Aggregate - Gold |
| Migration tool | Azure Data Factory (ADF) + Self-Hosted Integration Runtime (SHIR) |

---

## Entity Overview

| Source Table | Target Layer | Target Table / Path | Load Type |
|---|---|---|---|
| SalesLT.Customer | Bronze | bronze/customer | Full load |
| SalesLT.Product | Bronze | bronze/product | Full load |
| SalesLT.SalesOrderHeader | Bronze | bronze/sales_order_header | Full load |
| SalesLT.SalesOrderDetail | Bronze | bronze/sales_order_detail | Full load |
| bronze/customer (cleansed) | Silver | silver/customer | Full load |
| bronze/product (cleansed) | Silver | silver/product | Full load |
| bronze/sales_order_header (cleansed) | Silver | silver/sales_order | Full load |
| silver/sales_order + silver/product | Gold | gold/sales_analytics | Aggregated |

---

## Detailed Column Mapping

### SalesLT.Customer → Bronze → Silver

| Source Column | Source Type | Target Column (Bronze) | Target Column (Silver) | Transformation Rule | DQ Rule | Nullable |
|---|---|---|---|---|---|---|
| CustomerID | int | customer_id | customer_id | Direct copy | Must be unique, not null | No |
| FirstName | nvarchar(50) | first_name | first_name | Direct copy | Not null | No |
| LastName | nvarchar(50) | last_name | last_name | Direct copy | Not null | No |
| EmailAddress | nvarchar(50) | email_address | email_address | Lowercase | Valid email format | Yes |
| Phone | nvarchar(25) | phone | phone | Direct copy | None | Yes |
| CompanyName | nvarchar(128) | company_name | company_name | Direct copy | None | Yes |
| ModifiedDate | datetime | modified_date | modified_date | Direct copy | Not null | No |
| *(derived)* | - | load_timestamp | load_timestamp | Pipeline execution timestamp | Not null | No |
| *(derived)* | - | source_system | source_system | Hardcoded: 'SQL_ONPREM' | Not null | No |

---

### SalesLT.Product → Bronze → Silver

| Source Column | Source Type | Target Column (Bronze) | Target Column (Silver) | Transformation Rule | DQ Rule | Nullable |
|---|---|---|---|---|---|---|
| ProductID | int | product_id | product_id | Direct copy | Must be unique, not null | No |
| Name | nvarchar(50) | product_name | product_name | Direct copy | Not null | No |
| ProductNumber | nvarchar(25) | product_number | product_number | Uppercase | Not null | No |
| Color | nvarchar(15) | color | color | Direct copy | None | Yes |
| StandardCost | money | standard_cost | standard_cost | Direct copy | Must be ≥ 0 | No |
| ListPrice | money | list_price | list_price | Direct copy | Must be > 0 | No |
| Size | nvarchar(5) | size | size | Direct copy | None | Yes |
| Weight | decimal(8,2) | weight | weight | Direct copy | Must be > 0 if populated | Yes |
| ProductCategoryID | int | product_category_id | product_category_id | Direct copy | Referential integrity to category | Yes |
| ModifiedDate | datetime | modified_date | modified_date | Direct copy | Not null | No |
| *(derived)* | - | load_timestamp | load_timestamp | Pipeline execution timestamp | Not null | No |

---

### SalesLT.SalesOrderHeader → Bronze → Silver

| Source Column | Source Type | Target Column (Bronze) | Target Column (Silver) | Transformation Rule | DQ Rule | Nullable |
|---|---|---|---|---|---|---|
| SalesOrderID | int | sales_order_id | sales_order_id | Direct copy | Must be unique, not null | No |
| OrderDate | datetime | order_date | order_date | Cast to date | Not null | No |
| DueDate | datetime | due_date | due_date | Cast to date | Must be ≥ order_date | No |
| ShipDate | datetime | ship_date | ship_date | Cast to date | Must be ≥ order_date if populated | Yes |
| Status | tinyint | status | status_code | Direct copy | Value in (1,2,3,4,5,6) | No |
| CustomerID | int | customer_id | customer_id | Direct copy | Referential integrity to customer | No |
| SubTotal | money | sub_total | sub_total | Direct copy | Must be ≥ 0 | No |
| TaxAmt | money | tax_amount | tax_amount | Direct copy | Must be ≥ 0 | No |
| Freight | money | freight | freight | Direct copy | Must be ≥ 0 | No |
| TotalDue | money | total_due | total_due | Direct copy | Must equal sub_total + tax_amount + freight | No |
| *(derived)* | - | load_timestamp | load_timestamp | Pipeline execution timestamp | Not null | No |

---

### SalesLT.SalesOrderDetail → Bronze → Silver

| Source Column | Source Type | Target Column (Bronze) | Target Column (Silver) | Transformation Rule | DQ Rule | Nullable |
|---|---|---|---|---|---|---|
| SalesOrderID | int | sales_order_id | sales_order_id | Direct copy | Referential integrity to order header | No |
| SalesOrderDetailID | int | sales_order_detail_id | sales_order_detail_id | Direct copy | Must be unique, not null | No |
| OrderQty | smallint | order_qty | order_qty | Direct copy | Must be > 0 | No |
| ProductID | int | product_id | product_id | Direct copy | Referential integrity to product | No |
| UnitPrice | money | unit_price | unit_price | Direct copy | Must be > 0 | No |
| UnitPriceDiscount | money | unit_price_discount | unit_price_discount | Direct copy | Must be between 0 and 1 | No |
| LineTotal | money | line_total | line_total | Direct copy | Must equal order_qty × unit_price × (1 - discount) | No |
| *(derived)* | - | load_timestamp | load_timestamp | Pipeline execution timestamp | Not null | No |

---

## Gold Layer Aggregation Mapping

### Silver → Gold: sales_analytics

| Source (Silver) | Aggregation / Derivation | Target Column (Gold) | Business Definition |
|---|---|---|---|
| order_date (year) | YEAR(order_date) | order_year | Calendar year of sale |
| order_date (month) | MONTH(order_date) | order_month | Calendar month of sale |
| product_name | GROUP BY | product_name | Product sold |
| product_category_id | JOIN to category | category_name | Product category |
| order_qty | SUM | total_quantity_sold | Total units sold |
| unit_price | AVG | avg_unit_price | Average selling price |
| line_total | SUM | total_revenue | Total revenue for product/period |
| sales_order_id | COUNT DISTINCT | total_orders | Number of distinct orders |
| customer_id | COUNT DISTINCT | total_customers | Number of distinct customers |

---

## Data Quality Rules Summary

| Rule Type | Layer Applied | Action on Failure |
|---|---|---|
| Not null - mandatory fields | Bronze, Silver | Log and halt promotion |
| Uniqueness - primary keys | Bronze, Silver | Log and halt promotion |
| Referential integrity | Silver | Log orphaned records, halt promotion |
| Range checks (prices, quantities) | Silver | Log exceptions, investigate before promotion |
| Calculated field validation (TotalDue, LineTotal) | Silver | Log variance, investigate before promotion |
| Aggregate reconciliation | Gold | Log variance vs Silver totals, investigate |

---

## Assumptions and Decisions

| # | Assumption / Decision | Rationale |
|---|---|---|
| 1 | Full load pattern used for all Bronze tables | Source volume is small; incremental loading not required for this POC |
| 2 | Bronze layer preserves source data with no transformation | Maintains full auditability and rollback capability |
| 3 | Deduplication applied at Silver layer using Window Functions | Ensures cleansed layer is deduplicated without modifying Bronze |
| 4 | Derived columns (load_timestamp, source_system) added at Bronze | Supports data lineage tracking and audit requirements |
| 5 | Gold aggregations do not modify Silver records | Gold is a read-only analytical layer derived from Silver |
| 6 | All mapping agreed with business SMEs before migration execution | Prevents rework and ensures sign-off at each checkpoint |

---

## Sign-off

| Milestone | Confirmed By | Date |
|---|---|---|
| Source-to-target mapping agreed | Data Migration Lead | Apr 2026 |
| Bronze reconciliation passed | Data Migration Lead | Apr 2026 |
| Silver reconciliation passed | Data Migration Lead | Apr 2026 |
| Gold reconciliation passed | Data Migration Lead | Apr 2026 |

---

## Related Documents

- [Reconciliation Strategy](reconciliation-strategy.md)
- [README - Project Overview](../README.md)
- [Medallion Validation Script](../03_medallion_validation.py)
