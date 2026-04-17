# Databricks notebook source
# DBTITLE 1,Source to Gold DQ Tests
# MAGIC %md
# MAGIC # Source to Gold Data Quality Tests (Great Expectations)
# MAGIC
# MAGIC Automated DQ validation across the **medallion architecture** using Great Expectations v1 with an EphemeralDataContext and Spark datasource.
# MAGIC
# MAGIC | Layer | Table | Key Checks |
# MAGIC |-------|-------|------------|
# MAGIC | Bronze | `dbx_sql_to_bronze.bronze.dbo_Sales_Source_parquet` | Nulls, ranges, schema |
# MAGIC | Silver | `dbx_sql_to_bronze.silver.Sales_Source` | Uniqueness, null filters, row count |
# MAGIC | Gold | `sales_by_category`, `daily_sales_summary`, `customer_summary` | Aggregation integrity |
# MAGIC | Cross-Layer | All | Row count consistency |

# COMMAND ----------

# DBTITLE 1,Install Great Expectations
# MAGIC %pip install great_expectations

# COMMAND ----------

# DBTITLE 1,Imports, GX Context, Load Tables & Helpers
import great_expectations as gx
import great_expectations.expectations as gxe

# ── Ephemeral context (in-memory, no persistent store) ──
context = gx.get_context(mode="ephemeral")

# NOTE: Using Pandas datasource because GX's Spark engine calls PERSIST TABLE
# which is not supported on Databricks serverless compute.
datasource = context.data_sources.add_pandas(name="pandas_ds")

# ── Load all medallion tables via Spark ──
bronze_df = spark.table("dbx_sql_to_bronze.bronze.dbo_Sales_Source_parquet")
silver_df = spark.table("dbx_sql_to_bronze.silver.Sales_Source")
gold_category_df = spark.table("dbx_sql_to_bronze.gold.sales_by_category")
gold_daily_df = spark.table("dbx_sql_to_bronze.gold.daily_sales_summary")
gold_customer_df = spark.table("dbx_sql_to_bronze.gold.customer_summary")

# Cache row counts for cross-layer checks
bronze_count = bronze_df.count()
silver_count = silver_df.count()

print(f"Bronze rows  : {bronze_count:,}")
print(f"Silver rows  : {silver_count:,}")
print(f"Gold Category: {gold_category_df.count():,}")
print(f"Gold Daily   : {gold_daily_df.count():,}")
print(f"Gold Customer: {gold_customer_df.count():,}")

# ── Convert to Pandas for GX validation ──
bronze_pdf = bronze_df.toPandas()
silver_pdf = silver_df.toPandas()
gold_category_pdf = gold_category_df.toPandas()
gold_daily_pdf = gold_daily_df.toPandas()
gold_customer_pdf = gold_customer_df.toPandas()

# ── Helper: create a GX batch from a Pandas DataFrame ──
_ctr = 0
def make_batch(pdf, label):
    global _ctr
    _ctr += 1
    tag = f"{label}_{_ctr}"
    asset = datasource.add_dataframe_asset(name=f"{tag}_asset")
    bd = asset.add_batch_definition_whole_dataframe(f"{tag}_bd")
    return bd.get_batch(batch_parameters={"dataframe": pdf})

# ── Helper: validate & print PASS/FAIL per expectation ──
all_results = []  # collects every check for the final summary

def run_checks(pdf, layer_label, expectations):
    """Validate a list of GX expectations against a Pandas DataFrame and print results."""
    batch = make_batch(pdf, layer_label.replace(" ", "_").replace("—", ""))
    suite = gx.ExpectationSuite(name=f"suite_{_ctr}")
    suite = context.suites.add(suite)
    for e in expectations:
        suite.add_expectation(e)
    validation = batch.validate(suite)

    print(f"\n{'='*70}")
    print(f"  📋 {layer_label}")
    print(f"{'='*70}")

    passed = failed = 0
    for r in validation.results:
        ok = r.success
        tag = "✅ PASS" if ok else "❌ FAIL"
        exp = r.expectation_config
        # Extract expectation type (GX v1 stores it in .type or .kwargs)
        etype = (getattr(exp, 'type', None)
                 or getattr(exp, 'expectation_type', None)
                 or type(exp).__name__)
        # Extract readable parameters from kwargs dict
        kwargs = getattr(exp, 'kwargs', {})
        display_kwargs = {k: v for k, v in kwargs.items()
                         if k not in ('batch_id', 'result_format')}
        detail = ", ".join(f"{k}={v}" for k, v in display_kwargs.items())
        print(f"  {tag}  {etype}  [{detail}]")
        passed += ok
        failed += (not ok)
        all_results.append(dict(layer=layer_label, test=etype, detail=detail, passed=ok))

    print(f"\n  ➜ {passed} passed, {failed} failed / {passed + failed} total")
    return validation

print("\n✅ GX context, Pandas datasource, and helpers ready.")

# COMMAND ----------

# DBTITLE 1,Bronze DQ Checks
# ── BRONZE Layer DQ ──────────────────────────────────────────────────
# Note: _rescued_data column is auto-added by read_files() during bronze ingestion
bronze_expectations = [
    gxe.ExpectTableRowCountToBeBetween(min_value=1),
    gxe.ExpectColumnValuesToNotBeNull(column="OrderID"),
    gxe.ExpectColumnValuesToNotBeNull(column="Category"),
    gxe.ExpectColumnValuesToNotBeNull(column="OrderDate"),
    gxe.ExpectColumnValuesToBeBetween(column="Amount", min_value=0, max_value=110000),
    gxe.ExpectTableColumnsToMatchSet(
        column_set=["OrderID", "Product", "Category", "Amount", "OrderDate", "CustomerID", "_rescued_data"],
        exact_match=False,
    ),
]

run_checks(bronze_pdf, "Bronze — dbo_Sales_Source_parquet", bronze_expectations)

# COMMAND ----------

# DBTITLE 1,Silver DQ Checks
# ── SILVER Layer DQ ─────────────────────────────────────────────────
silver_expectations = [
    gxe.ExpectTableRowCountToBeBetween(min_value=1),
    gxe.ExpectColumnValuesToNotBeNull(column="OrderID"),
    gxe.ExpectColumnValuesToBeUnique(column="OrderID"),
    gxe.ExpectColumnValuesToBeBetween(column="Amount", min_value=0, strict_min=True),
    gxe.ExpectColumnValuesToNotBeNull(column="ingested_at"),
    gxe.ExpectTableRowCountToBeBetween(min_value=1, max_value=bronze_count),
    gxe.ExpectTableColumnsToMatchSet(
        column_set=["OrderID", "Product", "Category", "Amount",
                    "OrderDate", "CustomerID", "ingested_at"],
        exact_match=False,
    ),
]

run_checks(silver_pdf, "Silver — Sales_Source", silver_expectations)

# COMMAND ----------

# DBTITLE 1,Gold DQ Checks
# ── GOLD Layer DQ ───────────────────────────────────────────────────

# -- sales_by_category --
run_checks(gold_category_pdf, "Gold — sales_by_category", [
    gxe.ExpectColumnValuesToNotBeNull(column="Category"),
    gxe.ExpectColumnValuesToBeUnique(column="Category"),
    gxe.ExpectColumnValuesToBeBetween(column="total_orders", min_value=1),
    gxe.ExpectColumnValuesToBeBetween(column="total_revenue", min_value=0, strict_min=True),
])

# -- daily_sales_summary --
run_checks(gold_daily_pdf, "Gold — daily_sales_summary", [
    gxe.ExpectColumnValuesToNotBeNull(column="order_date"),
    gxe.ExpectColumnValuesToBeBetween(column="total_orders", min_value=1),
    gxe.ExpectColumnValuesToBeBetween(column="total_revenue", min_value=0, strict_min=True),
])

# -- customer_summary --
run_checks(gold_customer_pdf, "Gold — customer_summary", [
    gxe.ExpectColumnValuesToNotBeNull(column="CustomerID"),
    gxe.ExpectColumnValuesToBeUnique(column="CustomerID"),
    gxe.ExpectColumnValuesToBeBetween(column="lifetime_value", min_value=0, strict_min=True),
])

# COMMAND ----------

# DBTITLE 1,Cross-Layer Consistency & Final Summary
# ── CROSS-LAYER CONSISTENCY CHECKS ───────────────────────────────────
from pyspark.sql import functions as F

print("="*70)
print("  🔗 Cross-Layer Consistency Checks")
print("="*70)

def cross_check(description, condition):
    tag = "✅ PASS" if condition else "❌ FAIL"
    print(f"  {tag}  {description}")
    all_results.append(dict(layer="Cross-Layer", test=description, detail="", passed=condition))

# 1. Silver row count <= Bronze row count
cross_check(
    f"Silver count ({silver_count:,}) ≤ Bronze count ({bronze_count:,})",
    silver_count <= bronze_count
)

# 2. SUM(sales_by_category.total_orders) == Silver count
cat_total = int(gold_category_pdf["total_orders"].sum())
cross_check(
    f"SUM(sales_by_category.total_orders) ({cat_total:,}) == Silver count ({silver_count:,})",
    cat_total == silver_count
)

# 3. SUM(customer_summary.total_orders) == Silver count
cust_total = int(gold_customer_pdf["total_orders"].sum())
cross_check(
    f"SUM(customer_summary.total_orders) ({cust_total:,}) == Silver count ({silver_count:,})",
    cust_total == silver_count
)

# ── FINAL SUMMARY TABLE ──────────────────────────────────────────────
from collections import defaultdict

print(f"\n{'='*70}")
print("  📊 FINAL DQ SUMMARY — Source to Gold")
print(f"{'='*70}\n")

summary = defaultdict(lambda: {"passed": 0, "failed": 0})
for r in all_results:
    key = r["layer"].split(" — ")[0]
    summary[key]["passed" if r["passed"] else "failed"] += 1

print(f"  {'Layer':<20} {'Passed':>8} {'Failed':>8} {'Total':>8} {'Status':>14}")
print(f"  {'-'*20} {'-'*8} {'-'*8} {'-'*8} {'-'*14}")

overall_ok = True
for layer in ["Bronze", "Silver", "Gold", "Cross-Layer"]:
    if layer in summary:
        p, f = summary[layer]["passed"], summary[layer]["failed"]
        status = "✅ PASS" if f == 0 else "❌ FAIL"
        if f > 0:
            overall_ok = False
        print(f"  {layer:<20} {p:>8} {f:>8} {p+f:>8} {status:>14}")

tp = sum(s["passed"] for s in summary.values())
tf = sum(s["failed"] for s in summary.values())
verdict = "✅ ALL PASSED" if overall_ok else "❌ FAILURES DETECTED"
print(f"  {'-'*20} {'-'*8} {'-'*8} {'-'*8} {'-'*14}")
print(f"  {'OVERALL':<20} {tp:>8} {tf:>8} {tp+tf:>8} {verdict:>14}")
print(f"\n{'='*70}")
