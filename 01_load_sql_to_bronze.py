# Databricks notebook source
# DBTITLE 1,Read file
# 1. Get the table name from ADF (parameterized)
dbutils.widgets.text("table_name", "")
table_name = dbutils.widgets.get("table_name")

# 2. Define the paths (Based on your Azure Portal image)
storage_account = "deltastorage2602" 
container = "bronze"

source_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{table_name}"
target_table = f"dbx_sql_to_bronze.bronze.{table_name.replace('.', '_')}"

print(f"Source: {source_path}")
print(f"Target: {target_table}")

# 3. Read Parquet files using read_files()
df = spark.sql(f"SELECT * FROM read_files('{source_path}', format => 'parquet')")

# 4. Preview the data first
print(f"Row count: {df.count()}")
display(df.limit(10))

# COMMAND ----------

# DBTITLE 1,Write to Bronze Table
# Write as an external table in the ADLS location (workaround for managed storage issue)
table_location = f"abfss://{container}@{storage_account}.dfs.core.windows.net/delta_tables/{table_name.replace('.', '_')}"

spark.sql(f"""
    CREATE OR REPLACE TABLE {target_table}
    LOCATION '{table_location}'
    AS SELECT * FROM read_files('{source_path}', format => 'parquet')
""")

print(f"Successfully loaded data into {target_table}")
print(f"Table location: {table_location}")

# COMMAND ----------

# DBTITLE 1,Create Silver Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dbx_sql_to_bronze.silver.Sales_Source
# MAGIC LOCATION 'abfss://bronze@deltastorage2602.dfs.core.windows.net/delta_tables/silver_Sales_Source'
# MAGIC AS
# MAGIC SELECT OrderID, Product, Category, Amount, OrderDate, CustomerID, current_timestamp() AS ingested_at
# MAGIC FROM dbx_sql_to_bronze.bronze.dbo_Sales_Source_parquet
# MAGIC WHERE OrderID IS NOT NULL
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY OrderID ORDER BY OrderDate DESC) = 1;

# COMMAND ----------

# DBTITLE 1,Create Gold Schema
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dbx_sql_to_bronze.gold
# MAGIC COMMENT 'Gold layer - business-level aggregations and KPIs';

# COMMAND ----------

# DBTITLE 1,Gold: Sales by Category
# MAGIC %sql
# MAGIC -- Revenue, order count, and average order value by category
# MAGIC CREATE OR REPLACE TABLE dbx_sql_to_bronze.gold.sales_by_category
# MAGIC LOCATION 'abfss://bronze@deltastorage2602.dfs.core.windows.net/delta_tables/gold_sales_by_category'
# MAGIC AS
# MAGIC SELECT
# MAGIC   Category,
# MAGIC   COUNT(*) AS total_orders,
# MAGIC   SUM(Amount) AS total_revenue,
# MAGIC   ROUND(AVG(Amount), 2) AS avg_order_value,
# MAGIC   MIN(Amount) AS min_order,
# MAGIC   MAX(Amount) AS max_order,
# MAGIC   COUNT(DISTINCT CustomerID) AS unique_customers,
# MAGIC   current_timestamp() AS refreshed_at
# MAGIC FROM dbx_sql_to_bronze.silver.Sales_Source
# MAGIC GROUP BY Category
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# DBTITLE 1,Gold: Daily Sales Summary
# MAGIC %sql
# MAGIC -- Daily revenue trends and order volume
# MAGIC CREATE OR REPLACE TABLE dbx_sql_to_bronze.gold.daily_sales_summary
# MAGIC LOCATION 'abfss://bronze@deltastorage2602.dfs.core.windows.net/delta_tables/gold_daily_sales_summary'
# MAGIC AS
# MAGIC SELECT
# MAGIC   CAST(OrderDate AS DATE) AS order_date,
# MAGIC   COUNT(*) AS total_orders,
# MAGIC   SUM(Amount) AS total_revenue,
# MAGIC   ROUND(AVG(Amount), 2) AS avg_order_value,
# MAGIC   COUNT(DISTINCT CustomerID) AS unique_customers,
# MAGIC   COUNT(DISTINCT Category) AS categories_sold,
# MAGIC   current_timestamp() AS refreshed_at
# MAGIC FROM dbx_sql_to_bronze.silver.Sales_Source
# MAGIC GROUP BY CAST(OrderDate AS DATE)
# MAGIC ORDER BY order_date;

# COMMAND ----------

# DBTITLE 1,Gold: Customer Summary
# MAGIC %sql
# MAGIC -- Customer-level spend summary
# MAGIC CREATE OR REPLACE TABLE dbx_sql_to_bronze.gold.customer_summary
# MAGIC LOCATION 'abfss://bronze@deltastorage2602.dfs.core.windows.net/delta_tables/gold_customer_summary'
# MAGIC AS
# MAGIC SELECT
# MAGIC   CustomerID,
# MAGIC   COUNT(*) AS total_orders,
# MAGIC   SUM(Amount) AS lifetime_value,
# MAGIC   ROUND(AVG(Amount), 2) AS avg_order_value,
# MAGIC   MIN(OrderDate) AS first_order,
# MAGIC   MAX(OrderDate) AS last_order,
# MAGIC   COUNT(DISTINCT Category) AS categories_purchased,
# MAGIC   current_timestamp() AS refreshed_at
# MAGIC FROM dbx_sql_to_bronze.silver.Sales_Source
# MAGIC GROUP BY CustomerID
# MAGIC ORDER BY lifetime_value DESC;

# COMMAND ----------

# DBTITLE 1,Drop old Silver table from Bronze schema
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dbx_sql_to_bronze.bronze.silver_Sales_Source;
