-- Silver Layer: Cleansed and transformed data from Bronze
CREATE OR REPLACE TABLE dbx_sql_to_bronze.silver.Sales_Source
LOCATION 'abfss://bronze@deltastorage2602.dfs.core.windows.net/delta_tables/silver_Sales_Source'
AS
SELECT
  OrderID,
  Product,
  Category,
  Amount,
  OrderDate,
  CustomerID,
  current_timestamp() AS ingested_at
FROM dbx_sql_to_bronze.bronze.dbo_Sales_Source_parquet
WHERE OrderID IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY OrderID ORDER BY OrderDate DESC) = 1;

-- Preview the silver table
SELECT * FROM dbx_sql_to_bronze.silver.Sales_Source;