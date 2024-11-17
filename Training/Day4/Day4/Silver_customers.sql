-- Databricks notebook source
CREATE TABLE if not exists sbp_databricks.silver.customers (
  customer_id INT,
  customer_name STRING,
  customer_email STRING,
  customer_city STRING,
  customer_state STRING,
  operation STRING,
  sequenceNum INT,
  ingestion_date TIMESTAMP,
  source_path STRING,
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  is_current BOOLEAN
)
USING delta;

-- COMMAND ----------

WITH source_cte AS (
  SELECT
    customer_id,
    customer_name,
    customer_email,
    customer_city,
    customer_state,
    operation,
    sequenceNum,
    ingestion_date,
    source_path,
    current_timestamp() AS start_date,
    NULL AS end_date,
    TRUE AS is_current,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id 
      ORDER BY sequenceNum DESC, ingestion_date DESC
    ) AS rn
  FROM sbp_databricks.bronze.customers
)
MERGE INTO sbp_databricks.silver.customers AS target
USING (
  SELECT *
  FROM source_cte
  WHERE rn = 1
) AS source
ON target.customer_id = source.customer_id AND target.is_current = TRUE
WHEN MATCHED AND source.operation = 'UPDATE' THEN
  UPDATE SET
    target.end_date = current_timestamp(),
    target.is_current = FALSE

WHEN MATCHED and source.operation='DELETE' THEN
 UPDATE SET
    target.end_date = current_timestamp(),
    target.is_current = FALSE
WHEN NOT MATCHED THEN
  INSERT (
    customer_id,
    customer_name,
    customer_email,
    customer_city,
    customer_state,
    operation,
    sequenceNum,
    ingestion_date,
    source_path,
    start_date,
    end_date,
    is_current
  )
  VALUES (
    source.customer_id,
    source.customer_name,
    source.customer_email,
    source.customer_city,
    source.customer_state,
    source.operation,
    source.sequenceNum,
    source.ingestion_date,
    source.source_path,
    source.start_date,
    source.end_date,
    source.is_current
  );

-- COMMAND ----------

create or replace view sbp_databricks.silver.customer_current as 
select customer_id,customer_name, customer_email,customer_city, customer_state from sbp_databricks.silver.customers where is_current=true

-- COMMAND ----------

create or replace table sbp_databricks.silver.sales_customer
(select s.customer_id,s.product_id,s.quantity,s.discount_amount, s.total_amount,c.customer_name,c.customer_city,c.customer_state
from sbp_databricks.silver.sales s
inner join sbp_databricks.silver.customer_current c
on s.customer_id=c.customer_id)

