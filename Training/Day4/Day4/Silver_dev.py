# Databricks notebook source
# MAGIC %run /Workspace/Users/naval.datamaster@gmail.com/Michelin/Day4/include

# COMMAND ----------

# DBTITLE 1,PySpark
df=spark.table("michelin.bronze.sales")
df1=df.dropDuplicates().dropna()
df1.write.mode("overwrite").saveAsTable("michelin.silver.sales")

# COMMAND ----------

# DBTITLE 1,Spark SQL
# MAGIC %sql
# MAGIC -- create or replace table michelin.silver.sales as (select distinct order_id, customer_id,transaction_id,product_id,quantity,discount_amount,total_amount,order_date from michelin.bronze.sales where order_id is not null)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from michelin.bronze.products

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view products_bronze as (select * from michelin.bronze.products)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO michelin.silver.products sp
# MAGIC USING products_bronze bp
# MAGIC ON sp.product_id = bp.product_id 
# MAGIC WHEN MATCHED AND operation ='UPDATE'
# MAGIC THEN
# MAGIC   UPDATE SET
# MAGIC     product_name=bp.product_name,
# MAGIC     product_category=bp.product_category,
# MAGIC     product_price=bp.product_price
# MAGIC WHEN MATCHED AND operation ='DELETE'
# MAGIC THEN
# MAGIC DELETE  
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     product_category,
# MAGIC     product_price
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     product_category,
# MAGIC     product_price
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deduplicated_bp AS (
# MAGIC   SELECT
# MAGIC     bp.*,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY seqNum
# MAGIC DESC) AS row_num
# MAGIC   FROM
# MAGIC     products_bronze bp
# MAGIC )
# MAGIC MERGE INTO michelin.silver.products sp
# MAGIC USING (
# MAGIC   SELECT * FROM deduplicated_bp WHERE row_num = 1
# MAGIC ) bp
# MAGIC ON sp.product_id = bp.product_id 
# MAGIC WHEN MATCHED AND bp.operation = 'UPDATE'
# MAGIC THEN
# MAGIC   UPDATE SET
# MAGIC     product_name = bp.product_name,
# MAGIC     product_category = bp.product_category,
# MAGIC     product_price = bp.product_price
# MAGIC WHEN MATCHED AND bp.operation = 'DELETE'
# MAGIC THEN
# MAGIC   DELETE  
# MAGIC WHEN NOT MATCHED
# MAGIC THEN
# MAGIC   INSERT (
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     product_category,
# MAGIC     product_price
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     bp.product_id,
# MAGIC     bp.product_name,
# MAGIC     bp.product_category,
# MAGIC     bp.product_price
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table michelin.bronze.products

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE michelin.silver.products (
# MAGIC   product_id INT,
# MAGIC   product_name STRING,
# MAGIC   product_category STRING,
# MAGIC   product_price DOUBLE
# MAGIC   )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from michelin.bronze.products

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct * from michelin.silver.products
