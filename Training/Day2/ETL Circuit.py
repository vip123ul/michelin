# Databricks notebook source
# MAGIC %md
# MAGIC  ########Data object  

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema michelin

# COMMAND ----------

df=spark.read.csv("/Volumes/sbp_databricks/michelin/raw/circuits(in).csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *
df.select(col("circuitId").alias("circuit_id"),"circuitRef").display()

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ####

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Task:
# MAGIC 1. Rename circuitId to circuit_id, circuitRef ----- circuit_ref, lat --- latitude, lng ---- longitude
# MAGIC 2. get new column. colname ingestion_date which should contain current_timestamp
# MAGIC 3. drop url col

# COMMAND ----------

df1=df.withColumnsRenamed({"circuitId":"circuit_id","circuitRef":"circuit_ref","lat":"latitude","lng":"longitude"})
df1=df1.withColumn("ingestion_date",current_date())
df1=df1.drop("url")

df1.display()

# COMMAND ----------

df1.write.saveAsTable("michelin.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from michelin.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC create table michelin.constructor_sql as
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/sbp_databricks/michelin/raw/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from michelin.constructor_sql

# COMMAND ----------


