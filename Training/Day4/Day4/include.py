# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_path= "dbfs:/mnt/shivamdatabricks/raw/project/"

# COMMAND ----------

def add_ingestion_col(df):
  df_final=df.withColumn("ingestion_date",current_timestamp())
  df_final2=df_final.withColumn("source_path",input_file_name())
  return df_final2

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists bronze;
# MAGIC create schema if not exists silver;
# MAGIC create schema if not exists gold;

# COMMAND ----------

dbutils.widgets.text("table","")
source_file_name=dbutils.widgets.get("table")
