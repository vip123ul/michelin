# Databricks notebook source
# MAGIC %sql
# MAGIC create table emp_test values
# MAGIC
# MAGIC drop table emp_test ;
# MAGIC DESCRIBE HISTORY emp_test

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp (id int, name string , age int);

# COMMAND ----------

# MAGIC %sql describe extended emp

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into emp values (1, "Shivam",28),(2,"Shyam",28);

# COMMAND ----------

# MAGIC %sql describe history emp

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into emp values (1, "Shivam",28),(2,"Shyam",28);
# MAGIC insert into emp values (3, "Shivam",28),(7,"Shyam",28);
# MAGIC insert into emp values (4, "Shivam",28),(8,"Shyam",28);
# MAGIC insert into emp values (5, "Shivam",28),(9,"Shyam",28);
# MAGIC insert into emp values (6, "Shivam",28),(10,"Shyam",28);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %sql describe extended emp

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE emp ZORDER BY (id)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC vacuum emp 

# COMMAND ----------

# MAGIC %sql 
# MAGIC create view  michelin.circuit_view as (
# MAGIC select circuit_ref , name from sbp_databricks.michelin.circuits)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view country_global_temp as
# MAGIC (select country, location, count (country) as count from sbp_databricks.michelin.circuits group by all order by
# MAGIC count desc)

# COMMAND ----------

# MAGIC %sql 
# MAGIC show views in global_temp
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create function michelin.voter_eligible(age int)
# MAGIC returns string
# MAGIC return case
# MAGIC when age > 18 then 'eligible'
# MAGIC else 'You are not eligible for voting'
# MAGIC end

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, michelin.voter_eligible(age) as eligibility from emp
