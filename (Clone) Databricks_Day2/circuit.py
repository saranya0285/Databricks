# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True)

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

Renaming column and add new column as ingestion, drop url

# COMMAND ----------

from pyspark.sql.functions import *
df1=(df.withColumnRenamed("circuitId","circuit_id")
.withColumnRenamed("circuitRef","circuit_ref")
.withColumn("ingestion_Date",current_timestamp())
.drop("url"))

# COMMAND ----------

df1.write.saveAsTable("saranya.circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from saranya.circuit

# COMMAND ----------


