# Databricks notebook source
df=spark.read.csv("dbfs:/mnt/sanly/raw/jsoninputfiles/races.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.parquet("dbfs:/mnt/sanly/raw/proceeddata/Saranya/races")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/sanly/raw/proceeddata/

# COMMAND ----------


