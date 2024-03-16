# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1/

# COMMAND ----------

# MAGIC %sql
# MAGIC create table saranya.constructor1 as 
# MAGIC (select * from json.`dbfs:/FileStore/tables/formula1/pit_stops.json`)

# COMMAND ----------

df=spark.read.option("multiline",True).json("dbfs:/FileStore/tables/formula1/pit_stops.json")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.saveAsTable("saranya.pitstop")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from saranya.pitstop

# COMMAND ----------


