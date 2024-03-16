# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/formula1/constructors.json")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *
df1=(df.withColumnRenamed("constructorId","constructor_Id")
.withColumnRenamed("constructorRef","constructor_Ref")
.withColumn("ingestion_Date",current_timestamp())
.drop("url"))

# COMMAND ----------

df.display()

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.write.saveAsTable("Saranya.circuit2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from saranya.circuit2 where constructor_Id>5

# COMMAND ----------

df1.write.parquet("dbfs:/FileStore/tables/processedformual1/saranya/cirucit")

# COMMAND ----------

df=spark.read.parquet("dbfs:/FileStore/tables/processedformual1/saranya/cirucit")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/FileStore/tables/processedformual1/saranya/cirucit`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table saranya.constructor as 
# MAGIC (select constructorId as constructor_id, constructorRef as constructor_ref, name, nationality, current_timestamp() as ingestion_date from json.`dbfs:/FileStore/tables/formula1/constructors.json`)

# COMMAND ----------


