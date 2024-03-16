# Databricks notebook source
print("hello")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema saranya

# COMMAND ----------

data=([(1,'a',30),(2,'b',34)])
schema="id int, name string, age int"
df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.show()


# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/processed")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/raw/emp.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()


# COMMAND ----------

df.write.saveAsTable("saranya.emp_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from saranya.emp_demo

# COMMAND ----------


