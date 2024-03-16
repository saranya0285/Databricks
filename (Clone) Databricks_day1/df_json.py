# Databricks notebook source
# MAGIC %fs ls
# MAGIC

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/raw/iot1.json",header=True,inferSchema=True)

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/raw/iot1.json")

# COMMAND ----------

df.display()


# COMMAND ----------

df.write.saveAsTable("saranya.jsonSample")

# COMMAND ----------

# MAGIC %run "/Workspace/Users/naval@cloudthat.net/day 1/includes"

# COMMAND ----------

input_path

# COMMAND ----------

name="Saranya"
dept="data engineering"

# COMMAND ----------

print(f"I am {name} working as a {dept}")

# COMMAND ----------

df=spark.read.json(f"{input_path}iot1.json")


# COMMAND ----------

df.display()

# COMMAND ----------


