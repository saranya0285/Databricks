# Databricks notebook source
{"driverId":1,"driverRef":"hamilton","number":44,"code":"HAM","name":{"forename":"Lewis","surname":"Hamilton"},"dob":"1985-01-07","nationality":"British","url":"http://en.wikipedia.org/wiki/Lewis_Hamilton"}

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/formula1

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/formula1/drivers.json")

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

users_schem_pyspark=StructType([StructField("driverId",IntegerType()),
                                StructField("driverRef",StringType()),
                                StructField("number",StringType()),
                                StructField("code",StringType()),
                                StructField("name",MapType(StringType(),StringType())),
                                StructField("dob",StringType()),
                                StructField("nationality",StringType())
])

# COMMAND ----------

df=spark.read.schema(users_schem_pyspark).json("dbfs:/FileStore/tables/formula1/drivers.json")

# COMMAND ----------

display(df)

# COMMAND ----------

df2=spark.read.json("dbfs:/FileStore/tables/formula1/drivers.json")

# COMMAND ----------

display(df2)

# COMMAND ----------

from pyspark.sql.functions import *
df2.withColumn("forename",col("name.forename")).withColumn("surname",col("name.surname")).drop("name").display()

# COMMAND ----------

df2.write.saveAsTable("saranya.driver")

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.withColumn("forename",col("name.forename")).withColumn("surname",col("name.surname")).drop("name").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df1.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("saranya.driver")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from saranya.driver

# COMMAND ----------


