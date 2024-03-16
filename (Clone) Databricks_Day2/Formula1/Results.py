# Databricks notebook source
input_path


# COMMAND ----------

# MAGIC %run "/Workspace/Users/saranya.tamilmani@mmc.com/Databricks_Day2/Formula1/includes"

# COMMAND ----------

(spark.read
 .json(f"{input_path}results.json")
 .write.mode("overwrite")
 .format("parquet")
 .save(f"{output_path}saranya/results"))

# COMMAND ----------

input_path


# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/formula1_raw/

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/cloudthats3/formula1_raw/lap_times/", header=True,inferSchema=True)

# COMMAND ----------

df.count()

# COMMAND ----------

df.display()

# COMMAND ----------

users_schem_pyspark=StructType([StructField("raceId",IntegerType()),
                                StructField("driverID",IntegerType()),
                                StructField("lap",IntegerType()),
                                StructField("position",IntegerType()),
                                StructField("time",StringType()),
                                StructField("millisecond",IntegerType())
])

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df=spark.read.schema(users_schem_pyspark).csv("dbfs:/mnt/cloudthats3/formula1_raw/lap_times/",header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.parquet(f"{output_path}saranya/laptimes")

# COMMAND ----------


