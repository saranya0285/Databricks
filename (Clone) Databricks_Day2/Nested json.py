# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

from pyspark.sql.functions import *
df=spark.read.json("dbfs:/FileStore/tables/raw/complexjson.json", multiLine=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn("topping",explode("topping")).withColumn("topping_id",col("topping.id")).display()

# COMMAND ----------

df.withColumn("topping",explode("topping")).withColumn("topping_id",col("topping.id")).withColumn("topping_type",col("topping.type")).display()

# COMMAND ----------

df.withColumn("topping",explode("topping")).withColumn("topping_id",col("topping.id")).withColumn("topping_type",col("topping.type")).drop("topping").display()

# COMMAND ----------

df_final=df\
    .withColumn("topping",explode("topping"))\
        .withColumn("topping_id",col("topping.id"))\
            .withColumn("topping_type",col("topping.type"))\
                .drop("topping")\
                    .withColumn("batters",explode("batters.batter"))\
                        .withColumn("batter_id",col("batters.id"))\
                            .withColumn("batter_type",col("batters.type"))\
                                .drop("batters")\
                            

# COMMAND ----------

df_final.write.saveAsTable("saranya.nestedData")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from saranya.nestedData

# COMMAND ----------


