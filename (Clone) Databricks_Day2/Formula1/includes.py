# Databricks notebook source
input_path="dbfs:/mnt/cloudthats3/formula1_raw/"
output_path="dbfs:/mnt/cloudthats3/formula1_processed_parquet/"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
