# Databricks notebook source
# DBTITLE 1, EcoWind Energy Header
# MAGIC %md
# MAGIC ![EcoWind Energy Header](../../additional/Ecowind Energy Header slim.png)

# COMMAND ----------
# DBTITLE 1, Import required libraries
from pyspark.sql import SparkSession
from src.silver.transform import transform_bronze_to_silver

# COMMAND ----------
# DBTITLE 1, Initialize Spark Session
spark = SparkSession.builder \
    .appName("EcoWind Silver Transformation") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

# COMMAND ----------
# DBTITLE 1, Create and Start Streaming Query
query = transform_bronze_to_silver(spark)

# COMMAND ----------
# DBTITLE 1, Start the Streaming Query
query.awaitTermination() 