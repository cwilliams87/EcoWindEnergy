# Databricks notebook source
# DBTITLE 1, EcoWind Energy Header
# MAGIC %md
# MAGIC ![EcoWind Energy Header](../../additional/Ecowind Energy Header.png)

# COMMAND ----------
# DBTITLE 1, Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from databricks.sdk.runtime import dbutils

# COMMAND ----------
# DBTITLE 1, Configuration and Credentials
def get_adls_config():
    return {
        "storage_account": "your_storage_account",  # Replace with actual storage account
        "container": "dbx",
        "landing_path": "1-Landing",
        "bronze_path": "2-Bronze",
        "silver_path": "3-Silver"
    }

def get_service_principal_credentials():
    return {
        "client_id": dbutils.secrets.get("EcoWindCreds", "client_id"),
        "client_secret": dbutils.secrets.get("EcoWindCreds", "client_secret")
    }

# COMMAND ----------
# DBTITLE 1, Initialize Spark Session
spark = SparkSession.builder \
    .appName("EcoWind Bronze Ingestion") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

# COMMAND ----------
# DBTITLE 1, Get Configuration and Credentials
config = get_adls_config()
credentials = get_service_principal_credentials()

# COMMAND ----------
# DBTITLE 1, Configure Autoloader Read Stream
streaming_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"abfss://{config['container']}@{config['storage_account']}.dfs.core.windows.net/{config['bronze_path']}/schema")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.rescueDataColumn", "_rescued_data")
    .option("cloudFiles.connectionString", f"abfss://{config['container']}@{config['storage_account']}.dfs.core.windows.net")
    .option("fs.azure.account.key", credentials["client_id"])
    .option("fs.azure.account.key.type", "OAuth")
    .option("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    .option("fs.azure.account.oauth2.client.id", credentials["client_id"])
    .option("fs.azure.account.oauth2.client.endpoint", f"https://login.microsoftonline.com/{credentials['tenant_id']}/oauth2/token")
    .load(f"abfss://{config['container']}@{config['storage_account']}.dfs.core.windows.net/{config['landing_path']}"))

# COMMAND ----------
# DBTITLE 1, Add Metadata Columns
enriched_df = streaming_df.withColumn("ingestion_timestamp", current_timestamp())

# COMMAND ----------
# DBTITLE 1, Configure Write Stream to Bronze Table
query = (enriched_df.writeStream
    .format("delta")
    .option("checkpointLocation", f"abfss://{config['container']}@{config['storage_account']}.dfs.core.windows.net/{config['bronze_path']}/checkpoint")
    .outputMode("append")
    .toTable("dev_ecowind.turbines.turbinestats_bronze"))

# COMMAND ----------
# DBTITLE 1, Start the Streaming Query
query.awaitTermination() 