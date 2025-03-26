from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from src.utils.config import get_adls_config, get_service_principal_credentials
from src.utils.data_quality import (
    validate_power_output,
    validate_wind_speed,
    check_missing_values,
    quarantine_invalid_records
)

def create_streaming_query(spark: SparkSession) -> None:
    """Create and start the streaming query for bronze layer ingestion"""
    config = get_adls_config()
    credentials = get_service_principal_credentials()
    
    # Configure Autoloader read stream
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
    
    # Add metadata and perform data quality checks
    enriched_df = (streaming_df
        .withColumn("ingestion_timestamp", current_timestamp())
        .transform(validate_power_output)
        .transform(validate_wind_speed)
        .transform(check_missing_values))
    
    # Split into valid and invalid records
    valid_df, invalid_df = quarantine_invalid_records(enriched_df)
    
    # Configure write streams
    valid_query = (valid_df.writeStream
        .format("delta")
        .option("checkpointLocation", f"abfss://{config['container']}@{config['storage_account']}.dfs.core.windows.net/{config['bronze_path']}/checkpoint")
        .outputMode("append")
        .toTable("dev_ecowind.turbines.turbinestats_bronze"))
    
    invalid_query = (invalid_df.writeStream
        .format("delta")
        .option("checkpointLocation", f"abfss://{config['container']}@{config['storage_account']}.dfs.core.windows.net/{config['bronze_path']}/quarantine_checkpoint")
        .outputMode("append")
        .toTable("dev_ecowind.turbines.turbinestats_bronze_quarantine"))
    
    return valid_query, invalid_query 