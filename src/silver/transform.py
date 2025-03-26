from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, window, avg, min, max, stddev
from src.utils.config import get_catalog_config

def calculate_summary_statistics(df: DataFrame) -> DataFrame:
    """Calculate summary statistics for each turbine"""
    return (df
        .groupBy("turbine_id")
        .agg(
            min("power_output").alias("min_power"),
            max("power_output").alias("max_power"),
            avg("power_output").alias("avg_power"),
            stddev("power_output").alias("power_stddev")
        ))

def identify_anomalies(df: DataFrame) -> DataFrame:
    """Identify turbines with anomalous power output"""
    # Calculate z-score for power output
    return (df
        .withColumn(
            "power_zscore",
            (col("power_output") - col("avg_power")) / col("power_stddev")
        )
        .withColumn(
            "is_anomaly",
            abs(col("power_zscore")) > 2  # Outside 2 standard deviations
        ))

def transform_bronze_to_silver(spark: SparkSession) -> None:
    """Transform bronze data to silver layer"""
    catalog_config = get_catalog_config()
    
    # Read from bronze table
    bronze_df = spark.readStream.table(
        f"{catalog_config['catalog']}.{catalog_config['schema']}.{catalog_config['tables']['bronze']}"
    )
    
    # Calculate statistics and identify anomalies
    silver_df = (bronze_df
        .transform(calculate_summary_statistics)
        .transform(identify_anomalies))
    
    # Write to silver table
    query = (silver_df.writeStream
        .format("delta")
        .outputMode("complete")
        .toTable(f"{catalog_config['catalog']}.{catalog_config['schema']}.{catalog_config['tables']['silver']}"))
    
    return query 