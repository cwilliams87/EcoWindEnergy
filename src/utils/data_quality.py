from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull
from src.utils.config import load_config

def validate_power_output(df: DataFrame) -> DataFrame:
    """Validate power output values are within expected range"""
    config = load_config()
    power_range = config["data_quality"]["power_output_range"]
    
    return df.withColumn(
        "is_valid_power",
        col("power_output").between(
            power_range["min"],
            power_range["max"]
        )
    )

def validate_wind_speed(df: DataFrame) -> DataFrame:
    """Validate wind speed values are within expected range"""
    config = load_config()
    wind_range = config["data_quality"]["wind_speed_range"]
    
    return df.withColumn(
        "is_valid_wind",
        col("wind_speed").between(
            wind_range["min"],
            wind_range["max"]
        )
    )

def check_missing_values(df: DataFrame) -> DataFrame:
    """Check for missing values in required columns"""
    config = load_config()
    max_missing = config["data_quality"]["max_missing_percentage"]
    
    # Calculate missing value percentages
    total_rows = df.count()
    missing_stats = df.select([
        (count(when(isnan(c) | isnull(c), c)) / total_rows * 100).alias(f"{c}_missing_pct")
        for c in df.columns
    ])
    
    # Add validation flags
    for col_name in df.columns:
        df = df.withColumn(
            f"is_valid_{col_name}",
            col(f"{col_name}_missing_pct") <= max_missing
        )
    
    return df

def quarantine_invalid_records(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separate valid and invalid records"""
    # Combine all validation flags
    validation_cols = [col(c) for c in df.columns if c.startswith("is_valid_")]
    is_valid = all(validation_cols)
    
    # Split into valid and invalid records
    valid_df = df.filter(is_valid)
    invalid_df = df.filter(~is_valid)
    
    return valid_df, invalid_df 