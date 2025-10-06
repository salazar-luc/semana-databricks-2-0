# ============================================================================
# SILVER LAYER: Drivers Dimension
# ============================================================================
# Purpose: Clean and standardize driver data from PostgreSQL source
# Source: bronze_postgres_drivers
# Target: silver_drivers
# ============================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="silver_drivers",
    comment="Cleaned driver dimension with standardized vehicle info and validated data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "driver_id"
    }
)
@dlt.expect_or_drop("valid_driver_id", "driver_id IS NOT NULL")
@dlt.expect_or_drop("valid_license", "license_number IS NOT NULL")
@dlt.expect_or_drop("valid_name", "first_name IS NOT NULL AND last_name IS NOT NULL")
@dlt.expect_or_drop("valid_vehicle", "vehicle_type IS NOT NULL")
def silver_drivers():
    """
    Clean drivers data with basic transformations:
    - Remove invalid records (null IDs, missing license)
    - Standardize phone numbers (remove special characters)
    - Calculate age from date of birth
    - Create full vehicle description
    - Add data quality timestamp
    """

    # Read from Bronze layer (streaming)
    bronze_df = dlt.read_stream("semana.default.bronze_postgres_drivers")

    # Clean and transform data
    return (
        bronze_df
        # Select and cast columns
        .select(
            F.col("driver_id").cast("bigint").alias("driver_id"),
            F.col("license_number").cast("string").alias("license_number"),
            F.col("first_name").cast("string").alias("first_name"),
            F.col("last_name").cast("string").alias("last_name"),
            F.col("date_birth").cast("date").alias("date_birth"),

            # Clean phone number: remove (, ), -, and spaces
            F.regexp_replace(
                F.col("phone_number"),
                "[^0-9]",
                ""
            ).alias("phone_number_clean"),

            F.col("city").cast("string").alias("city"),
            F.col("vehicle_type").cast("string").alias("vehicle_type"),
            F.col("vehicle_make").cast("string").alias("vehicle_make"),
            F.col("vehicle_model").cast("string").alias("vehicle_model"),
            F.col("vehicle_year").cast("int").alias("vehicle_year"),

            # Metadata fields
            F.col("source_file").alias("source_file"),
            F.col("ingestion_time").alias("ingestion_time")
        )
        # Add calculated columns
        .withColumn(
            "age",
            F.floor(F.datediff(F.current_date(), F.col("date_birth")) / 365.25)
        )
        .withColumn(
            "full_name",
            F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))
        )
        .withColumn(
            "vehicle_description",
            F.concat(
                F.col("vehicle_year").cast("string"), F.lit(" "),
                F.col("vehicle_make"), F.lit(" "),
                F.col("vehicle_model")
            )
        )
        .withColumn(
            "processed_time",
            F.current_timestamp()
        )
    )
