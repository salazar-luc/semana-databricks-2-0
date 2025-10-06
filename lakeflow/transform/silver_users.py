# ============================================================================
# SILVER LAYER: Users Dimension
# ============================================================================
# Purpose: Clean and standardize user data from MSSQL source
# Source: bronze_mssql_users
# Target: silver_users
# ============================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="silver_users",
    comment="Cleaned user dimension with standardized phone numbers and validated data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "user_id"
    }
)
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_name", "first_name IS NOT NULL AND last_name IS NOT NULL")
def silver_users():
    """
    Clean users data with basic transformations:
    - Remove invalid records (null IDs, invalid CPF)
    - Standardize phone numbers (remove special characters)
    - Calculate age from birthday
    - Add data quality timestamp
    """

    # Read from Bronze layer (streaming)
    bronze_df = dlt.read_stream("semana.default.bronze_mssql_users")

    # Clean and transform data
    return (
        bronze_df
        # Select and rename columns for clarity
        .select(
            F.col("user_id").cast("bigint").alias("user_id"),
            F.col("cpf").cast("string").alias("cpf"),
            F.col("first_name").cast("string").alias("first_name"),
            F.col("last_name").cast("string").alias("last_name"),
            F.col("birthday").cast("date").alias("birthday"),

            # Clean phone number: remove (, ), -, and spaces
            F.regexp_replace(
                F.col("phone_number"),
                "[^0-9]",
                ""
            ).alias("phone_number_clean"),

            F.col("job").cast("string").alias("job"),
            F.col("company_name").cast("string").alias("company_name"),

            # Metadata fields
            F.col("source_file").alias("source_file"),
            F.col("ingestion_time").alias("ingestion_time")
        )
        # Add calculated columns
        .withColumn(
            "age",
            F.floor(F.datediff(F.current_date(), F.col("birthday")) / 365.25)
        )
        .withColumn(
            "full_name",
            F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))
        )
        .withColumn(
            "processed_time",
            F.current_timestamp()
        )
    )
