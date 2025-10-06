# ============================================================================
# SILVER LAYER: Restaurants Dimension
# ============================================================================
# Purpose: Clean and standardize restaurant data from MySQL source
# Source: bronze_mysql_restaurants
# Target: silver_restaurants
# ============================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="silver_restaurants",
    comment="Cleaned restaurant dimension with standardized ratings and operating hours",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "restaurant_id"
    }
)
@dlt.expect_or_drop("valid_name", "name IS NOT NULL")
@dlt.expect_or_drop("valid_rating", "average_rating >= 0 AND average_rating <= 5")
def silver_restaurants():
    """
    Clean restaurants data with basic transformations:
    - Remove invalid records (null IDs, invalid CNPJ)
    - Standardize phone numbers (remove special characters)
    - Validate rating range (0-5)
    - Calculate business hours duration
    - Add data quality timestamp
    """

    # Read from Bronze layer (streaming)
    bronze_df = dlt.read_stream("semana.default.bronze_mysql_restaurants")

    # Clean and transform data
    return (
        bronze_df
        # Select and cast columns
        .select(
            F.col("restaurant_id").cast("bigint").alias("restaurant_id"),
            F.col("cnpj").cast("string").alias("cnpj"),
            F.col("name").cast("string").alias("name"),
            F.col("cuisine_type").cast("string").alias("cuisine_type"),

            # Clean phone number: remove (, ), -, and spaces
            F.regexp_replace(
                F.col("phone_number"),
                "[^0-9]",
                ""
            ).alias("phone_number_clean"),

            F.col("city").cast("string").alias("city"),
            F.col("address").cast("string").alias("address"),
            F.col("average_rating").cast("double").alias("average_rating"),
            F.col("num_reviews").cast("int").alias("num_reviews"),
            F.col("opening_time").cast("string").alias("opening_time"),
            F.col("closing_time").cast("string").alias("closing_time"),

            # Metadata fields
            F.col("source_file").alias("source_file"),
            F.col("ingestion_time").alias("ingestion_time")
        )
        # Add calculated columns
        .withColumn(
            "rating_category",
            F.when(F.col("average_rating") >= 4.5, "Excellent")
             .when(F.col("average_rating") >= 4.0, "Very Good")
             .when(F.col("average_rating") >= 3.0, "Good")
             .when(F.col("average_rating") >= 2.0, "Fair")
             .otherwise("Poor")
        )
        .withColumn(
            "is_popular",
            F.when(F.col("num_reviews") >= 100, True).otherwise(False)
        )
        .withColumn(
            "processed_time",
            F.current_timestamp()
        )
    )
