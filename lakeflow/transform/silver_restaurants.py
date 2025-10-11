# ============================================================================
# SILVER LAYER: Restaurants Dimension
# ============================================================================
# Purpose: Clean and standardize restaurant data from MySQL source
# Source: bronze_mysql_restaurants
# Target: silver_restaurants
# ============================================================================
# Multi-Cloud Support:
#   AWS Schema: restaurant_id, cnpj, name, cuisine_type, phone_number, city,
#               address, average_rating, num_reviews, opening_time, closing_time
#   Azure Schema: restaurant_id, name, active, description, dt_current_timestamp,
#                 menu_section_id
# ============================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="silver_restaurants",
    comment="Cleaned restaurant dimension with multi-cloud schema support (AWS/Azure)",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "restaurant_id"
    }
)
@dlt.expect_or_drop("valid_restaurant_id", "restaurant_id IS NOT NULL")
@dlt.expect_or_drop("valid_name", "name IS NOT NULL")
def silver_restaurants():
    """
    Clean restaurants data with multi-cloud schema compatibility.

    Handles two different source schemas:
    - AWS: Full restaurant profile with CNPJ, ratings, contact info
    - Azure: Menu-focused data with sections and active status

    Creates unified silver table with defaults for missing columns.
    """

    # Read from Bronze layer (streaming)
    bronze_df = dlt.read_stream("bronze_mysql_restaurants")

    # Detect schema by checking if Azure-specific column exists
    # Azure has: active, description, dt_current_timestamp, menu_section_id
    # AWS has: cnpj, cuisine_type, phone_number, city, address, etc.
    available_columns = set(bronze_df.columns)
    is_azure_schema = "menu_section_id" in available_columns
    is_aws_schema = "cnpj" in available_columns

    # Build schema-specific transformations
    if is_azure_schema:
        # Azure schema: restaurant_id, name, active, description, dt_current_timestamp, menu_section_id
        return (
            bronze_df
            .select(
                F.col("restaurant_id").cast("bigint").alias("restaurant_id"),
                F.lit(None).cast("string").alias("cnpj"),
                F.col("name").cast("string").alias("name"),
                F.col("description").cast("string").alias("cuisine_type"),  # Map description -> cuisine_type
                F.lit(None).cast("string").alias("phone_number_clean"),
                F.lit("Unknown").cast("string").alias("city"),
                F.lit(None).cast("string").alias("address"),
                F.lit(4.0).cast("double").alias("average_rating"),  # Default rating
                F.lit(0).cast("int").alias("num_reviews"),
                F.lit(None).cast("string").alias("opening_time"),
                F.lit(None).cast("string").alias("closing_time"),
                F.col("active").cast("boolean").alias("is_active"),
                F.col("menu_section_id").cast("bigint").alias("menu_section_id"),
                F.col("source_file"),
                F.col("ingestion_time")
            )
            .withColumn("rating_category", F.lit("Good"))  # Default for Azure
            .withColumn("is_popular", F.lit(False))
            .withColumn("data_source", F.lit("azure"))
            .withColumn("processed_time", F.current_timestamp())
        )
    else:
        # AWS schema: restaurant_id, cnpj, name, cuisine_type, phone_number, city, address, etc.
        return (
            bronze_df
            .select(
                F.col("restaurant_id").cast("bigint").alias("restaurant_id"),
                F.col("cnpj").cast("string").alias("cnpj"),
                F.col("name").cast("string").alias("name"),
                F.col("cuisine_type").cast("string").alias("cuisine_type"),
                F.regexp_replace(F.col("phone_number"), "[^0-9]", "").alias("phone_number_clean"),
                F.col("city").cast("string").alias("city"),
                F.col("address").cast("string").alias("address"),
                F.col("average_rating").cast("double").alias("average_rating"),
                F.col("num_reviews").cast("int").alias("num_reviews"),
                F.col("opening_time").cast("string").alias("opening_time"),
                F.col("closing_time").cast("string").alias("closing_time"),
                F.lit(True).cast("boolean").alias("is_active"),  # Default for AWS
                F.lit(None).cast("bigint").alias("menu_section_id"),
                F.col("source_file"),
                F.col("ingestion_time")
            )
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
            .withColumn("data_source", F.lit("aws"))
            .withColumn("processed_time", F.current_timestamp())
        )
