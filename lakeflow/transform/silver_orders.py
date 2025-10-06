# ============================================================================
# SILVER LAYER: Orders Fact Table
# ============================================================================
# Purpose: Clean and standardize order data from Kafka source
# Source: bronze_kafka_orders
# Target: silver_orders
# ============================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="silver_orders",
    comment="Cleaned orders fact table with validated amounts and foreign keys",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "order_id,order_date"
    }
)
@dlt.expect_or_drop("valid_user_key", "user_key IS NOT NULL")
@dlt.expect_or_drop("valid_driver_key", "driver_key IS NOT NULL")
@dlt.expect_or_drop("valid_restaurant_key", "restaurant_key IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "total_amount > 0")
@dlt.expect_or_drop("valid_date", "order_date IS NOT NULL")
def silver_orders():
    """
    Clean orders data with basic transformations:
    - Remove invalid records (null IDs, negative amounts)
    - Cast keys and amounts to proper types
    - Parse order date/time fields
    - Calculate order value categories
    - Add data quality timestamp
    """

    # Read from Bronze layer (streaming)
    bronze_df = dlt.read_stream("semana.default.bronze_kafka_orders")

    # Clean and transform data
    return (
        bronze_df
        # Select and cast columns
        .select(
            F.col("order_id").cast("bigint").alias("order_id"),

            # Foreign keys (business keys from source systems)
            F.col("user_key").cast("string").alias("user_key"),        # CPF
            F.col("driver_key").cast("string").alias("driver_key"),    # License Number
            F.col("restaurant_key").cast("string").alias("restaurant_key"),  # CNPJ

            # Optional keys
            F.col("rating_key").cast("string").alias("rating_key"),
            F.col("payment_key").cast("string").alias("payment_key"),

            # Order details
            F.col("order_date").cast("timestamp").alias("order_date"),
            F.col("total_amount").cast("decimal(10,2)").alias("total_amount"),

            # Metadata fields
            F.col("source_file").alias("source_file"),
            F.col("ingestion_time").alias("ingestion_time")
        )
        # Add calculated columns
        .withColumn(
            "order_year",
            F.year(F.col("order_date"))
        )
        .withColumn(
            "order_month",
            F.month(F.col("order_date"))
        )
        .withColumn(
            "order_day",
            F.dayofmonth(F.col("order_date"))
        )
        .withColumn(
            "order_hour",
            F.hour(F.col("order_date"))
        )
        .withColumn(
            "amount_category",
            F.when(F.col("total_amount") >= 100, "High")
             .when(F.col("total_amount") >= 50, "Medium")
             .otherwise("Low")
        )
        .withColumn(
            "processed_time",
            F.current_timestamp()
        )
    )
