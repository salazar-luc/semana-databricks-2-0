# ============================================================================
# SILVER LAYER: Order Status Events
# ============================================================================
# Purpose: Flatten and clean order status events from Kafka source
# Source: bronze_kafka_status
# Target: silver_status
# ============================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="silver_status",
    comment="Flattened and cleaned order status events with parsed timestamps",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "status_id,order_id"
    }
)
@dlt.expect_or_drop("valid_status_id", "status_id IS NOT NULL")
@dlt.expect_or_drop("valid_status_name", "status_name IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "status_timestamp IS NOT NULL")
def silver_status():
    """
    Clean status events with basic transformations:
    - Flatten nested status structure (status.status_name, status.timestamp)
    - Remove invalid records (null IDs, missing status)
    - Parse timestamp fields
    - Add data quality timestamp

    NOTE: Bronze table has nested structure:
    - status_id (top level)
    - order_identifier (top level) -> renamed to order_id
    - status.status_name (nested)
    - status.timestamp (nested)
    """

    # Read from Bronze layer (streaming)
    bronze_df = dlt.read_stream("semana.default.bronze_kafka_status")

    # Flatten and transform data
    return (
        bronze_df
        # Select and flatten nested fields
        .select(
            F.col("status_id").cast("bigint").alias("status_id"),

            # Rename order_identifier to order_id for consistency
            F.col("order_identifier").cast("bigint").alias("order_id"),

            # Flatten nested status struct
            F.col("status.status_name").cast("string").alias("status_name"),
            F.col("status.timestamp").cast("timestamp").alias("status_timestamp"),

            # Metadata fields
            F.col("source_file").alias("source_file"),
            F.col("ingestion_time").alias("ingestion_time")
        )
        # Add calculated columns
        .withColumn(
            "status_date",
            F.to_date(F.col("status_timestamp"))
        )
        .withColumn(
            "status_hour",
            F.hour(F.col("status_timestamp"))
        )
        .withColumn(
            "is_final_status",
            F.when(
                F.col("status_name").isin("delivered", "cancelled", "completed"),
                True
            ).otherwise(False)
        )
        .withColumn(
            "processed_time",
            F.current_timestamp()
        )
    )
