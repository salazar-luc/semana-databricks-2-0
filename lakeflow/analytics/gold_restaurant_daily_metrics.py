# ============================================================================
# GOLD LAYER: Restaurant Daily Performance Metrics
# ============================================================================
# Purpose: Daily aggregated metrics per restaurant for business analytics
# Source: silver_orders_enriched
# Target: gold_restaurant_daily_metrics
# ============================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="gold_restaurant_daily_metrics",
    comment="Daily restaurant performance metrics including order volume, revenue, and customer analytics",
    table_properties={
        "quality": "gold",
        "domain": "restaurant_analytics",
        "pipelines.autoOptimize.zOrderCols": "date,restaurant_id"
    }
)
def gold_restaurant_daily_metrics():
    """
    Daily aggregated restaurant performance metrics.

    Metrics:
    - Order volume (count, distribution by amount category)
    - Revenue (total, average order value)
    - Customer analytics (unique customers, returning customers)
    - Operational metrics (orders per hour, peak hours)

    Business Use Cases:
    - Restaurant performance dashboards
    - Revenue tracking and forecasting
    - Customer retention analysis
    - Operational efficiency monitoring
    """

    # Read from Silver enriched table (batch - for aggregation)
    enriched = dlt.read("silver_orders_enriched")

    # Aggregate daily metrics per restaurant
    daily_metrics = (
        enriched
        # Group by restaurant and date
        .groupBy(
            "restaurant_id",
            "restaurant_name",
            "restaurant_cnpj",
            "cuisine_type",
            "restaurant_city",
            "restaurant_rating",
            "restaurant_rating_category",
            "restaurant_is_popular",
            F.to_date("order_date").alias("date")
        )
        # Calculate aggregated metrics
        .agg(
            # === Order Volume Metrics ===
            F.count("order_id").alias("total_orders"),
            F.countDistinct("user_id").alias("unique_customers"),

            # === Revenue Metrics ===
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("total_amount").alias("min_order_value"),
            F.max("total_amount").alias("max_order_value"),
            F.stddev("total_amount").alias("stddev_order_value"),

            # === Order Distribution ===
            F.sum(F.when(F.col("amount_category") == "High", 1).otherwise(0)).alias("orders_high_value"),
            F.sum(F.when(F.col("amount_category") == "Medium", 1).otherwise(0)).alias("orders_medium_value"),
            F.sum(F.when(F.col("amount_category") == "Low", 1).otherwise(0)).alias("orders_low_value"),

            # === Customer Demographics ===
            F.avg("customer_age").alias("avg_customer_age"),
            F.countDistinct("customer_age_group").alias("customer_age_groups_count"),

            # === Driver Metrics ===
            F.countDistinct("driver_id").alias("unique_drivers"),
            F.countDistinct("vehicle_type").alias("vehicle_types_used"),

            # === Temporal Metrics ===
            F.min("order_hour").alias("first_order_hour"),
            F.max("order_hour").alias("last_order_hour"),
            F.approx_count_distinct("order_hour").alias("active_hours"),

            # === Metadata ===
            F.max("processed_time").alias("last_processed_time")
        )
        # Add calculated KPIs
        .withColumn(
            "revenue_per_order",
            F.round(F.col("total_revenue") / F.col("total_orders"), 2)
        )
        .withColumn(
            "high_value_order_pct",
            F.round((F.col("orders_high_value") / F.col("total_orders")) * 100, 2)
        )
        .withColumn(
            "customer_retention_proxy",
            F.round(F.col("total_orders") / F.col("unique_customers"), 2)
        )
        .withColumn(
            "orders_per_driver",
            F.round(F.col("total_orders") / F.col("unique_drivers"), 2)
        )
        # Add date dimensions for easier filtering
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn(
            "day_name",
            F.when(F.col("day_of_week") == 1, "Sunday")
             .when(F.col("day_of_week") == 2, "Monday")
             .when(F.col("day_of_week") == 3, "Tuesday")
             .when(F.col("day_of_week") == 4, "Wednesday")
             .when(F.col("day_of_week") == 5, "Thursday")
             .when(F.col("day_of_week") == 6, "Friday")
             .otherwise("Saturday")
        )
        .withColumn(
            "is_weekend",
            F.when(F.col("day_of_week").isin(1, 7), True).otherwise(False)
        )
        .withColumn(
            "computed_time",
            F.current_timestamp()
        )
    )

    return daily_metrics
