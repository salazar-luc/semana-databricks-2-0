# ============================================================================
# GOLD LAYER: Driver Performance Metrics
# ============================================================================
# Purpose: Daily aggregated driver performance and efficiency metrics
# Source: silver_orders_enriched
# Target: gold_driver_performance
# ============================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="gold_driver_performance",
    comment="Daily driver performance metrics including delivery volume, revenue generated, and efficiency indicators",
    table_properties={
        "quality": "gold",
        "domain": "driver_analytics",
        "pipelines.autoOptimize.zOrderCols": "date,driver_id"
    }
)
def gold_driver_performance():
    """
    Daily aggregated driver performance metrics.

    Metrics:
    - Delivery volume (total orders, unique customers/restaurants served)
    - Revenue contribution (total, average per delivery)
    - Efficiency (deliveries per hour, cross-city deliveries)
    - Customer/restaurant diversity

    Business Use Cases:
    - Driver productivity dashboards
    - Performance-based compensation
    - Resource allocation optimization
    - Driver retention and engagement
    """

    # Read from Silver enriched table (batch - for aggregation)
    enriched = dlt.read("silver_orders_enriched")

    # Aggregate daily metrics per driver
    driver_metrics = (
        enriched
        # Group by driver and date
        .groupBy(
            "driver_id",
            "driver_name",
            "driver_license",
            "driver_city",
            "driver_age",
            "driver_age_group",
            "vehicle_type",
            "vehicle_make",
            "vehicle_model",
            "vehicle_year",
            "vehicle_description",
            F.to_date("order_date").alias("date")
        )
        # Calculate aggregated metrics
        .agg(
            # === Delivery Volume Metrics ===
            F.count("order_id").alias("total_deliveries"),
            F.countDistinct("user_id").alias("unique_customers_served"),
            F.countDistinct("restaurant_id").alias("unique_restaurants_served"),

            # === Revenue Metrics ===
            F.sum("total_amount").alias("total_revenue_generated"),
            F.avg("total_amount").alias("avg_order_value"),
            F.max("total_amount").alias("max_order_value"),

            # === Order Distribution ===
            F.sum(F.when(F.col("amount_category") == "High", 1).otherwise(0)).alias("high_value_deliveries"),
            F.sum(F.when(F.col("amount_category") == "Medium", 1).otherwise(0)).alias("medium_value_deliveries"),
            F.sum(F.when(F.col("amount_category") == "Low", 1).otherwise(0)).alias("low_value_deliveries"),

            # === Geography Metrics ===
            F.countDistinct("restaurant_city").alias("cities_served"),
            F.sum(F.when(F.col("is_same_city") == False, 1).otherwise(0)).alias("cross_city_deliveries"),

            # === Cuisine Diversity ===
            F.countDistinct("cuisine_type").alias("cuisine_types_delivered"),
            F.collect_set("cuisine_type").alias("cuisine_types_list"),

            # === Restaurant Quality Metrics ===
            F.avg("restaurant_rating").alias("avg_restaurant_rating"),
            F.sum(F.when(F.col("restaurant_is_popular") == True, 1).otherwise(0)).alias("popular_restaurant_deliveries"),

            # === Temporal Metrics ===
            F.min("order_hour").alias("first_delivery_hour"),
            F.max("order_hour").alias("last_delivery_hour"),
            F.approx_count_distinct("order_hour").alias("active_hours"),

            # === Customer Demographics ===
            F.avg("customer_age").alias("avg_customer_age_served"),

            # === Metadata ===
            F.max("processed_time").alias("last_processed_time")
        )
        # Add calculated KPIs
        .withColumn(
            "revenue_per_delivery",
            F.round(F.col("total_revenue_generated") / F.col("total_deliveries"), 2)
        )
        .withColumn(
            "deliveries_per_active_hour",
            F.round(F.col("total_deliveries") / F.col("active_hours"), 2)
        )
        .withColumn(
            "high_value_delivery_pct",
            F.round((F.col("high_value_deliveries") / F.col("total_deliveries")) * 100, 2)
        )
        .withColumn(
            "cross_city_delivery_pct",
            F.round((F.col("cross_city_deliveries") / F.col("total_deliveries")) * 100, 2)
        )
        .withColumn(
            "customer_diversity_score",
            F.round(F.col("unique_customers_served") / F.col("total_deliveries"), 2)
        )
        .withColumn(
            "restaurant_diversity_score",
            F.round(F.col("unique_restaurants_served") / F.col("total_deliveries"), 2)
        )
        .withColumn(
            "working_hours",
            F.col("last_delivery_hour") - F.col("first_delivery_hour") + 1
        )
        # Add date dimensions
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
        # Add performance categories
        .withColumn(
            "performance_tier",
            F.when(F.col("total_deliveries") >= 20, "High Performer")
             .when(F.col("total_deliveries") >= 10, "Average Performer")
             .otherwise("Low Performer")
        )
        .withColumn(
            "computed_time",
            F.current_timestamp()
        )
    )

    return driver_metrics
