# ============================================================================
# GOLD LAYER: Hourly Order Patterns & Demand Analytics
# ============================================================================
# Purpose: Time-based demand patterns for operational planning and forecasting
# Source: silver_orders_enriched
# Target: gold_hourly_order_patterns
# ============================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="gold_hourly_order_patterns",
    comment="Hourly aggregated order patterns including demand forecasting metrics, peak hour analysis, and day-of-week seasonality",
    table_properties={
        "quality": "gold",
        "domain": "operational_analytics",
        "pipelines.autoOptimize.zOrderCols": "date,hour"
    }
)
def gold_hourly_order_patterns():
    """
    Hourly order patterns and demand analytics.

    Metrics:
    - Order volume by hour of day
    - Revenue patterns across different time periods
    - Supply/demand balance (orders vs drivers/restaurants available)
    - Day-of-week and weekend patterns
    - Peak hour identification

    Business Use Cases:
    - Demand forecasting and capacity planning
    - Driver shift scheduling optimization
    - Restaurant partnership incentive timing
    - Marketing campaign timing
    - Surge pricing strategy
    - Operational efficiency optimization
    """

    # Read from Silver enriched table (batch - for temporal aggregation)
    enriched = dlt.read("silver_orders_enriched")

    # Aggregate by date and hour
    hourly_patterns = (
        enriched
        # Group by date and hour
        .groupBy(
            F.to_date("order_date").alias("date"),
            "order_hour"
        )
        # Calculate hourly metrics
        .agg(
            # === Volume Metrics ===
            F.count("order_id").alias("total_orders"),
            F.countDistinct("user_id").alias("unique_customers"),
            F.countDistinct("driver_id").alias("active_drivers"),
            F.countDistinct("restaurant_id").alias("active_restaurants"),

            # === Revenue Metrics ===
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("total_amount").alias("min_order_value"),
            F.max("total_amount").alias("max_order_value"),

            # === Order Distribution ===
            F.sum(F.when(F.col("amount_category") == "High", 1).otherwise(0)).alias("high_value_orders"),
            F.sum(F.when(F.col("amount_category") == "Medium", 1).otherwise(0)).alias("medium_value_orders"),
            F.sum(F.when(F.col("amount_category") == "Low", 1).otherwise(0)).alias("low_value_orders"),

            # === Customer Demographics ===
            F.avg("customer_age").alias("avg_customer_age"),
            F.countDistinct("customer_age_group").alias("age_groups_active"),

            # === Restaurant Metrics ===
            F.countDistinct("cuisine_type").alias("cuisine_types_active"),
            F.avg("restaurant_rating").alias("avg_restaurant_rating"),
            F.sum(F.when(F.col("restaurant_is_popular") == True, 1).otherwise(0)).alias("popular_restaurant_orders"),

            # === Geography ===
            F.countDistinct("restaurant_city").alias("cities_active"),
            F.sum(F.when(F.col("is_same_city") == True, 1).otherwise(0)).alias("same_city_orders"),
            F.sum(F.when(F.col("is_same_city") == False, 1).otherwise(0)).alias("cross_city_orders"),

            # === Vehicle Types ===
            F.countDistinct("vehicle_type").alias("vehicle_types_active"),

            # === Metadata ===
            F.max("processed_time").alias("last_processed_time")
        )
        # Add calculated KPIs
        .withColumn(
            "revenue_per_order",
            F.round(F.col("total_revenue") / F.col("total_orders"), 2)
        )
        .withColumn(
            "orders_per_driver",
            F.round(F.col("total_orders") / F.col("active_drivers"), 2)
        )
        .withColumn(
            "orders_per_restaurant",
            F.round(F.col("total_orders") / F.col("active_restaurants"), 2)
        )
        .withColumn(
            "high_value_order_pct",
            F.round((F.col("high_value_orders") / F.col("total_orders")) * 100, 2)
        )
        .withColumn(
            "customer_retention_rate",
            F.round(F.col("total_orders") / F.col("unique_customers"), 2)
        )
        .withColumn(
            "cross_city_order_pct",
            F.round((F.col("cross_city_orders") / F.col("total_orders")) * 100, 2)
        )
        .withColumn(
            "popular_restaurant_pct",
            F.round((F.col("popular_restaurant_orders") / F.col("total_orders")) * 100, 2)
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
        # Time of day categorization
        .withColumn(
            "time_of_day",
            F.when((F.col("order_hour") >= 6) & (F.col("order_hour") < 12), "Morning")
             .when((F.col("order_hour") >= 12) & (F.col("order_hour") < 18), "Afternoon")
             .when((F.col("order_hour") >= 18) & (F.col("order_hour") < 24), "Evening")
             .otherwise("Night")
        )
        # Meal time categorization
        .withColumn(
            "meal_period",
            F.when((F.col("order_hour") >= 6) & (F.col("order_hour") < 10), "Breakfast")
             .when((F.col("order_hour") >= 11) & (F.col("order_hour") < 15), "Lunch")
             .when((F.col("order_hour") >= 17) & (F.col("order_hour") < 22), "Dinner")
             .otherwise("Off-Hours")
        )
        # Supply-demand indicators
        .withColumn(
            "driver_supply_level",
            F.when(F.col("orders_per_driver") >= 5, "Undersupplied")
             .when(F.col("orders_per_driver") >= 3, "Balanced")
             .otherwise("Oversupplied")
        )
        .withColumn(
            "restaurant_supply_level",
            F.when(F.col("orders_per_restaurant") >= 10, "High Demand")
             .when(F.col("orders_per_restaurant") >= 5, "Moderate Demand")
             .otherwise("Low Demand")
        )
        # Demand intensity
        .withColumn(
            "demand_intensity",
            F.when(F.col("total_orders") >= 100, "Peak Hour")
             .when(F.col("total_orders") >= 50, "High Demand")
             .when(F.col("total_orders") >= 20, "Moderate Demand")
             .otherwise("Low Demand")
        )
        .withColumn(
            "computed_time",
            F.current_timestamp()
        )
    )

    return hourly_patterns
