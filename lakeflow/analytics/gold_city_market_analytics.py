# ============================================================================
# GOLD LAYER: City Market Performance Analytics
# ============================================================================
# Purpose: City-level aggregated metrics for market expansion and regional strategy
# Source: silver_orders_enriched
# Target: gold_city_market_analytics
# ============================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="gold_city_market_analytics",
    comment="Daily city-level market metrics including supply/demand balance, revenue performance, market penetration, and growth indicators",
    table_properties={
        "quality": "gold",
        "domain": "market_analytics",
        "pipelines.autoOptimize.zOrderCols": "date,city,total_orders"
    }
)
def gold_city_market_analytics():
    """
    City-level market performance and expansion analytics.

    Metrics:
    - Order volume and revenue by city
    - Supply metrics (restaurants, drivers available)
    - Demand metrics (customers, order patterns)
    - Market penetration and saturation
    - Competitive dynamics and growth potential

    Business Use Cases:
    - Market expansion planning
    - Regional performance comparison
    - Resource allocation by geography
    - Driver and restaurant recruitment targeting
    - Localized marketing strategies
    - Supply-demand optimization
    - City-specific pricing strategies
    """

    # Read from Silver enriched table (batch - for aggregation)
    enriched = dlt.read("silver_orders_enriched")

    # Aggregate by date and city (using restaurant city as market)
    city_analytics = (
        enriched
        # Group by date and restaurant city (where orders are fulfilled)
        .groupBy(
            F.to_date("order_date").alias("date"),
            F.col("restaurant_city").alias("city")
        )
        # Calculate aggregated metrics
        .agg(
            # === Volume Metrics ===
            F.count("order_id").alias("total_orders"),
            F.countDistinct("user_id").alias("unique_customers"),
            F.countDistinct("restaurant_id").alias("active_restaurants"),
            F.countDistinct("driver_id").alias("active_drivers"),

            # === Revenue Metrics ===
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("total_amount").alias("min_order_value"),
            F.max("total_amount").alias("max_order_value"),
            F.stddev("total_amount").alias("stddev_order_value"),

            # === Order Distribution ===
            F.sum(F.when(F.col("amount_category") == "High", 1).otherwise(0)).alias("high_value_orders"),
            F.sum(F.when(F.col("amount_category") == "Medium", 1).otherwise(0)).alias("medium_value_orders"),
            F.sum(F.when(F.col("amount_category") == "Low", 1).otherwise(0)).alias("low_value_orders"),

            # === Customer Demographics ===
            F.avg("customer_age").alias("avg_customer_age"),
            F.countDistinct("customer_age_group").alias("customer_age_groups"),
            F.countDistinct("customer_job").alias("unique_customer_jobs"),
            F.collect_set("customer_age_group").alias("age_groups_list"),

            # === Restaurant Supply ===
            F.countDistinct("cuisine_type").alias("cuisine_types_available"),
            F.collect_set("cuisine_type").alias("cuisines_list"),
            F.avg("restaurant_rating").alias("avg_restaurant_rating"),
            F.sum(F.when(F.col("restaurant_is_popular") == True, 1).otherwise(0)).alias("popular_restaurant_orders"),
            F.countDistinct(F.when(F.col("restaurant_is_popular") == True, F.col("restaurant_id"))).alias("popular_restaurants"),

            # === Driver Supply ===
            F.countDistinct("vehicle_type").alias("vehicle_types_available"),
            F.collect_set("vehicle_type").alias("vehicle_types_list"),
            F.avg("driver_age").alias("avg_driver_age"),

            # === Cross-City Dynamics ===
            F.sum(F.when(F.col("is_same_city") == True, 1).otherwise(0)).alias("local_orders"),
            F.sum(F.when(F.col("is_same_city") == False, 1).otherwise(0)).alias("cross_city_orders"),
            F.countDistinct("driver_city").alias("driver_cities_serving"),

            # === Temporal Patterns ===
            F.avg("order_hour").alias("avg_order_hour"),
            F.min("order_hour").alias("earliest_order_hour"),
            F.max("order_hour").alias("latest_order_hour"),
            F.approx_count_distinct("order_hour").alias("active_hours"),

            # === Time of Day Distribution ===
            F.sum(F.when((F.col("order_hour") >= 6) & (F.col("order_hour") < 12), 1).otherwise(0)).alias("morning_orders"),
            F.sum(F.when((F.col("order_hour") >= 12) & (F.col("order_hour") < 18), 1).otherwise(0)).alias("afternoon_orders"),
            F.sum(F.when((F.col("order_hour") >= 18) & (F.col("order_hour") < 24), 1).otherwise(0)).alias("evening_orders"),
            F.sum(F.when((F.col("order_hour") >= 0) & (F.col("order_hour") < 6), 1).otherwise(0)).alias("night_orders"),

            # === Metadata ===
            F.max("processed_time").alias("last_processed_time")
        )
        # Add calculated KPIs
        .withColumn(
            "revenue_per_order",
            F.round(F.col("total_revenue") / F.col("total_orders"), 2)
        )
        .withColumn(
            "orders_per_restaurant",
            F.round(F.col("total_orders") / F.col("active_restaurants"), 2)
        )
        .withColumn(
            "orders_per_driver",
            F.round(F.col("total_orders") / F.col("active_drivers"), 2)
        )
        .withColumn(
            "orders_per_customer",
            F.round(F.col("total_orders") / F.col("unique_customers"), 2)
        )
        .withColumn(
            "revenue_per_restaurant",
            F.round(F.col("total_revenue") / F.col("active_restaurants"), 2)
        )
        .withColumn(
            "revenue_per_driver",
            F.round(F.col("total_revenue") / F.col("active_drivers"), 2)
        )
        .withColumn(
            "revenue_per_customer",
            F.round(F.col("total_revenue") / F.col("unique_customers"), 2)
        )
        .withColumn(
            "high_value_order_pct",
            F.round((F.col("high_value_orders") / F.col("total_orders")) * 100, 2)
        )
        .withColumn(
            "cross_city_order_pct",
            F.round((F.col("cross_city_orders") / F.col("total_orders")) * 100, 2)
        )
        .withColumn(
            "popular_restaurant_pct",
            F.round((F.col("popular_restaurant_orders") / F.col("total_orders")) * 100, 2)
        )
        .withColumn(
            "customer_repeat_rate",
            F.round(F.col("total_orders") / F.col("unique_customers"), 2)
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
        # Market size classification
        .withColumn(
            "market_size",
            F.when(F.col("total_orders") >= 500, "Major Market")
             .when(F.col("total_orders") >= 200, "Large Market")
             .when(F.col("total_orders") >= 100, "Medium Market")
             .when(F.col("total_orders") >= 50, "Small Market")
             .otherwise("Emerging Market")
        )
        # Revenue classification
        .withColumn(
            "revenue_tier",
            F.when(F.col("total_revenue") >= 50000, "Tier 1 Revenue")
             .when(F.col("total_revenue") >= 20000, "Tier 2 Revenue")
             .when(F.col("total_revenue") >= 10000, "Tier 3 Revenue")
             .otherwise("Tier 4 Revenue")
        )
        # Driver supply status
        .withColumn(
            "driver_supply_status",
            F.when(F.col("orders_per_driver") >= 15, "Driver Shortage")
             .when(F.col("orders_per_driver") >= 10, "Tight Driver Supply")
             .when(F.col("orders_per_driver") >= 5, "Balanced Driver Supply")
             .otherwise("Surplus Driver Supply")
        )
        # Restaurant supply status
        .withColumn(
            "restaurant_supply_status",
            F.when(F.col("orders_per_restaurant") >= 25, "Restaurant Shortage")
             .when(F.col("orders_per_restaurant") >= 15, "High Restaurant Demand")
             .when(F.col("orders_per_restaurant") >= 8, "Balanced Restaurant Supply")
             .otherwise("Surplus Restaurant Supply")
        )
        # Market maturity indicator
        .withColumn(
            "market_maturity",
            F.when(
                (F.col("cuisine_types_available") >= 10) &
                (F.col("active_restaurants") >= 50) &
                (F.col("customer_repeat_rate") >= 2.0),
                "Mature Market"
            )
            .when(
                (F.col("cuisine_types_available") >= 5) &
                (F.col("active_restaurants") >= 20),
                "Growing Market"
            )
            .otherwise("Developing Market")
        )
        # Cuisine diversity score
        .withColumn(
            "cuisine_diversity_score",
            F.round(F.col("cuisine_types_available") / 10.0, 2)
        )
        # Market opportunity score (simple composite)
        .withColumn(
            "market_opportunity_score",
            F.round(
                (F.col("orders_per_driver") * 0.3) +
                (F.col("orders_per_restaurant") * 0.3) +
                (F.col("high_value_order_pct") * 0.2) +
                (F.col("customer_repeat_rate") * 0.2),
                2
            )
        )
        # Primary time of day
        .withColumn(
            "primary_time_of_day",
            F.when(
                F.col("evening_orders") >= F.greatest("morning_orders", "afternoon_orders", "night_orders"),
                "Evening"
            )
            .when(
                F.col("afternoon_orders") >= F.greatest("morning_orders", "night_orders"),
                "Afternoon"
            )
            .when(F.col("morning_orders") >= F.col("night_orders"), "Morning")
            .otherwise("Night")
        )
        .withColumn(
            "computed_time",
            F.current_timestamp()
        )
    )

    return city_analytics
