# ============================================================================
# GOLD LAYER: Cuisine Type Performance Analytics
# ============================================================================
# Purpose: Cuisine-level aggregated metrics for menu strategy and market trends
# Source: silver_orders_enriched
# Target: gold_cuisine_type_analytics
# ============================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="gold_cuisine_type_analytics",
    comment="Daily cuisine type performance metrics including popularity, revenue contribution, customer preferences, and market trends",
    table_properties={
        "quality": "gold",
        "domain": "product_analytics",
        "pipelines.autoOptimize.zOrderCols": "date,cuisine_type,total_orders"
    }
)
def gold_cuisine_type_analytics():
    """
    Cuisine type performance and trend analytics.

    Metrics:
    - Order volume and revenue by cuisine type
    - Customer demographics and preferences
    - Restaurant quality and availability
    - Temporal patterns (day/time preferences)
    - Market share and growth trends

    Business Use Cases:
    - Menu optimization and expansion planning
    - Restaurant partnership strategy
    - Marketing campaign targeting
    - Competitive analysis by cuisine segment
    - Customer preference insights
    - Revenue diversification tracking
    """

    # Read from Silver enriched table (batch - for aggregation)
    enriched = dlt.read("silver_orders_enriched")

    # Aggregate by date and cuisine type
    cuisine_analytics = (
        enriched
        # Group by date and cuisine
        .groupBy(
            F.to_date("order_date").alias("date"),
            "cuisine_type"
        )
        # Calculate aggregated metrics
        .agg(
            # === Volume Metrics ===
            F.count("order_id").alias("total_orders"),
            F.countDistinct("user_id").alias("unique_customers"),
            F.countDistinct("restaurant_id").alias("restaurants_serving"),

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
            F.collect_set("customer_age_group").alias("age_groups_list"),

            # === Customer Behavior ===
            F.countDistinct("customer_job").alias("unique_customer_jobs"),

            # === Restaurant Quality ===
            F.avg("restaurant_rating").alias("avg_restaurant_rating"),
            F.min("restaurant_rating").alias("min_restaurant_rating"),
            F.max("restaurant_rating").alias("max_restaurant_rating"),
            F.sum(F.when(F.col("restaurant_is_popular") == True, 1).otherwise(0)).alias("popular_restaurant_orders"),

            # === Delivery Metrics ===
            F.countDistinct("driver_id").alias("drivers_involved"),
            F.countDistinct("vehicle_type").alias("vehicle_types_used"),
            F.avg("driver_age").alias("avg_driver_age"),

            # === Geography ===
            F.countDistinct("restaurant_city").alias("cities_available"),
            F.collect_set("restaurant_city").alias("cities_list"),
            F.sum(F.when(F.col("is_same_city") == True, 1).otherwise(0)).alias("same_city_orders"),
            F.sum(F.when(F.col("is_same_city") == False, 1).otherwise(0)).alias("cross_city_orders"),

            # === Temporal Patterns ===
            F.avg("order_hour").alias("avg_order_hour"),
            F.min("order_hour").alias("earliest_order_hour"),
            F.max("order_hour").alias("latest_order_hour"),
            F.sum(F.when((F.col("order_hour") >= 6) & (F.col("order_hour") < 12), 1).otherwise(0)).alias("morning_orders"),
            F.sum(F.when((F.col("order_hour") >= 12) & (F.col("order_hour") < 18), 1).otherwise(0)).alias("afternoon_orders"),
            F.sum(F.when((F.col("order_hour") >= 18) & (F.col("order_hour") < 24), 1).otherwise(0)).alias("evening_orders"),
            F.sum(F.when((F.col("order_hour") >= 0) & (F.col("order_hour") < 6), 1).otherwise(0)).alias("night_orders"),

            # === Meal Periods ===
            F.sum(F.when((F.col("order_hour") >= 6) & (F.col("order_hour") < 10), 1).otherwise(0)).alias("breakfast_orders"),
            F.sum(F.when((F.col("order_hour") >= 11) & (F.col("order_hour") < 15), 1).otherwise(0)).alias("lunch_orders"),
            F.sum(F.when((F.col("order_hour") >= 17) & (F.col("order_hour") < 22), 1).otherwise(0)).alias("dinner_orders"),

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
            F.round(F.col("total_orders") / F.col("restaurants_serving"), 2)
        )
        .withColumn(
            "revenue_per_customer",
            F.round(F.col("total_revenue") / F.col("unique_customers"), 2)
        )
        .withColumn(
            "customer_loyalty_rate",
            F.round(F.col("total_orders") / F.col("unique_customers"), 2)
        )
        .withColumn(
            "high_value_order_pct",
            F.round((F.col("high_value_orders") / F.col("total_orders")) * 100, 2)
        )
        .withColumn(
            "popular_restaurant_pct",
            F.round((F.col("popular_restaurant_orders") / F.col("total_orders")) * 100, 2)
        )
        .withColumn(
            "cross_city_delivery_pct",
            F.round((F.col("cross_city_orders") / F.col("total_orders")) * 100, 2)
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
        # Primary meal period identification
        .withColumn(
            "primary_meal_period",
            F.when(
                F.col("dinner_orders") >= F.greatest("breakfast_orders", "lunch_orders"),
                "Dinner"
            )
            .when(F.col("lunch_orders") >= F.col("breakfast_orders"), "Lunch")
            .otherwise("Breakfast")
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
        # Restaurant supply status
        .withColumn(
            "restaurant_supply_status",
            F.when(F.col("orders_per_restaurant") >= 15, "High Demand Per Restaurant")
             .when(F.col("orders_per_restaurant") >= 8, "Moderate Demand")
             .otherwise("Low Demand")
        )
        # Popularity tier
        .withColumn(
            "popularity_tier",
            F.when(F.col("total_orders") >= 100, "Top Cuisine")
             .when(F.col("total_orders") >= 50, "Popular Cuisine")
             .when(F.col("total_orders") >= 20, "Moderate Cuisine")
             .otherwise("Niche Cuisine")
        )
        # Revenue tier
        .withColumn(
            "revenue_tier",
            F.when(F.col("total_revenue") >= 10000, "Premium Revenue")
             .when(F.col("total_revenue") >= 5000, "High Revenue")
             .when(F.col("total_revenue") >= 2000, "Medium Revenue")
             .otherwise("Low Revenue")
        )
        # Quality indicator
        .withColumn(
            "quality_indicator",
            F.when(F.col("avg_restaurant_rating") >= 4.5, "Excellent Quality")
             .when(F.col("avg_restaurant_rating") >= 4.0, "High Quality")
             .when(F.col("avg_restaurant_rating") >= 3.5, "Good Quality")
             .otherwise("Average Quality")
        )
        .withColumn(
            "computed_time",
            F.current_timestamp()
        )
    )

    return cuisine_analytics
