# ============================================================================
# GOLD LAYER: Customer Behavior & Lifetime Value Metrics
# ============================================================================
# Purpose: Customer-level aggregated metrics for CLV, retention, and segmentation
# Source: silver_orders_enriched
# Target: gold_customer_behavior_metrics
# ============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dlt.table(
    name="gold_customer_behavior_metrics",
    comment="Customer lifetime value metrics including order frequency, spending patterns, preferences, and loyalty segmentation",
    table_properties={
        "quality": "gold",
        "domain": "customer_analytics",
        "pipelines.autoOptimize.zOrderCols": "user_id,total_orders_lifetime"
    }
)
def gold_customer_behavior_metrics():
    """
    Customer behavior and lifetime value analytics.

    Metrics:
    - Lifetime value (total spend, average order value, order count)
    - Temporal patterns (first/last order, days active, recency)
    - Preferences (favorite cuisines, restaurants, order times)
    - Behavioral segments (loyalty tier, value tier, activity level)

    Business Use Cases:
    - Customer lifetime value (CLV) analysis
    - Retention and churn prediction
    - Personalized marketing campaigns
    - Loyalty program optimization
    - Customer segmentation strategies
    """

    # Read from Silver enriched table (batch - for full customer history)
    enriched = dlt.read("silver_orders_enriched")

    # Calculate customer-level aggregated metrics
    customer_metrics = (
        enriched
        # Group by customer
        .groupBy(
            "user_id",
            "customer_cpf",
            "customer_name",
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_age_group",
            "customer_phone",
            "customer_job",
            "customer_company"
        )
        # Calculate lifetime metrics
        .agg(
            # === Lifetime Value Metrics ===
            F.count("order_id").alias("total_orders_lifetime"),
            F.sum("total_amount").alias("total_lifetime_value"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("total_amount").alias("min_order_value"),
            F.max("total_amount").alias("max_order_value"),
            F.stddev("total_amount").alias("stddev_order_value"),

            # === Temporal Behavior ===
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct(F.to_date("order_date")).alias("days_with_orders"),

            # === Restaurant Preferences ===
            F.countDistinct("restaurant_id").alias("unique_restaurants_visited"),
            F.first("restaurant_name").alias("most_recent_restaurant"),
            F.collect_set("cuisine_type").alias("cuisines_ordered"),
            F.countDistinct("cuisine_type").alias("unique_cuisines_tried"),

            # === Driver Interaction ===
            F.countDistinct("driver_id").alias("unique_drivers_interacted"),

            # === Order Distribution ===
            F.sum(F.when(F.col("amount_category") == "High", 1).otherwise(0)).alias("high_value_orders"),
            F.sum(F.when(F.col("amount_category") == "Medium", 1).otherwise(0)).alias("medium_value_orders"),
            F.sum(F.when(F.col("amount_category") == "Low", 1).otherwise(0)).alias("low_value_orders"),

            # === Temporal Patterns ===
            F.avg("order_hour").alias("avg_order_hour"),
            F.collect_set("order_hour").alias("order_hours_list"),
            F.sum(F.when(F.col("order_hour") >= 6, F.when(F.col("order_hour") < 12, 1).otherwise(0)).otherwise(0)).alias("morning_orders"),
            F.sum(F.when(F.col("order_hour") >= 12, F.when(F.col("order_hour") < 18, 1).otherwise(0)).otherwise(0)).alias("afternoon_orders"),
            F.sum(F.when(F.col("order_hour") >= 18, F.when(F.col("order_hour") < 24, 1).otherwise(0)).otherwise(0)).alias("evening_orders"),
            F.sum(F.when(F.col("order_hour") >= 0, F.when(F.col("order_hour") < 6, 1).otherwise(0)).otherwise(0)).alias("night_orders"),

            # === City Coverage ===
            F.countDistinct("restaurant_city").alias("cities_ordered_from"),
            F.collect_set("restaurant_city").alias("cities_list"),

            # === Quality Preference ===
            F.avg("restaurant_rating").alias("avg_restaurant_rating_preference"),
            F.sum(F.when(F.col("restaurant_is_popular") == True, 1).otherwise(0)).alias("popular_restaurant_orders"),

            # === Metadata ===
            F.max("processed_time").alias("last_processed_time")
        )
        # Add calculated KPIs and segments
        .withColumn(
            "customer_tenure_days",
            F.datediff(F.col("last_order_date"), F.col("first_order_date")) + 1
        )
        .withColumn(
            "recency_days",
            F.datediff(F.current_date(), F.col("last_order_date"))
        )
        .withColumn(
            "order_frequency",
            F.round(F.col("total_orders_lifetime") / F.col("customer_tenure_days"), 3)
        )
        .withColumn(
            "high_value_order_pct",
            F.round((F.col("high_value_orders") / F.col("total_orders_lifetime")) * 100, 2)
        )
        .withColumn(
            "restaurant_loyalty_score",
            F.round(F.col("total_orders_lifetime") / F.col("unique_restaurants_visited"), 2)
        )
        .withColumn(
            "cuisine_diversity_score",
            F.round(F.col("unique_cuisines_tried") / F.col("total_orders_lifetime"), 2)
        )
        .withColumn(
            "popular_restaurant_preference_pct",
            F.round((F.col("popular_restaurant_orders") / F.col("total_orders_lifetime")) * 100, 2)
        )
        # Customer Segmentation: RFM-inspired (Recency, Frequency, Monetary)
        .withColumn(
            "recency_segment",
            F.when(F.col("recency_days") <= 7, "Active")
             .when(F.col("recency_days") <= 30, "Recent")
             .when(F.col("recency_days") <= 90, "Lapsed")
             .otherwise("Churned")
        )
        .withColumn(
            "frequency_segment",
            F.when(F.col("total_orders_lifetime") >= 50, "Very Frequent")
             .when(F.col("total_orders_lifetime") >= 20, "Frequent")
             .when(F.col("total_orders_lifetime") >= 5, "Occasional")
             .otherwise("Rare")
        )
        .withColumn(
            "monetary_segment",
            F.when(F.col("total_lifetime_value") >= 5000, "VIP")
             .when(F.col("total_lifetime_value") >= 2000, "High Value")
             .when(F.col("total_lifetime_value") >= 500, "Medium Value")
             .otherwise("Low Value")
        )
        # Loyalty Tier (combining frequency and monetary)
        .withColumn(
            "loyalty_tier",
            F.when(
                (F.col("total_orders_lifetime") >= 50) & (F.col("total_lifetime_value") >= 5000),
                "Platinum"
            )
            .when(
                (F.col("total_orders_lifetime") >= 20) & (F.col("total_lifetime_value") >= 2000),
                "Gold"
            )
            .when(
                (F.col("total_orders_lifetime") >= 5) & (F.col("total_lifetime_value") >= 500),
                "Silver"
            )
            .otherwise("Bronze")
        )
        # Preferred time of day
        .withColumn(
            "preferred_time_of_day",
            F.when(F.col("morning_orders") >= F.greatest("afternoon_orders", "evening_orders", "night_orders"), "Morning")
             .when(F.col("afternoon_orders") >= F.greatest("evening_orders", "night_orders"), "Afternoon")
             .when(F.col("evening_orders") >= F.col("night_orders"), "Evening")
             .otherwise("Night")
        )
        # Churn risk indicator
        .withColumn(
            "churn_risk",
            F.when(
                (F.col("recency_days") > 90) & (F.col("total_orders_lifetime") >= 5),
                "High Risk"
            )
            .when(
                (F.col("recency_days") > 60) & (F.col("total_orders_lifetime") >= 5),
                "Medium Risk"
            )
            .when(F.col("recency_days") > 30, "Low Risk")
            .otherwise("Active")
        )
        .withColumn(
            "computed_time",
            F.current_timestamp()
        )
    )

    return customer_metrics
