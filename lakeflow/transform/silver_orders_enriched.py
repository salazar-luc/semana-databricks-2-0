# ============================================================================
# SILVER LAYER TIER 2: Orders Enriched (Domain Table)
# ============================================================================
# Purpose: Denormalized fact table with complete order context
# Sources: silver_orders + silver_users + silver_drivers + silver_restaurants
# Target: silver_orders_enriched
# ============================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="silver_orders_enriched",
    comment="Complete order context with denormalized user, driver, and restaurant dimensions. Optimized for analytics without repeated joins.",
    table_properties={
        "quality": "silver",
        "domain": "orders_analytics",
        "pipelines.autoOptimize.zOrderCols": "order_id,restaurant_id,order_date"
    }
)
@dlt.expect_or_drop("valid_user_join", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_driver_join", "driver_id IS NOT NULL")
@dlt.expect_or_drop("valid_restaurant_join", "restaurant_id IS NOT NULL")
def silver_orders_enriched():
    """
    Enriched order fact table with denormalized dimensions.

    Joins:
    - silver_orders (fact) with user_key (CPF)
    - silver_users (dimension) on cpf
    - silver_drivers (dimension) on license_number
    - silver_restaurants (dimension) on cnpj

    Business Value:
    - Single table for order analysis (no downstream joins needed)
    - Self-service analytics enablement
    - Query performance optimization
    - Complete customer, driver, and restaurant context
    """

    # Read from Silver Tier 1 tables
    # Orders: streaming (fact table updates frequently)
    orders = dlt.read_stream("silver_orders")

    # Dimensions: batch reads (slowly changing, less frequent updates)
    users = dlt.read("silver_users")
    drivers = dlt.read("silver_drivers")
    restaurants = dlt.read("silver_restaurants")

    # Perform enrichment joins
    enriched = (
        orders
        # Join user dimension (left join to preserve orders even if user not found)
        .join(
            users,
            orders.user_key == users.cpf,
            "left"
        )
        # Join driver dimension
        .join(
            drivers,
            orders.driver_key == drivers.license_number,
            "left"
        )
        # Join restaurant dimension
        .join(
            restaurants,
            orders.restaurant_key == restaurants.cnpj,
            "left"
        )
        # Select enriched columns
        .select(
            # === Order Facts ===
            orders.order_id,
            orders.order_date,
            orders.total_amount,
            orders.amount_category,
            orders.order_year,
            orders.order_month,
            orders.order_day,
            orders.order_hour,

            # === User Dimension ===
            users.user_id,
            users.cpf.alias("customer_cpf"),
            users.full_name.alias("customer_name"),
            users.first_name.alias("customer_first_name"),
            users.last_name.alias("customer_last_name"),
            users.age.alias("customer_age"),
            users.phone_number_clean.alias("customer_phone"),
            users.job.alias("customer_job"),
            users.company_name.alias("customer_company"),

            # === Driver Dimension ===
            drivers.driver_id,
            drivers.license_number.alias("driver_license"),
            drivers.full_name.alias("driver_name"),
            drivers.first_name.alias("driver_first_name"),
            drivers.last_name.alias("driver_last_name"),
            drivers.age.alias("driver_age"),
            drivers.phone_number_clean.alias("driver_phone"),
            drivers.city.alias("driver_city"),
            drivers.vehicle_type,
            drivers.vehicle_make,
            drivers.vehicle_model,
            drivers.vehicle_year,
            drivers.vehicle_description,

            # === Restaurant Dimension ===
            restaurants.restaurant_id,
            restaurants.cnpj.alias("restaurant_cnpj"),
            restaurants.name.alias("restaurant_name"),
            restaurants.cuisine_type,
            restaurants.phone_number_clean.alias("restaurant_phone"),
            restaurants.city.alias("restaurant_city"),
            restaurants.address.alias("restaurant_address"),
            restaurants.average_rating.alias("restaurant_rating"),
            restaurants.num_reviews.alias("restaurant_reviews"),
            restaurants.rating_category.alias("restaurant_rating_category"),
            restaurants.is_popular.alias("restaurant_is_popular"),
            restaurants.opening_time,
            restaurants.closing_time,

            # === Business Keys (for reference) ===
            orders.user_key,
            orders.driver_key,
            orders.restaurant_key,

            # === Optional Keys ===
            orders.rating_key,
            orders.payment_key,

            # === Metadata ===
            orders.source_file.alias("order_source_file"),
            orders.ingestion_time.alias("order_ingestion_time")
        )
        # Add calculated enrichment fields
        .withColumn(
            "customer_age_group",
            F.when(F.col("customer_age") < 25, "18-24")
             .when(F.col("customer_age") < 35, "25-34")
             .when(F.col("customer_age") < 45, "35-44")
             .when(F.col("customer_age") < 55, "45-54")
             .when(F.col("customer_age") < 65, "55-64")
             .otherwise("65+")
        )
        .withColumn(
            "driver_age_group",
            F.when(F.col("driver_age") < 25, "18-24")
             .when(F.col("driver_age") < 35, "25-34")
             .when(F.col("driver_age") < 45, "35-44")
             .when(F.col("driver_age") < 55, "45-54")
             .otherwise("55+")
        )
        .withColumn(
            "is_same_city",
            F.when(F.col("driver_city") == F.col("restaurant_city"), True).otherwise(False)
        )
        .withColumn(
            "processed_time",
            F.current_timestamp()
        )
    )

    return enriched
