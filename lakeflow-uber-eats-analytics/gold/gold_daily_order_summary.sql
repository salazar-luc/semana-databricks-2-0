-- ============================================================================
-- GOLD LAYER: Daily Order Summary Analytics
-- ============================================================================
-- Purpose: Business-ready daily metrics for dashboards and reporting
-- Source: silver_orders_enriched
-- Target: gold_daily_order_summary
-- Demo: Shows aggregations, KPIs, and business metrics
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_order_summary(
  -- Strict validation: FAIL if metrics are invalid (prevents bad data in reports)
  CONSTRAINT valid_metrics EXPECT (total_orders > 0 AND total_revenue > 0) ON VIOLATION FAIL UPDATE
)
COMMENT "Daily order metrics including volume, revenue, and operational KPIs. Ready for BI dashboards."
TBLPROPERTIES (
  "quality" = "gold",
  "layer" = "business_analytics",
  "pipelines.autoOptimize.zOrderCols" = "order_date"
)
AS SELECT
  -- === Date Dimension ===
  DATE(order_date) AS order_date,

  -- === Volume Metrics ===
  COUNT(order_id) AS total_orders,
  COUNT(DISTINCT user_key) AS unique_customers,
  COUNT(DISTINCT driver_key) AS unique_drivers,
  COUNT(DISTINCT restaurant_key) AS unique_restaurants,

  -- === Revenue Metrics ===
  SUM(total_amount) AS total_revenue,
  AVG(total_amount) AS avg_order_value,
  MIN(total_amount) AS min_order_value,
  MAX(total_amount) AS max_order_value,
  STDDEV(total_amount) AS stddev_order_value,

  -- === Order Distribution by Amount Category ===
  SUM(CASE WHEN amount_category = 'High' THEN 1 ELSE 0 END) AS orders_high_value,
  SUM(CASE WHEN amount_category = 'Medium' THEN 1 ELSE 0 END) AS orders_medium_value,
  SUM(CASE WHEN amount_category = 'Low' THEN 1 ELSE 0 END) AS orders_low_value,

  -- === Time Distribution ===
  SUM(CASE WHEN time_of_day = 'Morning' THEN 1 ELSE 0 END) AS orders_morning,
  SUM(CASE WHEN time_of_day = 'Afternoon' THEN 1 ELSE 0 END) AS orders_afternoon,
  SUM(CASE WHEN time_of_day = 'Evening' THEN 1 ELSE 0 END) AS orders_evening,
  SUM(CASE WHEN time_of_day = 'Night' THEN 1 ELSE 0 END) AS orders_night,

  -- === Peak Hours Performance ===
  SUM(CASE WHEN is_peak_hour = TRUE THEN 1 ELSE 0 END) AS orders_peak_hours,
  SUM(CASE WHEN is_peak_hour = TRUE THEN total_amount ELSE 0 END) AS revenue_peak_hours,

  -- === Weekend vs Weekday ===
  FIRST(is_weekend) AS is_weekend,
  FIRST(day_name) AS day_name,

  -- === Calculated KPIs ===
  ROUND(SUM(total_amount) / COUNT(order_id), 2) AS revenue_per_order,
  ROUND((SUM(CASE WHEN amount_category = 'High' THEN 1 ELSE 0 END) / COUNT(order_id)) * 100, 2) AS high_value_order_pct,
  ROUND((SUM(CASE WHEN is_peak_hour = TRUE THEN 1 ELSE 0 END) / COUNT(order_id)) * 100, 2) AS peak_hour_order_pct,
  ROUND((SUM(CASE WHEN is_peak_hour = TRUE THEN total_amount ELSE 0 END) / SUM(total_amount)) * 100, 2) AS peak_hour_revenue_pct,

  -- === Per-Entity KPIs ===
  ROUND(SUM(total_amount) / COUNT(DISTINCT user_key), 2) AS avg_revenue_per_customer,
  ROUND(SUM(total_amount) / COUNT(DISTINCT driver_key), 2) AS avg_revenue_per_driver,
  ROUND(SUM(total_amount) / COUNT(DISTINCT restaurant_key), 2) AS avg_revenue_per_restaurant,
  ROUND(COUNT(order_id) / COUNT(DISTINCT user_key), 2) AS orders_per_customer,
  ROUND(COUNT(order_id) / COUNT(DISTINCT driver_key), 2) AS orders_per_driver,

  -- === Date Dimensions for BI Tools ===
  YEAR(order_date) AS year,
  MONTH(order_date) AS month,
  QUARTER(order_date) AS quarter,
  DAY(order_date) AS day_of_month,
  WEEKOFYEAR(order_date) AS week_of_year,

  -- === Month Name for Reports ===
  CASE MONTH(order_date)
    WHEN 1 THEN 'January'
    WHEN 2 THEN 'February'
    WHEN 3 THEN 'March'
    WHEN 4 THEN 'April'
    WHEN 5 THEN 'May'
    WHEN 6 THEN 'June'
    WHEN 7 THEN 'July'
    WHEN 8 THEN 'August'
    WHEN 9 THEN 'September'
    WHEN 10 THEN 'October'
    WHEN 11 THEN 'November'
    ELSE 'December'
  END AS month_name,

  -- === Processing Metadata ===
  MAX(processed_time) AS last_silver_processed_time,
  CURRENT_TIMESTAMP() AS computed_time,
  '1.0' AS gold_layer_version

FROM silver_orders_enriched
GROUP BY DATE(order_date);
