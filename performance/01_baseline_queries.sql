-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🐌 Performance Tuning Demo - Stage 1: Baseline (Slow)
-- MAGIC
-- MAGIC **Purpose**: Establish baseline performance with ZERO optimizations
-- MAGIC
-- MAGIC **What we'll show**:
-- MAGIC - Complex analytical queries on UberEats data
-- MAGIC - No statistics, no Z-ordering, no AQE
-- MAGIC - Query profiles showing inefficiencies
-- MAGIC
-- MAGIC **Expected**: 60-120 second query times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup: Disable All Optimizations

-- COMMAND ----------

-- Turn OFF all optimizations for true baseline
SET spark.sql.adaptive.enabled = false;
SET spark.databricks.optimizer.dynamicFilePruning = false;
SET spark.sql.statistics.fallBackToHdfs = false;
SET spark.databricks.photon.enabled = false;

-- Verify current configuration
SET -v;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Baseline Query 1: Restaurant Performance Analysis
-- MAGIC
-- MAGIC **Business Question**: Which restaurants perform best by cuisine and city?
-- MAGIC
-- MAGIC **Complexity**:
-- MAGIC - 4-table join (orders, restaurants, drivers, status)
-- MAGIC - Multiple aggregations
-- MAGIC - Window functions
-- MAGIC - 30 days of data
-- MAGIC
-- MAGIC **Expected Time**: 90-120 seconds

-- COMMAND ----------

-- ⏰ START TIMER: Note the start time

SELECT
  r.cuisine_type,
  r.city_normalized as city,
  r.data_source,

  -- Order metrics
  COUNT(DISTINCT o.order_id) as total_orders,
  COUNT(DISTINCT o.customer_cpf) as unique_customers,
  COUNT(DISTINCT o.driver_license) as unique_drivers,

  -- Revenue metrics
  ROUND(AVG(o.order_amount), 2) as avg_order_value,
  ROUND(SUM(o.order_amount), 2) as total_revenue,
  ROUND(SUM(o.order_amount) / COUNT(DISTINCT o.order_id), 2) as revenue_per_order,

  -- Delivery metrics
  ROUND(AVG(
    CASE
      WHEN s.final_status_timestamp IS NOT NULL
      THEN DATEDIFF(SECOND, o.order_timestamp, s.final_status_timestamp) / 60.0
    END
  ), 2) as avg_delivery_minutes,

  -- High-value order percentage
  ROUND(
    100.0 * SUM(CASE WHEN o.order_amount >= 100 THEN 1 ELSE 0 END) / COUNT(*),
    2
  ) as high_value_order_pct

FROM semana.default.silver_orders o

  -- Join restaurants (dimension table ~10K rows)
  INNER JOIN semana.default.silver_restaurants r
    ON o.restaurant_cnpj = r.cnpj

  -- Join drivers (dimension table ~5K rows)
  INNER JOIN semana.default.silver_drivers d
    ON o.driver_license = d.driver_license

  -- Join status timeline (subquery adds complexity)
  LEFT JOIN (
    SELECT
      order_id,
      MAX(status_timestamp) as final_status_timestamp
    FROM semana.default.silver_status
    WHERE status_name = 'delivered'
    GROUP BY order_id
  ) s
    ON o.order_id = s.order_id

WHERE
  o.order_timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS

GROUP BY
  r.cuisine_type,
  r.city_normalized,
  r.data_source

HAVING
  COUNT(DISTINCT o.order_id) >= 100  -- Only active restaurants

ORDER BY
  total_revenue DESC

LIMIT 100;

-- ⏰ END TIMER: Note the end time
-- 🎤 DEMO TIP: Click "View Query Profile" and show:
--    - Large shuffle operations
--    - No broadcast joins
--    - Files scanned vs files needed
--    - Partition count

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Baseline Query 2: Driver Efficiency Rankings
-- MAGIC
-- MAGIC **Business Question**: Which drivers are most efficient by vehicle type?
-- MAGIC
-- MAGIC **Complexity**:
-- MAGIC - Window functions (RANK)
-- MAGIC - Multiple aggregations
-- MAGIC - 90 days of data
-- MAGIC
-- MAGIC **Expected Time**: 60-90 seconds

-- COMMAND ----------

-- ⏰ START TIMER

SELECT
  d.driver_license,
  d.driver_name,
  d.vehicle_type,
  d.city as driver_city,

  -- Delivery metrics
  COUNT(DISTINCT o.order_id) as total_deliveries,
  COUNT(DISTINCT o.restaurant_cnpj) as restaurants_served,
  COUNT(DISTINCT o.customer_cpf) as customers_served,
  COUNT(DISTINCT DATE(o.order_timestamp)) as active_days,

  -- Revenue metrics
  ROUND(SUM(o.order_amount), 2) as total_revenue_generated,
  ROUND(AVG(o.order_amount), 2) as avg_order_value,
  ROUND(SUM(o.order_amount) / COUNT(DISTINCT o.order_id), 2) as revenue_per_delivery,

  -- Efficiency metrics
  ROUND(
    COUNT(DISTINCT o.order_id) /
    NULLIF(DATEDIFF(DAY, MIN(o.order_timestamp), MAX(o.order_timestamp)) + 1, 0),
    2
  ) as deliveries_per_day,

  -- Diversity score
  ROUND(
    (COUNT(DISTINCT o.restaurant_cnpj) * 1.0 / NULLIF(COUNT(DISTINCT o.order_id), 0)) * 100,
    2
  ) as restaurant_diversity_pct,

  -- Ranking within vehicle type (causes shuffle)
  RANK() OVER (
    PARTITION BY d.vehicle_type
    ORDER BY SUM(o.order_amount) DESC
  ) as revenue_rank_in_vehicle_type,

  RANK() OVER (
    PARTITION BY d.vehicle_type
    ORDER BY COUNT(DISTINCT o.order_id) DESC
  ) as delivery_rank_in_vehicle_type

FROM semana.default.silver_orders o

  INNER JOIN semana.default.silver_drivers d
    ON o.driver_license = d.driver_license

WHERE
  o.order_timestamp >= CURRENT_DATE() - INTERVAL 90 DAYS

GROUP BY
  d.driver_license,
  d.driver_name,
  d.vehicle_type,
  d.city

HAVING
  COUNT(DISTINCT o.order_id) >= 50  -- Active drivers only

ORDER BY
  total_revenue_generated DESC

LIMIT 100;

-- ⏰ END TIMER
-- 🎤 DEMO TIP: Show query profile focusing on:
--    - Window function causing extra shuffle
--    - Sort operations for RANK
--    - Data spill to disk (if happening)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Baseline Query 3: Customer Behavior Patterns
-- MAGIC
-- MAGIC **Business Question**: Analyze customer ordering patterns and preferences
-- MAGIC
-- MAGIC **Complexity**:
-- MAGIC - Self-join on orders for repeat customer detection
-- MAGIC - Multiple complex aggregations
-- MAGIC - Array aggregations
-- MAGIC
-- MAGIC **Expected Time**: 70-100 seconds

-- COMMAND ----------

-- ⏰ START TIMER

SELECT
  u.customer_cpf,
  u.customer_name,
  u.city as customer_city,
  u.age_group,

  -- Order frequency
  COUNT(DISTINCT o.order_id) as total_orders,
  COUNT(DISTINCT DATE(o.order_timestamp)) as active_days,
  ROUND(
    COUNT(DISTINCT o.order_id) * 1.0 /
    NULLIF(DATEDIFF(DAY, MIN(o.order_timestamp), MAX(o.order_timestamp)) + 1, 0),
    2
  ) as orders_per_day,

  -- Spending patterns
  ROUND(SUM(o.order_amount), 2) as total_spent,
  ROUND(AVG(o.order_amount), 2) as avg_order_value,
  ROUND(MIN(o.order_amount), 2) as min_order_value,
  ROUND(MAX(o.order_amount), 2) as max_order_value,

  -- Restaurant preferences
  COUNT(DISTINCT o.restaurant_cnpj) as unique_restaurants,
  ROUND(
    COUNT(DISTINCT o.restaurant_cnpj) * 100.0 / NULLIF(COUNT(DISTINCT o.order_id), 0),
    2
  ) as restaurant_variety_pct,

  -- Favorite cuisine (requires expensive subquery)
  (
    SELECT r.cuisine_type
    FROM semana.default.silver_orders o2
      INNER JOIN semana.default.silver_restaurants r
        ON o2.restaurant_cnpj = r.cnpj
    WHERE o2.customer_cpf = u.customer_cpf
      AND o2.order_timestamp >= CURRENT_DATE() - INTERVAL 60 DAYS
    GROUP BY r.cuisine_type
    ORDER BY COUNT(*) DESC
    LIMIT 1
  ) as favorite_cuisine,

  -- Loyalty indicator
  CASE
    WHEN COUNT(DISTINCT o.order_id) >= 50 THEN 'VIP'
    WHEN COUNT(DISTINCT o.order_id) >= 20 THEN 'Loyal'
    WHEN COUNT(DISTINCT o.order_id) >= 10 THEN 'Regular'
    ELSE 'Occasional'
  END as customer_segment

FROM semana.default.silver_users u

  INNER JOIN semana.default.silver_orders o
    ON u.customer_cpf = o.customer_cpf

WHERE
  o.order_timestamp >= CURRENT_DATE() - INTERVAL 60 DAYS

GROUP BY
  u.customer_cpf,
  u.customer_name,
  u.city,
  u.age_group

HAVING
  COUNT(DISTINCT o.order_id) >= 5  -- Active customers only

ORDER BY
  total_spent DESC

LIMIT 100;

-- ⏰ END TIMER
-- 🎤 DEMO TIP: This query is SLOW because of:
--    - Correlated subquery (favorite_cuisine)
--    - Multiple passes over the same data
--    - No statistics to optimize join order

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Baseline Query 4: Time-Based Order Patterns
-- MAGIC
-- MAGIC **Business Question**: When do orders peak? Hourly patterns by day of week
-- MAGIC
-- MAGIC **Complexity**:
-- MAGIC - Date/time extractions
-- MAGIC - Multiple grouping dimensions
-- MAGIC - Percentage calculations
-- MAGIC
-- MAGIC **Expected Time**: 40-60 seconds

-- COMMAND ----------

-- ⏰ START TIMER

SELECT
  DAYOFWEEK(o.order_timestamp) as day_of_week,
  CASE DAYOFWEEK(o.order_timestamp)
    WHEN 1 THEN 'Sunday'
    WHEN 2 THEN 'Monday'
    WHEN 3 THEN 'Tuesday'
    WHEN 4 THEN 'Wednesday'
    WHEN 5 THEN 'Thursday'
    WHEN 6 THEN 'Friday'
    WHEN 7 THEN 'Saturday'
  END as day_name,
  HOUR(o.order_timestamp) as hour_of_day,

  -- Volume metrics
  COUNT(DISTINCT o.order_id) as total_orders,
  COUNT(DISTINCT o.customer_cpf) as unique_customers,

  -- Revenue metrics
  ROUND(SUM(o.order_amount), 2) as total_revenue,
  ROUND(AVG(o.order_amount), 2) as avg_order_value,

  -- Percentage of daily volume
  ROUND(
    100.0 * COUNT(DISTINCT o.order_id) /
    SUM(COUNT(DISTINCT o.order_id)) OVER (PARTITION BY DAYOFWEEK(o.order_timestamp)),
    2
  ) as pct_of_day_volume,

  -- Driver utilization
  COUNT(DISTINCT o.driver_license) as active_drivers,
  ROUND(
    COUNT(DISTINCT o.order_id) * 1.0 / NULLIF(COUNT(DISTINCT o.driver_license), 0),
    2
  ) as orders_per_driver

FROM semana.default.silver_orders o

WHERE
  o.order_timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS

GROUP BY
  DAYOFWEEK(o.order_timestamp),
  HOUR(o.order_timestamp)

ORDER BY
  day_of_week,
  hour_of_day;

-- ⏰ END TIMER
-- 🎤 DEMO TIP: Window function over large partition causes shuffle

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📊 Baseline Performance Summary
-- MAGIC
-- MAGIC Record these baseline metrics before moving to Stage 2:

-- COMMAND ----------

-- Create results table
CREATE OR REPLACE TABLE semana.default.performance_demo_baseline AS
SELECT
  'Query 1: Restaurant Performance' as query_name,
  NULL as execution_time_seconds,  -- Fill in manually from your runs
  NULL as rows_returned,
  NULL as bytes_scanned,
  NULL as files_scanned,
  'No optimizations' as notes
UNION ALL
SELECT
  'Query 2: Driver Efficiency',
  NULL,
  NULL,
  NULL,
  NULL,
  'No optimizations'
UNION ALL
SELECT
  'Query 3: Customer Behavior',
  NULL,
  NULL,
  NULL,
  NULL,
  'Correlated subquery - very slow'
UNION ALL
SELECT
  'Query 4: Time Patterns',
  NULL,
  NULL,
  NULL,
  NULL,
  'Window function shuffle';

-- View the template
SELECT * FROM semana.default.performance_demo_baseline;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🎯 Next Steps
-- MAGIC
-- MAGIC You've established the baseline! Now move to:
-- MAGIC
-- MAGIC **📓 Notebook 02: Quick Wins** (ANALYZE + Z-ORDER)
-- MAGIC - Expected: 5-10x speedup
-- MAGIC - Time investment: 5 minutes
-- MAGIC
-- MAGIC **Key Takeaways from Baseline**:
-- MAGIC - ❌ No table statistics → poor join ordering
-- MAGIC - ❌ No Z-ordering → scanning unnecessary files
-- MAGIC - ❌ No broadcast hints → large shuffles
-- MAGIC - ❌ AQE disabled → no runtime optimizations
-- MAGIC - ❌ Correlated subqueries → multiple passes
-- MAGIC
-- MAGIC **The good news**: All of these are fixable! 🚀
