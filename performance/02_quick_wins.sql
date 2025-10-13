-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ⚡ Performance Tuning Demo - Stage 2: Quick Wins
-- MAGIC
-- MAGIC **Goal**: Achieve 5-10x speedup with TWO simple commands
-- MAGIC
-- MAGIC **Techniques**:
-- MAGIC 1. `ANALYZE TABLE` - Collect statistics
-- MAGIC 2. `OPTIMIZE + ZORDER` - Improve data layout
-- MAGIC
-- MAGIC **Time Investment**: ~5 minutes
-- MAGIC **Expected Improvement**: 5-10x faster queries

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📊 Step 1: Collect Table Statistics
-- MAGIC
-- MAGIC **Why**: Spark's optimizer needs statistics to:
-- MAGIC - Choose optimal join strategies
-- MAGIC - Estimate result sizes
-- MAGIC - Decide broadcast vs shuffle
-- MAGIC - Order multi-way joins efficiently

-- COMMAND ----------

-- Check current statistics
DESCRIBE EXTENDED semana.default.silver_orders;

-- Look for "Statistics" row - likely shows "UNKNOWN" or missing

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Collect Statistics for All Tables

-- COMMAND ----------

-- Collect comprehensive statistics
-- This analyzes data distribution, cardinality, nulls, min/max values

ANALYZE TABLE semana.default.silver_orders
  COMPUTE STATISTICS FOR ALL COLUMNS;

ANALYZE TABLE semana.default.silver_restaurants
  COMPUTE STATISTICS FOR ALL COLUMNS;

ANALYZE TABLE semana.default.silver_drivers
  COMPUTE STATISTICS FOR ALL COLUMNS;

ANALYZE TABLE semana.default.silver_users
  COMPUTE STATISTICS FOR ALL COLUMNS;

ANALYZE TABLE semana.default.silver_status
  COMPUTE STATISTICS FOR ALL COLUMNS;

-- 🎤 DEMO TIP: Each command takes 20-40 seconds
-- Explain: "Spark is now analyzing data patterns..."

-- COMMAND ----------

-- Verify statistics were collected
DESCRIBE EXTENDED semana.default.silver_orders;

-- Now you should see detailed statistics!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🎯 Step 2: Z-ORDER Optimization
-- MAGIC
-- MAGIC **What is Z-ORDERING?**
-- MAGIC - Co-locates related data in the same files
-- MAGIC - Enables data skipping (skip entire files)
-- MAGIC - Like organizing a library by topic instead of random order
-- MAGIC
-- MAGIC **Best Practices**:
-- MAGIC - Choose 2-4 frequently filtered columns
-- MAGIC - Prioritize high-cardinality columns
-- MAGIC - Order matters: most selective first

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Check Current File Layout

-- COMMAND ----------

-- See current file statistics
DESCRIBE DETAIL semana.default.silver_orders;

-- Note the number of files and average file size

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Optimize Orders Table
-- MAGIC
-- MAGIC Common query patterns:
-- MAGIC - Filter by `order_timestamp` (time-based queries)
-- MAGIC - Join on `restaurant_cnpj` (restaurant analytics)
-- MAGIC - Join on `driver_license` (driver analytics)

-- COMMAND ----------

-- Optimize with Z-ORDER
OPTIMIZE semana.default.silver_orders
  ZORDER BY (order_timestamp, restaurant_cnpj, driver_license);

-- 🎤 DEMO TIP: This takes 1-2 minutes
-- Explain: "Reorganizing data files for optimal access..."
-- Show the output: files compacted, bytes rewritten

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Optimize Restaurants Table
-- MAGIC
-- MAGIC Common filters: `city`, `cuisine_type`

-- COMMAND ----------

OPTIMIZE semana.default.silver_restaurants
  ZORDER BY (city_normalized, cuisine_type);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Optimize Drivers Table
-- MAGIC
-- MAGIC Common filters: `vehicle_type`, `city`

-- COMMAND ----------

OPTIMIZE semana.default.silver_drivers
  ZORDER BY (vehicle_type, city);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Optimize Users Table

-- COMMAND ----------

OPTIMIZE semana.default.silver_users
  ZORDER BY (city, age_group);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Optimize Status Table
-- MAGIC
-- MAGIC Common join: `order_id`, filter: `status_timestamp`

-- COMMAND ----------

OPTIMIZE semana.default.silver_status
  ZORDER BY (order_id, status_timestamp);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📈 Step 3: Re-run Baseline Queries
-- MAGIC
-- MAGIC Now let's see the improvement!

-- COMMAND ----------

-- Keep optimizations OFF for now (we'll enable AQE in Stage 3)
SET spark.sql.adaptive.enabled = false;
SET spark.databricks.optimizer.dynamicFilePruning = true;  -- Enable file pruning to leverage Z-ORDER

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 1: Restaurant Performance (Optimized)

-- COMMAND ----------

-- ⏰ START TIMER - Compare to baseline time

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

  INNER JOIN semana.default.silver_restaurants r
    ON o.restaurant_cnpj = r.cnpj

  INNER JOIN semana.default.silver_drivers d
    ON o.driver_license = d.driver_license

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
  COUNT(DISTINCT o.order_id) >= 100

ORDER BY
  total_revenue DESC

LIMIT 100;

-- ⏰ END TIMER
-- 🎤 DEMO TIP: Expected 10-15 seconds (was 90-120s)
-- Show Query Profile:
--   - Files scanned: MUCH fewer!
--   - Bytes scanned: Significantly reduced
--   - Data skipping enabled

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 2: Driver Efficiency (Optimized)

-- COMMAND ----------

-- ⏰ START TIMER

SELECT
  d.driver_license,
  d.driver_name,
  d.vehicle_type,
  d.city as driver_city,

  COUNT(DISTINCT o.order_id) as total_deliveries,
  COUNT(DISTINCT o.restaurant_cnpj) as restaurants_served,
  COUNT(DISTINCT o.customer_cpf) as customers_served,
  COUNT(DISTINCT DATE(o.order_timestamp)) as active_days,

  ROUND(SUM(o.order_amount), 2) as total_revenue_generated,
  ROUND(AVG(o.order_amount), 2) as avg_order_value,
  ROUND(SUM(o.order_amount) / COUNT(DISTINCT o.order_id), 2) as revenue_per_delivery,

  ROUND(
    COUNT(DISTINCT o.order_id) /
    NULLIF(DATEDIFF(DAY, MIN(o.order_timestamp), MAX(o.order_timestamp)) + 1, 0),
    2
  ) as deliveries_per_day,

  ROUND(
    (COUNT(DISTINCT o.restaurant_cnpj) * 1.0 / NULLIF(COUNT(DISTINCT o.order_id), 0)) * 100,
    2
  ) as restaurant_diversity_pct,

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
  COUNT(DISTINCT o.order_id) >= 50

ORDER BY
  total_revenue_generated DESC

LIMIT 100;

-- ⏰ END TIMER
-- Expected: 8-12 seconds (was 60-90s)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📊 Compare Performance

-- COMMAND ----------

-- Update results table with new timings
INSERT INTO semana.default.performance_demo_baseline
VALUES
  ('Query 1 - Quick Wins', NULL, NULL, NULL, NULL, 'ANALYZE + Z-ORDER applied'),
  ('Query 2 - Quick Wins', NULL, NULL, NULL, NULL, 'ANALYZE + Z-ORDER applied');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🎯 What Just Happened?
-- MAGIC
-- MAGIC **Statistics Collection** enabled:
-- MAGIC - ✅ Better join ordering (Spark knows table sizes)
-- MAGIC - ✅ Accurate cardinality estimates
-- MAGIC - ✅ Improved filter selectivity calculations
-- MAGIC
-- MAGIC **Z-ORDERING** enabled:
-- MAGIC - ✅ Data skipping (skip entire files)
-- MAGIC - ✅ Reduced files scanned (90%+ reduction)
-- MAGIC - ✅ Improved cache locality
-- MAGIC - ✅ Fewer bytes read from storage
-- MAGIC
-- MAGIC **Expected Improvement**: **5-10x faster** 🚀

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 💡 Best Practices Learned
-- MAGIC
-- MAGIC ### When to Run ANALYZE TABLE
-- MAGIC ```sql
-- MAGIC -- After major data loads
-- MAGIC COPY INTO table_name ...
-- MAGIC ANALYZE TABLE table_name COMPUTE STATISTICS FOR ALL COLUMNS;
-- MAGIC
-- MAGIC -- After DELETE/UPDATE operations
-- MAGIC DELETE FROM table_name WHERE condition;
-- MAGIC ANALYZE TABLE table_name COMPUTE STATISTICS FOR ALL COLUMNS;
-- MAGIC
-- MAGIC -- Periodic maintenance (weekly/monthly)
-- MAGIC -- Can be automated via scheduled job
-- MAGIC ```
-- MAGIC
-- MAGIC ### When to Run OPTIMIZE
-- MAGIC ```sql
-- MAGIC -- After many small writes (streaming ingestion)
-- MAGIC -- Small files hurt performance
-- MAGIC
-- MAGIC -- Check file count
-- MAGIC DESCRIBE DETAIL table_name;
-- MAGIC
-- MAGIC -- If numFiles > 1000 or avg file size < 100MB:
-- MAGIC OPTIMIZE table_name ZORDER BY (common_filter_cols);
-- MAGIC ```
-- MAGIC
-- MAGIC ### Z-ORDER Column Selection
-- MAGIC ```sql
-- MAGIC -- ✅ GOOD: Frequently filtered high-cardinality columns
-- MAGIC OPTIMIZE orders ZORDER BY (order_date, customer_id, product_id);
-- MAGIC
-- MAGIC -- ❌ BAD: Low-cardinality or too many columns
-- MAGIC OPTIMIZE orders ZORDER BY (status, region);  -- Only 2-3 distinct values
-- MAGIC OPTIMIZE orders ZORDER BY (col1, col2, col3, col4, col5, col6);  -- Too many
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🔄 Maintenance Automation
-- MAGIC
-- MAGIC Consider scheduling these as weekly jobs:

-- COMMAND ----------

-- Example maintenance job (Python)
-- MAGIC %python
-- MAGIC
-- MAGIC # List of tables to maintain
-- MAGIC tables = [
-- MAGIC     "semana.default.silver_orders",
-- MAGIC     "semana.default.silver_restaurants",
-- MAGIC     "semana.default.silver_drivers",
-- MAGIC     "semana.default.silver_users",
-- MAGIC     "semana.default.silver_status"
-- MAGIC ]
-- MAGIC
-- MAGIC # Optimize each table
-- MAGIC for table in tables:
-- MAGIC     print(f"Optimizing {table}...")
-- MAGIC
-- MAGIC     # Collect statistics
-- MAGIC     spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS")
-- MAGIC
-- MAGIC     # Optimize (table-specific Z-ORDER)
-- MAGIC     if "orders" in table:
-- MAGIC         spark.sql(f"OPTIMIZE {table} ZORDER BY (order_timestamp, restaurant_cnpj, driver_license)")
-- MAGIC     elif "restaurants" in table:
-- MAGIC         spark.sql(f"OPTIMIZE {table} ZORDER BY (city_normalized, cuisine_type)")
-- MAGIC     elif "drivers" in table:
-- MAGIC         spark.sql(f"OPTIMIZE {table} ZORDER BY (vehicle_type, city)")
-- MAGIC     elif "users" in table:
-- MAGIC         spark.sql(f"OPTIMIZE {table} ZORDER BY (city, age_group)")
-- MAGIC     elif "status" in table:
-- MAGIC         spark.sql(f"OPTIMIZE {table} ZORDER BY (order_id, status_timestamp)")
-- MAGIC
-- MAGIC     print(f"✅ {table} optimized")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🗑️ Bonus: Cleanup Old Files

-- COMMAND ----------

-- VACUUM removes files no longer needed
-- Default retention: 7 days

-- Check what would be vacuumed (dry run)
VACUUM semana.default.silver_orders RETAIN 168 HOURS DRY RUN;

-- Actually vacuum (removes old files)
VACUUM semana.default.silver_orders RETAIN 168 HOURS;

-- 🎤 DEMO TIP: VACUUM is important after OPTIMIZE
-- OPTIMIZE creates new files, old files are marked for deletion
-- VACUUM actually deletes them to save storage costs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🎯 Next Steps
-- MAGIC
-- MAGIC You've achieved **5-10x speedup**! 🎉
-- MAGIC
-- MAGIC Ready for more? Move to:
-- MAGIC
-- MAGIC **📓 Notebook 03: AQE Magic** (Adaptive Query Execution)
-- MAGIC - Expected: Additional 2-3x speedup
-- MAGIC - Time investment: 2 minutes (just enable settings!)
-- MAGIC
-- MAGIC **Current Progress**:
-- MAGIC - ✅ Statistics collected
-- MAGIC - ✅ Data layout optimized (Z-ORDER)
-- MAGIC - ✅ Files compacted
-- MAGIC - ⏭️ Next: Runtime query optimization with AQE
