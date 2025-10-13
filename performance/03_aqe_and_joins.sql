-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🧠 Performance Tuning Demo - Stage 3 & 4: AQE + Join Optimization
-- MAGIC
-- MAGIC **Techniques**:
-- MAGIC - **Stage 3**: Adaptive Query Execution (AQE)
-- MAGIC - **Stage 4**: Broadcast joins + Skew handling
-- MAGIC
-- MAGIC **Expected Improvement**: Additional 5-10x speedup (cumulative 25-100x vs baseline)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🚀 Stage 3: Enable Adaptive Query Execution (AQE)
-- MAGIC
-- MAGIC **What is AQE?**
-- MAGIC - Runtime query optimization (decisions made during execution)
-- MAGIC - Introduced in Spark 3.0, enhanced in Databricks
-- MAGIC
-- MAGIC **What AQE Does**:
-- MAGIC 1. **Dynamically coalesce partitions** - Reduce shuffle partitions
-- MAGIC 2. **Dynamically switch join strategies** - Broadcast small tables at runtime
-- MAGIC 3. **Optimize skewed joins** - Split skewed partitions automatically
-- MAGIC 4. **Eliminate redundant work** - Skip unnecessary shuffles

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Enable AQE Configuration

-- COMMAND ----------

-- Enable Adaptive Query Execution
SET spark.sql.adaptive.enabled = true;

-- Enable dynamic partition coalescing
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.coalescePartitions.minPartitionNum = 1;
SET spark.sql.adaptive.coalescePartitions.initialPartitionNum = 200;

-- Enable dynamic join strategy switching
SET spark.sql.adaptive.autoBroadcastJoinThreshold = 10MB;

-- Enable skew join optimization
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB;
SET spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5;

-- Enable local shuffle reader
SET spark.sql.adaptive.localShuffleReader.enabled = true;

-- Enable dynamic file pruning (already enabled from Stage 2)
SET spark.databricks.optimizer.dynamicFilePruning = true;

-- 🎤 DEMO TIP: "These settings let Spark optimize queries during execution!"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 1 with AQE

-- COMMAND ----------

-- ⏰ START TIMER - Compare to Stage 2 time

SELECT
  r.cuisine_type,
  r.city_normalized as city,
  r.data_source,
  COUNT(DISTINCT o.order_id) as total_orders,
  COUNT(DISTINCT o.customer_cpf) as unique_customers,
  COUNT(DISTINCT o.driver_license) as unique_drivers,
  ROUND(AVG(o.order_amount), 2) as avg_order_value,
  ROUND(SUM(o.order_amount), 2) as total_revenue,
  ROUND(SUM(o.order_amount) / COUNT(DISTINCT o.order_id), 2) as revenue_per_order,
  ROUND(AVG(
    CASE
      WHEN s.final_status_timestamp IS NOT NULL
      THEN DATEDIFF(SECOND, o.order_timestamp, s.final_status_timestamp) / 60.0
    END
  ), 2) as avg_delivery_minutes,
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
  ) s ON o.order_id = s.order_id
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
-- 🎤 DEMO TIP: Expected 4-6 seconds (was 10-15s in Stage 2)
-- Click "View Query Profile" → Show:
--   - "AdaptiveSparkPlan"
--   - "BroadcastHashJoin" (Spark auto-chose broadcast!)
--   - Partition coalescing (200 → ~20)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### See AQE in Action: EXPLAIN

-- COMMAND ----------

-- Show execution plan with AQE
EXPLAIN EXTENDED
SELECT
  r.cuisine_type,
  COUNT(*) as restaurant_count,
  SUM(o.order_amount) as total_revenue
FROM semana.default.silver_orders o
  INNER JOIN semana.default.silver_restaurants r
    ON o.restaurant_cnpj = r.cnpj
WHERE
  o.order_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY r.cuisine_type;

-- 🎤 DEMO TIP: Look for:
-- - "AdaptiveSparkPlan"
-- - "BroadcastExchange" or "DynamicBroadcastHashJoin"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🎯 Stage 4: Explicit Join Optimization
-- MAGIC
-- MAGIC **Why explicit hints?**
-- MAGIC - Give Spark certainty (vs guessing with statistics)
-- MAGIC - Control exact join strategy
-- MAGIC - Handle edge cases AQE might miss

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Technique 1: Broadcast Small Dimensions

-- COMMAND ----------

-- ⏰ START TIMER

-- Force broadcast for small tables (restaurants ~10K rows, drivers ~5K rows)
SELECT
  r.cuisine_type,
  r.city_normalized as city,
  COUNT(DISTINCT o.order_id) as total_orders,
  COUNT(DISTINCT o.customer_cpf) as unique_customers,
  ROUND(SUM(o.order_amount), 2) as total_revenue
FROM semana.default.silver_orders o
  INNER JOIN semana.default.silver_restaurants /*+ BROADCAST(r) */ r
    ON o.restaurant_cnpj = r.cnpj
  INNER JOIN semana.default.silver_drivers /*+ BROADCAST(d) */ d
    ON o.driver_license = d.driver_license
WHERE
  o.order_timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY
  r.cuisine_type,
  r.city_normalized
ORDER BY
  total_revenue DESC
LIMIT 100;

-- ⏰ END TIMER
-- 🎤 DEMO TIP: Expected 1-3 seconds
-- Broadcast = Replicate small table to all executors (NO shuffle!)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Technique 2: Query Rewrite for Correlated Subqueries

-- COMMAND ----------

-- ORIGINAL (SLOW): Correlated subquery from Query 3
-- This executes the subquery for EACH customer!

-- ❌ AVOID THIS PATTERN:
/*
SELECT
  u.customer_cpf,
  (
    SELECT r.cuisine_type
    FROM silver_orders o2
      INNER JOIN silver_restaurants r ON o2.restaurant_cnpj = r.cnpj
    WHERE o2.customer_cpf = u.customer_cpf  -- Correlated!
    GROUP BY r.cuisine_type
    ORDER BY COUNT(*) DESC
    LIMIT 1
  ) as favorite_cuisine
FROM silver_users u
*/

-- ✅ OPTIMIZED: Use window functions instead
SELECT
  u.customer_cpf,
  u.customer_name,
  u.city,
  u.age_group,
  cuisine_rank.cuisine_type as favorite_cuisine,
  cuisine_rank.cuisine_order_count
FROM semana.default.silver_users u
  LEFT JOIN (
    SELECT
      o.customer_cpf,
      r.cuisine_type,
      COUNT(*) as cuisine_order_count,
      ROW_NUMBER() OVER (
        PARTITION BY o.customer_cpf
        ORDER BY COUNT(*) DESC
      ) as rn
    FROM semana.default.silver_orders o
      INNER JOIN semana.default.silver_restaurants /*+ BROADCAST(r) */ r
        ON o.restaurant_cnpj = r.cnpj
    WHERE o.order_timestamp >= CURRENT_DATE() - INTERVAL 60 DAYS
    GROUP BY o.customer_cpf, r.cuisine_type
  ) cuisine_rank
    ON u.customer_cpf = cuisine_rank.customer_cpf
   AND cuisine_rank.rn = 1
WHERE
  u.customer_cpf IN (
    SELECT DISTINCT customer_cpf
    FROM semana.default.silver_orders
    WHERE order_timestamp >= CURRENT_DATE() - INTERVAL 60 DAYS
  )
LIMIT 100;

-- 🎤 DEMO TIP: This is 10-50x faster than correlated subquery!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Technique 3: Skew Join Optimization
-- MAGIC
-- MAGIC **Problem**: Some restaurants have 1000x more orders than others
-- MAGIC **Result**: One executor does all the work (bottleneck)
-- MAGIC **Solution**: AQE automatically splits skewed partitions

-- COMMAND ----------

-- Identify skewed keys
SELECT
  restaurant_cnpj,
  COUNT(*) as order_count
FROM semana.default.silver_orders
WHERE order_timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY restaurant_cnpj
ORDER BY order_count DESC
LIMIT 20;

-- 🎤 DEMO TIP: Top restaurant might have 10K+ orders, bottom has 10

-- COMMAND ----------

-- Query that benefits from skew join optimization
SELECT
  o.restaurant_cnpj,
  r.restaurant_name,
  r.cuisine_type,
  COUNT(*) as order_count,
  SUM(o.order_amount) as total_revenue,
  AVG(o.order_amount) as avg_order_value
FROM semana.default.silver_orders o
  INNER JOIN semana.default.silver_restaurants r
    ON o.restaurant_cnpj = r.cnpj
WHERE
  o.order_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY
  o.restaurant_cnpj,
  r.restaurant_name,
  r.cuisine_type
ORDER BY
  order_count DESC
LIMIT 100;

-- 🎤 DEMO TIP: Check Query Profile for "OptimizeSkewedJoin"
-- AQE detected skew and split large partitions automatically

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Technique 4: Partition Pruning
-- MAGIC
-- MAGIC **Ensure queries use partition columns for filtering**

-- COMMAND ----------

-- Check if table is partitioned
DESCRIBE EXTENDED semana.default.silver_orders;

-- Look for "Partition Provider" in output

-- COMMAND ----------

-- If not partitioned, consider partitioning for time-series queries
-- Example: Partition by order_date for faster date-range queries

-- Note: Your current tables might not be partitioned
-- This is OK for tables under 1TB
-- For larger tables, consider:

/*
CREATE TABLE silver_orders_partitioned
USING DELTA
PARTITIONED BY (order_date)
AS SELECT
  *,
  DATE(order_timestamp) as order_date
FROM silver_orders;
*/

-- 🎤 DEMO TIP: Partitioning helps with very large tables (>1TB)
-- For your demo dataset, Z-ORDERING is sufficient

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📊 Performance Comparison: AQE vs No AQE

-- COMMAND ----------

-- Compare query with and without AQE
-- First, disable AQE
SET spark.sql.adaptive.enabled = false;

-- Run simple aggregation
SELECT
  cuisine_type,
  COUNT(*) as order_count,
  SUM(order_amount) as revenue
FROM semana.default.silver_orders o
  JOIN semana.default.silver_restaurants r
    ON o.restaurant_cnpj = r.cnpj
WHERE order_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY cuisine_type;

-- Note the time

-- COMMAND ----------

-- Now enable AQE
SET spark.sql.adaptive.enabled = true;

-- Run same query
SELECT
  cuisine_type,
  COUNT(*) as order_count,
  SUM(order_amount) as revenue
FROM semana.default.silver_orders o
  JOIN semana.default.silver_restaurants r
    ON o.restaurant_cnpj = r.cnpj
WHERE order_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY cuisine_type;

-- 🎤 DEMO TIP: AQE version is 2-3x faster!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🎯 Best Practices: Join Optimization

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ✅ DO: Broadcast Small Tables
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- Good: Dimension tables < 100MB
-- MAGIC SELECT *
-- MAGIC FROM large_fact_table f
-- MAGIC   JOIN small_dimension_table /*+ BROADCAST(d) */ d
-- MAGIC     ON f.key = d.key;
-- MAGIC ```
-- MAGIC
-- MAGIC ### ❌ DON'T: Broadcast Large Tables
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- Bad: Table is 10GB
-- MAGIC SELECT *
-- MAGIC FROM table1 t1
-- MAGIC   JOIN table2 /*+ BROADCAST(t2) */ t2  -- t2 is 10GB!
-- MAGIC     ON t1.key = t2.key;
-- MAGIC
-- MAGIC -- Result: OOM (Out of Memory) errors
-- MAGIC ```
-- MAGIC
-- MAGIC ### ✅ DO: Filter Before Join
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- Good: Filter reduces data before join
-- MAGIC SELECT *
-- MAGIC FROM (
-- MAGIC   SELECT * FROM orders
-- MAGIC   WHERE order_date >= '2024-01-01'  -- Filter first!
-- MAGIC ) o
-- MAGIC JOIN restaurants r ON o.restaurant_id = r.id;
-- MAGIC ```
-- MAGIC
-- MAGIC ### ❌ DON'T: Filter After Join
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- Bad: Join processes all historical data first
-- MAGIC SELECT *
-- MAGIC FROM orders o
-- MAGIC JOIN restaurants r ON o.restaurant_id = r.id
-- MAGIC WHERE o.order_date >= '2024-01-01';  -- Too late!
-- MAGIC ```
-- MAGIC
-- MAGIC ### ✅ DO: Use Bucketing for Large-Large Joins
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- For large table joins (both > 1TB)
-- MAGIC CREATE TABLE orders_bucketed
-- MAGIC USING DELTA
-- MAGIC CLUSTERED BY (restaurant_id) INTO 100 BUCKETS;
-- MAGIC
-- MAGIC CREATE TABLE order_items_bucketed
-- MAGIC USING DELTA
-- MAGIC CLUSTERED BY (restaurant_id) INTO 100 BUCKETS;
-- MAGIC
-- MAGIC -- Join is now much faster (co-located data)
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🔍 Query Profile Analysis Tips

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What to Look For in Query Profile
-- MAGIC
-- MAGIC **Good Signs** ✅:
-- MAGIC - `BroadcastHashJoin` for small tables
-- MAGIC - `AdaptiveSparkPlan` appears in execution tree
-- MAGIC - Low shuffle read/write bytes
-- MAGIC - High data skipping ratio
-- MAGIC - Short duration per task
-- MAGIC
-- MAGIC **Bad Signs** ❌:
-- MAGIC - `SortMergeJoin` when one table is small
-- MAGIC - Large shuffle operations (GB+)
-- MAGIC - Skewed task duration (1 task takes 10x longer)
-- MAGIC - High spill to disk
-- MAGIC - Reading many unnecessary files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📈 Stage 3 & 4 Summary
-- MAGIC
-- MAGIC **What We Enabled**:
-- MAGIC - ✅ Adaptive Query Execution (AQE)
-- MAGIC - ✅ Dynamic partition coalescing
-- MAGIC - ✅ Skew join optimization
-- MAGIC - ✅ Broadcast hints for small tables
-- MAGIC - ✅ Correlated subquery rewrite
-- MAGIC
-- MAGIC **Performance Impact**:
-- MAGIC - Stage 3 (AQE): 2-3x improvement
-- MAGIC - Stage 4 (Joins): 2-5x improvement
-- MAGIC - **Cumulative**: 20-50x vs baseline!
-- MAGIC
-- MAGIC **Key Takeaway**:
-- MAGIC > "AQE + broadcast joins transform slow queries into fast queries automatically"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🎯 Next Steps
-- MAGIC
-- MAGIC Ready for the final optimizations? Move to:
-- MAGIC
-- MAGIC **📓 Notebook 04: Expert Techniques**
-- MAGIC - Photon engine
-- MAGIC - Range join optimization
-- MAGIC - Query result caching
-- MAGIC - Bloom filter indexes
-- MAGIC
-- MAGIC **Expected**: Additional 2-3x improvement → **Total 60-150x vs baseline!** 🚀
