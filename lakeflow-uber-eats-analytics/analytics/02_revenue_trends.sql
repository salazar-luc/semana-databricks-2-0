-- ============================================================================
-- INTERMEDIATE: Revenue Trends with Growth Analysis
-- ============================================================================
-- Skill Level: Intermediate (Window functions: LAG, AVG OVER, running totals)
-- Purpose: Track revenue performance with day-over-day comparisons and trends
-- Use Case: Revenue forecasting, trend analysis, performance monitoring
-- Visualization: Line charts with multiple series, KPI cards with change %
-- ============================================================================

SELECT
  order_date,
  day_name,
  is_weekend,

  -- Current Metrics
  total_revenue,
  total_orders,
  revenue_per_order,

  -- === Window Function 1: Day-over-Day Comparison ===
  -- LAG() retrieves previous row's value for comparison
  LAG(total_revenue, 1) OVER (ORDER BY order_date) AS prev_day_revenue,

  -- Calculate change from previous day
  total_revenue - LAG(total_revenue, 1) OVER (ORDER BY order_date) AS revenue_change,

  -- Growth percentage calculation
  ROUND(
    ((total_revenue - LAG(total_revenue, 1) OVER (ORDER BY order_date)) /
     NULLIF(LAG(total_revenue, 1) OVER (ORDER BY order_date), 0)) * 100,
    2
  ) AS revenue_growth_pct,

  -- === Window Function 2: Moving Average (Trend Smoothing) ===
  -- 7-day moving average - smooths out daily fluctuations
  ROUND(
    AVG(total_revenue) OVER (
      ORDER BY order_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ),
    2
  ) AS revenue_7day_avg,

  -- === Window Function 3: Running Total (Cumulative) ===
  -- Month-to-date cumulative revenue
  ROUND(
    SUM(total_revenue) OVER (
      PARTITION BY YEAR(order_date), MONTH(order_date)
      ORDER BY order_date
    ),
    2
  ) AS revenue_mtd,

  -- === Trend Indicator ===
  -- Compare current day to 7-day average
  CASE
    WHEN total_revenue > AVG(total_revenue) OVER (
      ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) THEN 'Above Trend'
    WHEN total_revenue < AVG(total_revenue) OVER (
      ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) THEN 'Below Trend'
    ELSE 'On Trend'
  END AS performance_indicator

FROM files.default.gold_daily_order_summary

ORDER BY order_date DESC
LIMIT 90;  -- Last 3 months

-- ============================================================================
-- SQL Concepts Demonstrated:
-- ✅ Window Functions: LAG() for previous row access
-- ✅ Window Functions: AVG() OVER() with ROWS frame for moving averages
-- ✅ Window Functions: SUM() OVER() with PARTITION BY for running totals
-- ✅ NULLIF() for safe division (prevents divide-by-zero errors)
-- ✅ Nested window functions in CASE statements
-- ============================================================================

-- ============================================================================
-- Pro Tips for Window Functions:
--
-- LAG(column, offset) OVER (ORDER BY ...)
--   → Gets value from N rows back (offset=1 means previous row)
--
-- AVG(column) OVER (ORDER BY ... ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
--   → Calculates average over last 7 rows (6 previous + current)
--
-- SUM(column) OVER (PARTITION BY group ORDER BY ...)
--   → Running total that resets for each partition (e.g., each month)
-- ============================================================================
