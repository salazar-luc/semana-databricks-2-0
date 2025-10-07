-- ============================================================================
-- BASIC: Daily Performance Summary
-- ============================================================================
-- Skill Level: Beginner (Basic SQL: SELECT, GROUP BY, aggregate functions)
-- Purpose: Simple daily metrics - order volume and revenue totals
-- Use Case: Executive KPI dashboard, daily reports
-- Visualization: Table view, simple KPI cards
-- ============================================================================

SELECT
  order_date,
  day_name,

  -- Volume Metrics
  total_orders,
  unique_customers,
  unique_drivers,
  unique_restaurants,

  -- Revenue Metrics
  total_revenue,
  avg_order_value,

  -- Simple Calculated Field
  ROUND(total_revenue / total_orders, 2) AS revenue_per_order,

  -- Weekend Indicator
  is_weekend

FROM files.default.gold_daily_order_summary

ORDER BY order_date DESC
LIMIT 30;

-- ============================================================================
-- SQL Concepts Demonstrated:
-- ✅ SELECT from single table
-- ✅ Direct column references (no complex logic)
-- ✅ Simple calculations (division)
-- ✅ ORDER BY and LIMIT
-- ============================================================================
