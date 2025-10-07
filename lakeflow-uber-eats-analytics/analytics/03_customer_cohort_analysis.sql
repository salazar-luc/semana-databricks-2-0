-- ============================================================================
-- ADVANCED: Customer Cohort & Behavioral Segmentation
-- ============================================================================
-- Skill Level: Advanced (CTEs, complex joins, multi-level aggregations, segmentation)
-- Purpose: Analyze customer behavior patterns with RFM-style segmentation
-- Use Case: Marketing strategy, customer retention, lifetime value modeling
-- Visualization: Cohort heatmaps, segmentation matrices, funnel charts
-- ============================================================================

-- === CTE 1: Daily Customer Activity Metrics ===
WITH daily_customer_metrics AS (
  SELECT
    order_date,
    unique_customers,
    total_orders,
    total_revenue,
    orders_per_customer,
    avg_revenue_per_customer,
    high_value_order_pct,
    is_weekend
  FROM files.default.gold_daily_order_summary
),

-- === CTE 2: Customer Engagement Segmentation ===
customer_segments AS (
  SELECT
    order_date,

    -- Engagement Score (based on orders per customer)
    orders_per_customer,
    CASE
      WHEN orders_per_customer >= 3.0 THEN 'High Engagement (3+ orders/customer)'
      WHEN orders_per_customer >= 2.0 THEN 'Medium Engagement (2-3 orders/customer)'
      WHEN orders_per_customer >= 1.5 THEN 'Low Engagement (1.5-2 orders/customer)'
      ELSE 'Very Low Engagement (<1.5 orders/customer)'
    END AS engagement_tier,

    -- Value Score (based on revenue per customer)
    avg_revenue_per_customer,
    CASE
      WHEN avg_revenue_per_customer >= 100 THEN 'Premium ($100+)'
      WHEN avg_revenue_per_customer >= 50 THEN 'High Value ($50-$100)'
      WHEN avg_revenue_per_customer >= 25 THEN 'Medium Value ($25-$50)'
      ELSE 'Entry Level (<$25)'
    END AS value_tier,

    -- Quality Score (based on high-value order percentage)
    high_value_order_pct,
    CASE
      WHEN high_value_order_pct >= 40 THEN 'Premium Buyers (40%+ high-value orders)'
      WHEN high_value_order_pct >= 25 THEN 'Quality Buyers (25-40% high-value orders)'
      WHEN high_value_order_pct >= 15 THEN 'Mixed Buyers (15-25% high-value orders)'
      ELSE 'Value Buyers (<15% high-value orders)'
    END AS quality_tier,

    unique_customers,
    total_revenue,
    is_weekend

  FROM daily_customer_metrics
),

-- === CTE 3: Behavioral Patterns with Trends ===
customer_trends AS (
  SELECT
    order_date,
    unique_customers,
    avg_revenue_per_customer,
    engagement_tier,
    value_tier,
    quality_tier,

    -- Customer Growth Analysis (vs previous week same day)
    LAG(unique_customers, 7) OVER (ORDER BY order_date) AS customers_last_week,
    ROUND(
      ((unique_customers - LAG(unique_customers, 7) OVER (ORDER BY order_date)) /
       NULLIF(LAG(unique_customers, 7) OVER (ORDER BY order_date), 0)) * 100,
      2
    ) AS customer_growth_wow_pct,

    -- 14-day rolling average (longer trend view)
    ROUND(
      AVG(unique_customers) OVER (
        ORDER BY order_date
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
      ),
      2
    ) AS customers_14day_avg,

    -- Revenue per Customer Trend
    ROUND(
      AVG(avg_revenue_per_customer) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ),
      2
    ) AS avg_customer_value_7day_trend,

    -- Percentile Ranking (where does this day rank?)
    PERCENT_RANK() OVER (ORDER BY unique_customers) AS customer_volume_percentile,

    is_weekend

  FROM customer_segments
),

-- === CTE 4: Cohort Performance Scoring ===
cohort_scores AS (
  SELECT
    order_date,
    engagement_tier,
    value_tier,
    quality_tier,

    -- Composite Customer Health Score (0-100)
    ROUND(
      (
        -- Engagement contributes 40%
        (CASE
          WHEN engagement_tier LIKE 'High%' THEN 40
          WHEN engagement_tier LIKE 'Medium%' THEN 30
          WHEN engagement_tier LIKE 'Low%' THEN 20
          ELSE 10
        END) +

        -- Value contributes 40%
        (CASE
          WHEN value_tier LIKE '💎%' THEN 40
          WHEN value_tier LIKE '🥇%' THEN 30
          WHEN value_tier LIKE '🥈%' THEN 20
          ELSE 10
        END) +

        -- Quality contributes 20%
        (CASE
          WHEN quality_tier LIKE 'Premium%' THEN 20
          WHEN quality_tier LIKE 'Quality%' THEN 15
          WHEN quality_tier LIKE 'Mixed%' THEN 10
          ELSE 5
        END)
      ),
      0
    ) AS customer_health_score,

    unique_customers,
    customer_growth_wow_pct,
    customers_14day_avg,
    avg_customer_value_7day_trend,
    customer_volume_percentile,
    is_weekend

  FROM customer_trends
)

-- === FINAL OUTPUT: Comprehensive Customer Analytics ===
SELECT
  order_date,

  -- Segmentation
  engagement_tier,
  value_tier,
  quality_tier,

  -- Health Metrics
  customer_health_score,
  CASE
    WHEN customer_health_score >= 80 THEN 'Excellent (80+)'
    WHEN customer_health_score >= 60 THEN 'Good (60-79)'
    WHEN customer_health_score >= 40 THEN 'Fair (40-59)'
    ELSE 'Needs Attention (<40)'
  END AS health_status,

  -- Volume & Growth
  unique_customers,
  customer_growth_wow_pct,
  customers_14day_avg,

  -- Value Trends
  avg_customer_value_7day_trend,

  -- Performance Ranking
  ROUND(customer_volume_percentile * 100, 1) AS volume_percentile_rank,

  -- Strategic Insights
  CASE
    WHEN customer_health_score >= 80 AND customer_growth_wow_pct > 5
      THEN 'High Growth + High Quality → Scale Marketing'
    WHEN customer_health_score >= 80 AND customer_growth_wow_pct < -5
      THEN 'High Quality but Declining → Retention Campaign'
    WHEN customer_health_score < 40 AND customer_growth_wow_pct > 10
      THEN 'Growing but Low Quality → Improve Experience'
    WHEN customer_health_score < 40 AND customer_growth_wow_pct < 0
      THEN 'Critical: Declining + Low Quality → Urgent Intervention'
    ELSE 'Monitor'
  END AS strategic_recommendation,

  is_weekend

FROM cohort_scores

ORDER BY order_date DESC
LIMIT 60;

-- ============================================================================
-- SQL Concepts Demonstrated:
-- ✅ Multiple CTEs (WITH clause) for modular query building
-- ✅ Complex CASE statements for multi-tier segmentation
-- ✅ Composite scoring algorithm (weighted calculation)
-- ✅ Week-over-week comparisons (LAG with offset=7)
-- ✅ PERCENT_RANK() for percentile calculations
-- ✅ Nested window functions (AVG OVER within CASE)
-- ✅ Business logic translation (RFM-style scoring)
-- ✅ Strategic insights generation (actionable recommendations)
-- ============================================================================

-- ============================================================================
-- Business Logic Explained:
--
-- Customer Health Score (0-100):
--   - Engagement (40%): How often do customers order?
--   - Value (40%): How much do customers spend?
--   - Quality (20%): Do customers buy high-value items?
--
-- Strategic Recommendations Matrix:
--   ┌─────────────────┬──────────────────┬──────────────────┐
--   │                 │ Growing          │ Declining        │
--   ├─────────────────┼──────────────────┼──────────────────┤
--   │ High Quality    │ Scale Marketing  │ Retention Focus  │
--   │ Low Quality     │ Improve UX       │ Critical Alert   │
--   └─────────────────┴──────────────────┴──────────────────┘
--
-- Use Cases:
--   1. Identify high-value customer days → Replicate success patterns
--   2. Detect declining quality → Trigger retention campaigns
--   3. Segment marketing spend → Focus on high-health cohorts
--   4. Forecast revenue → Correlation between health score and revenue
-- ============================================================================
