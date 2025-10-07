# Databricks SQL Analytics Queries

**3-tier progression: Basic → Intermediate → Advanced**

These queries transform the `gold_daily_order_summary` table into actionable insights while teaching SQL concepts progressively. Perfect for live demos and training sessions.

---

## 🎯 Query Progression

### **Learning Path**: Start with Basic → Master Intermediate → Apply Advanced

| Level | Query | SQL Concepts | Use Case |
|-------|-------|--------------|----------|
| **Basic** | Daily Summary | SELECT, simple calculations | Executive KPIs |
| **Intermediate** | Revenue Trends | Window functions (LAG, AVG OVER, SUM OVER) | Forecasting, trend analysis |
| **Advanced** | Customer Cohorts | CTEs, composite scoring, segmentation | Marketing strategy, retention |

---

## 📊 Query Catalog

### 1️⃣ **BASIC: Daily Performance Summary**
**File**: [`01_basic_daily_summary.sql`](01_basic_daily_summary.sql)

**Business Question**: *What are our daily metrics at a glance?*

**SQL Concepts**:
- ✅ Direct column selection
- ✅ Simple calculations (division)
- ✅ ORDER BY and LIMIT
- ✅ Reading from single table

**Output Columns**:
```
order_date, day_name, total_orders, unique_customers, unique_drivers,
unique_restaurants, total_revenue, avg_order_value, revenue_per_order, is_weekend
```

**Use Cases**:
- Daily standup reports
- Executive KPI dashboards
- Quick performance snapshots

**Recommended Visualizations**:
```
📋 Table: Last 30 days overview
💰 KPI Card: Latest total_revenue
📊 KPI Card: Latest total_orders
👥 KPI Card: Latest unique_customers
```

**Difficulty**: ⭐☆☆☆☆ (Beginner-friendly)

---

### 2️⃣ **INTERMEDIATE: Revenue Trends with Growth Analysis**
**File**: [`02_intermediate_revenue_trends.sql`](02_intermediate_revenue_trends.sql)

**Business Question**: *How is our revenue trending and where are we headed?*

**SQL Concepts**:
- ✅ **LAG()** - Access previous row for comparisons
- ✅ **AVG() OVER()** with ROWS frame - Moving averages
- ✅ **SUM() OVER()** with PARTITION BY - Running totals
- ✅ **NULLIF()** - Safe division (prevents errors)
- ✅ Nested window functions in CASE statements

**Key Metrics**:
- **Day-over-day**: Growth % vs yesterday
- **7-day moving average**: Smoothed trend line
- **Month-to-date**: Cumulative revenue by month
- **Performance indicator**: Above/below trend

**Output Columns**:
```
order_date, day_name, is_weekend, total_revenue, total_orders, revenue_per_order,
prev_day_revenue, revenue_change, revenue_growth_pct, revenue_7day_avg,
revenue_mtd, performance_indicator
```

**Use Cases**:
- Revenue forecasting
- Trend analysis (is growth accelerating?)
- Performance monitoring (beating targets?)
- Anomaly detection (unusual spikes/drops)

**Recommended Visualizations**:
```
📈 Line Chart (dual-axis):
   - Bars: total_revenue (daily)
   - Line overlay: revenue_7day_avg (trend)

💰 KPI Cards:
   - revenue_growth_pct (day-over-day %)
   - revenue_mtd (month progress)

📊 Area Chart: revenue_mtd over time (cumulative growth curve)
```

**Window Function Cheat Sheet**:
```sql
-- Get previous row (yesterday's value)
LAG(metric, 1) OVER (ORDER BY date)

-- 7-day moving average
AVG(metric) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)

-- Running total (resets each month)
SUM(metric) OVER (PARTITION BY year, month ORDER BY date)
```

**Difficulty**: ⭐⭐⭐☆☆ (Intermediate - requires window function understanding)

---

### 3️⃣ **ADVANCED: Customer Cohort & Behavioral Segmentation**
**File**: [`03_advanced_customer_cohort_analysis.sql`](03_advanced_customer_cohort_analysis.sql)

**Business Question**: *Who are our customers, how are they behaving, and what should we do about it?*

**SQL Concepts**:
- ✅ **Multiple CTEs** (WITH clause) - Modular query building
- ✅ **Complex CASE statements** - Multi-tier segmentation
- ✅ **Composite scoring** - Weighted algorithm (RFM-style)
- ✅ **Week-over-week comparisons** - LAG with offset=7
- ✅ **PERCENT_RANK()** - Percentile calculations
- ✅ **Business logic encoding** - Strategic recommendations in SQL

**4-Stage CTE Pipeline**:
1. **daily_customer_metrics**: Extract base metrics
2. **customer_segments**: Apply 3-dimensional segmentation (Engagement/Value/Quality)
3. **customer_trends**: Add growth trends and rolling averages
4. **cohort_scores**: Calculate composite health score (0-100)

**Segmentation Dimensions**:

| Dimension | Tiers | Based On |
|-----------|-------|----------|
| **Engagement** | High / Medium / Low / Very Low | Orders per customer |
| **Value** | Premium / High / Medium / Entry | Revenue per customer |
| **Quality** | Premium / Quality / Mixed / Value | % of high-value orders |

**Customer Health Score** (0-100):
```
= Engagement (40%) + Value (40%) + Quality (20%)
```

**Strategic Recommendations Matrix**:
```
┌─────────────────┬──────────────────┬──────────────────┐
│                 │ Growing          │ Declining        │
├─────────────────┼──────────────────┼──────────────────┤
│ High Quality    │ Scale Marketing  │ Retention Focus  │
│ Low Quality     │ Improve UX       │ Critical Alert   │
└─────────────────┴──────────────────┴──────────────────┘
```

**Output Columns**:
```
order_date, engagement_tier, value_tier, quality_tier, customer_health_score,
health_status, unique_customers, customer_growth_wow_pct, customers_14day_avg,
avg_customer_value_7day_trend, volume_percentile_rank, strategic_recommendation,
is_weekend
```

**Use Cases**:
- Marketing campaign targeting (focus on high-health cohorts)
- Retention strategy (trigger campaigns for declining segments)
- Customer lifetime value modeling (health score correlates to LTV)
- Product strategy (quality tiers reveal pricing opportunities)

**Recommended Visualizations**:
```
🎯 Gauge Chart: customer_health_score (0-100 scale, color by health_status)

📊 Heatmap Matrix:
   - Rows: engagement_tier
   - Columns: value_tier
   - Color intensity: customer_health_score

📈 Line Chart (multi-series):
   - unique_customers (actual daily)
   - customers_14day_avg (trend line)

🍩 Donut Charts (3 separate):
   - Engagement tier distribution
   - Value tier distribution
   - Quality tier distribution

📋 Filtered Table: strategic_recommendation with drill-down
```

**Difficulty**: ⭐⭐⭐⭐⭐ (Advanced - production-grade analytics engineering)

---

## 🚀 How to Use These Queries

### **Option 1: Quick Testing (Databricks SQL Editor)**
1. Open Databricks SQL workspace
2. Create new query
3. Copy query content from files
4. Run and explore results

### **Option 2: Build Dashboard (Production)**

**5-Minute Dashboard Setup**:
1. Create 3 queries (one per file)
2. Add visualizations:
   - Query 1 → Table + KPI cards
   - Query 2 → Line chart + KPI cards
   - Query 3 → Gauge + Heatmap + Donut charts
3. Arrange in dashboard grid
4. Schedule auto-refresh (hourly/daily)

**Suggested Dashboard Layout**:
```
┌─────────────────────────────────────────────────────────┐
│               UberEats Analytics Dashboard              │
├───────────┬───────────┬───────────┬─────────────────────┤
│ Revenue   │ Orders    │ Customers │ Health Score        │
│ (Basic)   │ (Basic)   │ (Basic)   │ (Advanced)          │
├───────────┴───────────┴───────────┴─────────────────────┤
│ Revenue Trends (Intermediate - Line Chart)              │
│ [Revenue bars + 7-day average line overlay]             │
├─────────────────────────────────┬───────────────────────┤
│ Customer Segmentation           │ Strategic Actions     │
│ (Advanced - Heatmap)            │ (Advanced - Table)    │
└─────────────────────────────────┴───────────────────────┘
```

### **Option 3: Live Demo Script**

**10-Minute Demo Flow**:

1. **Query 1 (2 min)**: Show basic daily metrics
   - *"This is what business sees every morning - simple, clear KPIs."*

2. **Query 2 (4 min)**: Explain window functions
   - *"Now we add intelligence - is revenue growing or declining?"*
   - Show LAG() for day-over-day
   - Show AVG OVER for trend smoothing
   - Highlight performance indicator logic

3. **Query 3 (4 min)**: Demonstrate advanced analytics
   - *"This is where data science meets SQL - customer segmentation at scale."*
   - Show CTE structure (modular building blocks)
   - Explain health score calculation
   - Show strategic recommendations in action

---

## 💡 Teaching Progression

### **For Beginners (Start Here)**
- Run Query 1
- Understand: SELECT, simple math, ORDER BY
- Modify: Change LIMIT to 60 (last 2 months)
- Add: New calculated field like `total_revenue / unique_restaurants`

### **For Intermediate Learners**
- Run Query 2
- Understand: Window functions (LAG, AVG OVER, SUM OVER)
- Modify: Change 7-day average to 14-day
- Add: Quarter-to-date cumulative revenue

### **For Advanced Practitioners**
- Run Query 3
- Understand: CTEs, composite scoring, segmentation
- Modify: Adjust health score weights (e.g., 50% engagement, 30% value, 20% quality)
- Add: Churn risk prediction (e.g., declining health + decreasing frequency)

---

## 🔧 Customization Examples

### **Add Date Filters**
```sql
WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAYS
```

### **Focus on Weekdays Only**
```sql
WHERE is_weekend = FALSE
```

### **Change Moving Average Window**
```sql
-- From 7-day to 14-day
AVG(metric) OVER (ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
```

### **Adjust Health Score Weights**
```sql
-- In Query 3, modify cohort_scores CTE
customer_health_score = (engagement * 0.5) + (value * 0.3) + (quality * 0.2)
```

---

## 📚 SQL Concepts Reference

### **Window Functions** (Query 2)
```sql
-- Previous row access
LAG(column, offset) OVER (ORDER BY date)

-- Moving average (last N rows)
AVG(column) OVER (ORDER BY date ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW)

-- Running total (resets per partition)
SUM(column) OVER (PARTITION BY group ORDER BY date)

-- Percentile ranking
PERCENT_RANK() OVER (ORDER BY column)
```

### **CTEs - Common Table Expressions** (Query 3)
```sql
WITH cte_name AS (
  SELECT ... FROM table
),
another_cte AS (
  SELECT ... FROM cte_name  -- Can reference previous CTEs
)
SELECT ... FROM another_cte;
```

### **Safe Division**
```sql
-- Prevents divide-by-zero errors
column_a / NULLIF(column_b, 0)
```

---

## 🎓 Sample Output Preview

### Query 1 Output (Basic)
```
order_date  | total_orders | total_revenue | revenue_per_order
------------|--------------|---------------|-------------------
2024-10-07  | 1,245        | 52,340.00     | 42.04
2024-10-06  | 1,189        | 49,876.00     | 41.95
```

### Query 2 Output (Intermediate)
```
order_date  | revenue_growth_pct | revenue_7day_avg | performance_indicator
------------|-------------------|------------------|----------------------
2024-10-07  | +5.2%             | 50,125.00        | 📈 Above Trend
2024-10-06  | -2.1%             | 49,890.00        | 📉 Below Trend
```

### Query 3 Output (Advanced)
```
date       | engagement_tier | value_tier | health_score | strategic_recommendation
-----------|----------------|------------|--------------|-------------------------
2024-10-07 | High Engagement| 🥇 High Val| 85           | 🚀 Scale Marketing
2024-10-06 | Medium Engage. | 🥈 Med Val | 62           | ➖ Monitor
```

---

## 🔗 Related Resources

- **Pipeline Documentation**: [`../README.md`](../README.md)
- **Gold Layer Definition**: [`../gold/gold_daily_order_summary.sql`](../gold/gold_daily_order_summary.sql)
- **Live Demo Script**: [`../DEMO_SCRIPT.md`](../DEMO_SCRIPT.md)

---

**Ready to analyze!** Start with Query 1 for quick wins, then progress through 2 and 3 as you build SQL expertise. 🚀
