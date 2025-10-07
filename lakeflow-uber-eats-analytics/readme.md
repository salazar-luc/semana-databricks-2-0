# UberEats Analytics - Lakeflow Demo Pipeline

**Simple Medallion Architecture demonstration for live presentations**

This is a streamlined Lakeflow (Delta Live Tables) pipeline showing Bronze → Silver → Gold progression with one table per layer.

---

## 📊 Pipeline Architecture

```
┌─────────────────────────────────────────────────────────┐
│  BRONZE: bronze_orders                                  │
│  Raw ingestion with Auto Loader                         │
│  Quality: WARN on malformed JSON                        │
│  File: bronze/bronze_orders.sql                         │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│  SILVER: silver_orders_enriched                         │
│  Data quality + temporal enrichments                    │
│  Quality: DROP invalid records                          │
│  File: silver/silver_orders_enriched.sql                │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│  GOLD: gold_daily_order_summary                         │
│  Daily business metrics and KPIs                        │
│  Quality: FAIL on invalid metrics                       │
│  File: gold/gold_daily_order_summary.sql                │
└─────────────────────────────────────────────────────────┘
```

---

## 🗂️ Project Structure

```
lakeflow-uber-eats-analytics/
├── bronze/
│   └── bronze_orders.sql          # Raw order ingestion (Auto Loader)
├── silver/
│   └── silver_orders_enriched.sql # Cleaned + enriched orders
├── gold/
│   └── gold_daily_order_summary.sql # Daily business analytics
└── README.md
```

---

## 🔵 Bronze Layer

**File**: `bronze/bronze_orders.sql`

**Purpose**: Raw data ingestion with minimal transformation

**Key Features**:
- **Auto Loader** streaming ingestion from JSON files
- **Metadata tracking**: `source_file`, `ingestion_time` for lineage
- **Quality policy**: `EXPECT (_rescued_data IS NULL)` - WARN on malformed JSON
- **Preserves everything**: No data is dropped, invalid records tracked in `_rescued_data`

**Source**: `/Volumes/semana/default/vol-owshq-shadow-traffic/kafka_orders_*`

**Demo Points**:
1. Show `read_files()` for incremental cloud storage ingestion
2. Explain `_rescued_data` column for malformed JSON tracking
3. Highlight `EXPECT` (warn only, don't drop)

---

## 🥈 Silver Layer

**File**: `silver/silver_orders_enriched.sql`

**Purpose**: Apply data quality, transformations, and business logic

**Key Features**:
- **Strict quality checks**: 4 constraints with `DROP ROW` policy
  - Valid order ID (not null)
  - Valid amount (> 0)
  - Valid date (not null)
  - Valid keys (user, driver, restaurant not null)
- **Temporal enrichments**:
  - Date parts: year, month, day, hour, minute
  - Day of week, day name, weekend flag
  - Quarter for business reporting
- **Business logic**:
  - Amount category (Low < $20 < Medium < $50 < High)
  - Time of day (Morning/Afternoon/Evening/Night)
  - Peak hour detection (11am-2pm, 6pm-9pm)

**Demo Points**:
1. Show `CONSTRAINT ... EXPECT ... ON VIOLATION DROP ROW` for strict quality
2. Explain temporal enrichments enable analytics without complex date math
3. Highlight business categories (amount, time of day, peak hours)

---

## 🥇 Gold Layer

**File**: `gold/gold_daily_order_summary.sql`

**Purpose**: Business-ready daily aggregations for dashboards and reporting

**Key Features**:
- **Strictest quality**: `FAIL UPDATE` prevents bad metrics in reports
- **Materialized View**: Batch aggregation (not streaming)
- **Comprehensive metrics**:
  - **Volume**: Orders, unique customers/drivers/restaurants
  - **Revenue**: Total, average, min/max, standard deviation
  - **Distribution**: Orders by amount category, time of day
  - **Peak hours**: Order and revenue during peak times
  - **KPIs**: Revenue per order, high-value order %, efficiency metrics
- **BI-ready dimensions**: Year, month, quarter, week, day name

**Demo Points**:
1. Show `MATERIALIZED VIEW` vs `STREAMING TABLE` (batch vs streaming)
2. Explain `ON VIOLATION FAIL UPDATE` - strict validation for reports
3. Highlight rich KPIs calculated from aggregated data
4. Show date dimensions for easy filtering in BI tools

---

## 🎯 Data Quality Strategy

| Layer | Policy | SQL Syntax | Business Logic |
|-------|--------|------------|----------------|
| **Bronze** | **WARN** | `EXPECT (...)` | Track malformed JSON, preserve all data |
| **Silver** | **DROP** | `EXPECT (...) ON VIOLATION DROP ROW` | Remove invalid records (null keys, bad amounts) |
| **Gold** | **FAIL** | `EXPECT (...) ON VIOLATION FAIL UPDATE` | Strict validation - prevent bad metrics |

---

## 🚀 How to Deploy

### 1. Create Pipeline in Databricks UI

**Settings**:
- **Name**: `demo_uber_eats_analytics`
- **Product edition**: Advanced
- **Pipeline mode**: Triggered (or Continuous for real-time demo)
- **Serverless**: ✅ Enabled (recommended)
- **Target catalog**: `semana`
- **Target schema**: `default`
- **Source code paths**:
  - `/Workspace/path/to/lakeflow-uber-eats-analytics/bronze/bronze_orders.sql`
  - `/Workspace/path/to/lakeflow-uber-eats-analytics/silver/silver_orders_enriched.sql`
  - `/Workspace/path/to/lakeflow-uber-eats-analytics/gold/gold_daily_order_summary.sql`

### 2. Configure Source Path

Ensure your JSON files exist at:
```
/Volumes/semana/default/vol-owshq-shadow-traffic/kafka_orders_*
```

### 3. Run Pipeline

Click **Start** in the pipeline UI and watch the data flow:
1. Bronze ingests raw JSON files
2. Silver cleans and enriches
3. Gold aggregates daily metrics

---

## 📈 Sample Queries

### Query Bronze (raw data)
```sql
SELECT * FROM semana.default.bronze_orders LIMIT 10;
```

### Query Silver (enriched data)
```sql
-- Orders by time of day
SELECT
  time_of_day,
  COUNT(*) as order_count,
  AVG(total_amount) as avg_amount
FROM semana.default.silver_orders_enriched
GROUP BY time_of_day
ORDER BY order_count DESC;
```

### Query Gold (business metrics)
```sql
-- Daily performance dashboard
SELECT
  order_date,
  day_name,
  total_orders,
  total_revenue,
  revenue_per_order,
  high_value_order_pct,
  peak_hour_revenue_pct
FROM semana.default.gold_daily_order_summary
ORDER BY order_date DESC
LIMIT 30;
```

---

## 🎓 Live Demo Script

### **Slide 1: Bronze Layer**
1. Open `bronze/bronze_orders.sql`
2. Explain Auto Loader pattern: `read_files()` + `cloudFiles` format
3. Show metadata tracking: `source_file`, `ingestion_time`
4. Highlight `EXPECT` policy: warns but doesn't drop

### **Slide 2: Silver Layer**
1. Open `silver/silver_orders_enriched.sql`
2. Show 4 data quality constraints with `DROP ROW`
3. Explain temporal enrichments (date parts, day name, weekend)
4. Highlight business logic (amount category, time of day, peak hours)

### **Slide 3: Gold Layer**
1. Open `gold/gold_daily_order_summary.sql`
2. Explain `MATERIALIZED VIEW` for batch aggregations
3. Show `FAIL UPDATE` - strictest quality for business reports
4. Highlight comprehensive KPIs and date dimensions

### **Slide 4: Pipeline Execution**
1. Show Databricks pipeline UI
2. Click **Start** and watch the graph
3. Show data quality metrics (warnings, drops, failures)
4. Query Gold table for results

---

## 💡 Key Takeaways

1. **Medallion Architecture**: Progressive refinement (Raw → Clean → Aggregate)
2. **Layered Quality**: Different policies per layer (Warn → Drop → Fail)
3. **Auto Loader**: Incremental ingestion without managing state
4. **Declarative**: SQL/Python definitions, Databricks handles orchestration
5. **Streaming + Batch**: Mix streaming tables (Silver) and materialized views (Gold)

---

## 🔗 Related Documentation

- **Lakeflow Knowledge Base**: `.claude/kb/lakeflow/`
- **Full Implementation**: `lakeflow/` (multi-table production example)
- **Official Docs**: [Databricks Lakeflow](https://docs.databricks.com/aws/en/dlt/)

---

## 📝 Notes

- **Demo-optimized**: Single table per layer for clarity
- **Production-ready patterns**: Same best practices as complex pipelines
- **Live presentation friendly**: Clear progression, easy to explain
- **All SQL**: Simpler syntax for broad audiences (Python version available if needed)

**Perfect for**: Workshops, demos, training sessions, proof-of-concepts
