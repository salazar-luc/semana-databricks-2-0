# 🚀 Databricks Performance Tuning Demo

> **Transform queries from 2 minutes to sub-second using real-world Databricks optimization techniques**

---

## 📦 What's Included

This comprehensive demo package shows progressive optimization of your **Lakeflow UberEats pipeline** with before/after metrics.

### Demo Materials

```
performance/
├── README.md                          # This file
├── 01_baseline_queries.sql           # Stage 1: Slow baseline (90-120s)
├── 02_quick_wins.sql                 # Stage 2: ANALYZE + Z-ORDER (10-15s)
├── 03_aqe_and_joins.sql              # Stage 3-4: AQE + Joins (1-3s)
└── 04_expert_techniques.sql          # Stage 5: Photon + Advanced (0.3s)
```

### Supporting Documentation

- **[PERFORMANCE_TUNING_DEMO.md](../docs/PERFORMANCE_TUNING_DEMO.md)** - Complete guide with theory and best practices

---

## 🎯 Quick Start

### 1. Prerequisites

✅ Lakeflow pipeline deployed and running ([../lakeflow/README.md](../lakeflow/README.md))
✅ Data in silver tables (orders, restaurants, drivers, users, status)
✅ Databricks SQL Warehouse (Medium or Large)
✅ 15-20 minutes for complete demo

### 2. Run the Demo

**Import notebooks to Databricks**:

```bash
# From your terminal
databricks workspace import performance/01_baseline_queries.sql \
  /Users/your.email@company.com/Performance_Demo/01_baseline_queries.sql --language SQL

databricks workspace import performance/02_quick_wins.sql \
  /Users/your.email@company.com/Performance_Demo/02_quick_wins.sql --language SQL

databricks workspace import performance/03_aqe_and_joins.sql \
  /Users/your.email@company.com/Performance_Demo/03_aqe_and_joins.sql --language SQL

databricks workspace import performance/04_expert_techniques.sql \
  /Users/your.email@company.com/Performance_Demo/04_expert_techniques.sql --language SQL
```

**Or use the UI**:
1. Go to Databricks Workspace
2. Click "Create" → "Import"
3. Upload each `.sql` file
4. Run sequentially

### 3. The 15-Minute Demo Flow

| Minutes | Notebook | What You Show | Expected Result |
|---------|----------|---------------|-----------------|
| **0-5** | 01_baseline_queries.sql | "Here's our slow query" | 90-120 seconds ❌ |
| **5-8** | 02_quick_wins.sql | "Two commands fix 80% of problems" | 10-15 seconds ⚡ |
| **8-11** | 03_aqe_and_joins.sql | "Enable Spark's intelligence" | 1-3 seconds 🚀 |
| **11-15** | 04_expert_techniques.sql | "The final optimizations" | 0.3 seconds 🔥 |

**Total Improvement**: **250-400x faster!** 🎉

---

## 📊 Performance Journey

### The Story

```
┌─────────────────────────────────────────────────────────────┐
│  "Complex analytical queries on UberEats data are SLOW"    │
│  Business users wait 2 minutes for dashboards to load      │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Stage 1: Baseline - 100 seconds                           │
│  • No statistics                                            │
│  • Random data layout                                       │
│  • Large shuffles                                           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Stage 2: Quick Wins - 12 seconds (8x faster!)             │
│  • ANALYZE TABLE (collect statistics)                      │
│  • Z-ORDER (co-locate related data)                        │
│  • OPTIMIZE (compact small files)                          │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Stage 3: AQE - 4 seconds (25x faster!)                    │
│  • Adaptive Query Execution enabled                        │
│  • Dynamic partition coalescing                            │
│  • Runtime join optimization                               │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Stage 4: Join Mastery - 1.5 seconds (66x faster!)         │
│  • Broadcast small dimensions                              │
│  • Skew join handling                                      │
│  • Correlated subquery rewrite                             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Stage 5: Expert Mode - 0.4 seconds (250x faster!) 🏆      │
│  • Photon engine                                           │
│  • Range join optimization                                 │
│  • Query result caching                                    │
│  • Bloom filter indexes                                    │
└─────────────────────────────────────────────────────────────┘
```

---

## 🎓 What Each Stage Teaches

### Stage 1: Baseline ⏱️

**File**: `01_baseline_queries.sql`

**Purpose**: Establish pain points

**Key Queries**:
- Q1: Multi-table join with aggregations (4 tables)
- Q2: Driver efficiency with window functions
- Q3: Customer behavior (correlated subquery - very slow!)
- Q4: Time-based patterns

**Demo Tips**:
- Run Query 1, show 90-120 second execution time
- Click "View Query Profile" and show bottlenecks:
  - Large shuffle operations (GB of data moved)
  - No broadcast joins
  - Many files scanned (90%+ unnecessary)
  - Sort/merge joins instead of hash joins

**Takeaway**: *"Without optimizations, Spark makes poor decisions"*

---

### Stage 2: Quick Wins ⚡

**File**: `02_quick_wins.sql`

**Purpose**: Show 80/20 rule - biggest improvements with least effort

**Techniques**:
1. **ANALYZE TABLE** - Collect data statistics
2. **Z-ORDER** - Optimize data layout for common queries
3. **OPTIMIZE** - Compact small files

**Demo Tips**:
- Run `ANALYZE TABLE` on all tables (explain what it does)
- Run `OPTIMIZE ... ZORDER BY` showing thought process for column selection
- Re-run Query 1: **10-15 seconds** ✨
- Show Query Profile: Files scanned reduced by 90%!

**Takeaway**: *"Two commands gave us 8-10x improvement!"*

---

### Stage 3: AQE + Joins 🚀

**File**: `03_aqe_and_joins.sql`

**Purpose**: Enable Spark's runtime intelligence

**Techniques**:
1. **Adaptive Query Execution** - Runtime optimization
2. **Broadcast hints** - Force optimal join strategy
3. **Skew join handling** - Auto-balance hot keys
4. **Subquery rewrite** - Eliminate correlated subqueries

**Demo Tips**:
- Enable AQE config (explain each setting)
- Re-run Query 1: **4 seconds** 🔥
- Show Query Profile: "AdaptiveSparkPlan", "DynamicBroadcastHashJoin"
- Add explicit broadcast hints: **1.5 seconds** 💨
- Show correlated subquery before/after: 50x improvement!

**Takeaway**: *"AQE + broadcast joins = game changer"*

---

### Stage 4: Expert Techniques 🔥

**File**: `04_expert_techniques.sql`

**Purpose**: Final polish to reach sub-second queries

**Techniques**:
1. **Photon Engine** - C++ vectorized execution (2-10x for aggregations)
2. **Range Join Optimization** - Inequality joins 10-100x faster
3. **Query Result Caching** - Instant repeated queries
4. **Bloom Filter Indexes** - Skip files for point lookups
5. **Liquid Clustering** - Next-gen Z-ordering

**Demo Tips**:
- Show Photon config in SQL Warehouse
- Run aggregation-heavy query with Photon: **0.4 seconds** ⚡
- Run cached query twice: First 2s, second 100ms
- Show final results table: **250x total improvement!** 🎉

**Takeaway**: *"From 2 minutes to sub-second - 250x faster!"*

---

## 💡 Key Techniques Reference

### Must-Do Optimizations ✅

| Technique | Impact | Effort | When to Apply |
|-----------|--------|--------|---------------|
| **ANALYZE TABLE** | High | Low | After data loads, weekly |
| **Z-ORDER** | Very High | Medium | Recurring queries, high-cardinality filters |
| **OPTIMIZE** | High | Low | Many small files (>1000) |
| **Enable AQE** | High | Very Low | All analytical queries |
| **Broadcast Joins** | Very High | Low | Small dimensions (<100MB) |

### Advanced Optimizations 🚀

| Technique | Impact | Effort | When to Apply |
|-----------|--------|--------|---------------|
| **Photon** | High | Very Low | Aggregations, scans, joins |
| **Range Join Opt** | Very High | Low | Inequality joins (BETWEEN) |
| **Bloom Filters** | Medium | Medium | Point lookups, high-cardinality |
| **Liquid Clustering** | High | Medium | Alternative to Z-ORDER |

---

## 🎤 Demo Script Cheat Sheet

### Opening (1 minute)

> "We have a Lakeflow pipeline processing UberEats data. Business users run analytical queries to understand restaurant performance, driver efficiency, and customer behavior. **Problem**: These queries are SLOW. Let me show you how we fixed it..."

### Stage 1: Show the Pain (3 minutes)

> "Here's a typical query our analysts run - joining 4 tables, aggregating 30 days of data..."
>
> [Run Query 1]
>
> "90 seconds. Let's look at why..." [Show Query Profile]
>
> "No statistics → poor join ordering. Random data layout → scanning 10,000 unnecessary files. Let's fix this..."

### Stage 2: Quick Wins (4 minutes)

> "First, let's tell Spark about our data..."
>
> [Run ANALYZE TABLE]
>
> "Now let's organize data so related records are co-located..."
>
> [Run OPTIMIZE ZORDER BY]
>
> "Let's re-run the EXACT same query..." [Run Query 1]
>
> "**12 seconds!** We just got 8x faster with two commands. Look at files scanned - reduced by 90%!"

### Stage 3: AQE (3 minutes)

> "Now let's enable Spark's runtime intelligence..."
>
> [Enable AQE config]
>
> [Re-run Query 1]
>
> "**4 seconds!** AQE detected small tables and broadcast them automatically. Coalesced 200 partitions down to 20. We're now 25x faster than baseline."

### Stage 4: Expert Mode (4 minutes)

> "Final optimizations... Photon engine for vectorized execution... Query caching for dashboards..."
>
> [Enable Photon, run final query]
>
> "**0.4 seconds.** From 90 seconds to sub-second. That's **250x faster!**"
>
> [Show results table]
>
> "Here's the complete journey - from slow to lightning fast."

### Closing (1 minute)

> "Key takeaways: Statistics are essential. Data layout matters. Let AQE do the work. Broadcast small tables. Enable Photon. **Always profile your queries.**"
>
> **[MIC DROP]** 🎤⬇️

---

## 📈 Expected Results Template

Use this table to record your actual demo results:

| Stage | Technique | Expected Time | Your Time | Speedup |
|-------|-----------|---------------|-----------|---------|
| Baseline | None | 90-120s | _____s | 1x |
| Quick Wins | ANALYZE + Z-ORDER | 10-15s | _____s | ____x |
| AQE | Adaptive Execution | 4-6s | _____s | ____x |
| Joins | Broadcast + Skew | 1-3s | _____s | ____x |
| Expert | Photon + All | 0.3-0.5s | _____s | ____x |

---

## 🐛 Troubleshooting

### Issue: "Baseline query is already fast"

**Cause**: Tables might already be optimized from previous demos

**Fix**:
```sql
-- Disable optimizations for true baseline
SET spark.sql.adaptive.enabled = false;
SET spark.databricks.optimizer.dynamicFilePruning = false;
```

---

### Issue: "ANALYZE TABLE taking too long"

**Cause**: Large tables with many columns

**Fix**: Analyze only key columns
```sql
ANALYZE TABLE table_name COMPUTE STATISTICS
  FOR COLUMNS (col1, col2, col3);  -- Only important columns
```

---

### Issue: "Photon not available"

**Cause**: Warehouse doesn't have Photon enabled or workspace tier limitation

**Fix**:
1. Check SQL Warehouse settings → Enable Photon
2. If unavailable, show estimated impact (2-3x) without actually running

---

### Issue: "Queries already cached"

**Cause**: Running same query multiple times

**Fix**: Clear cache between demo runs
```sql
CLEAR CACHE;
```

---

## 🎯 Customization Tips

### Adapt to Your Data

The notebooks use the UberEats pipeline tables. To use with your own data:

1. **Update table references**
   ```sql
   -- Change:
   FROM semana.default.silver_orders o

   -- To:
   FROM your_catalog.your_schema.your_table t
   ```

2. **Adjust Z-ORDER columns** based on your query patterns
   ```sql
   -- What columns do you frequently filter on?
   OPTIMIZE your_table ZORDER BY (your_filter_cols);
   ```

3. **Modify queries** to match your business questions

---

## 📚 Additional Resources

### Databricks Documentation

- [Performance Tuning Guide](https://docs.databricks.com/optimizations/index.html)
- [Delta Lake Optimize](https://docs.databricks.com/delta/optimize.html)
- [Adaptive Query Execution](https://docs.databricks.com/optimizations/aqe.html)
- [Photon Engine](https://docs.databricks.com/runtime/photon.html)
- [Query Profile Tutorial](https://docs.databricks.com/sql/user/queries/query-profile.html)

### Demo Materials

- **[Complete Demo Guide](../docs/PERFORMANCE_TUNING_DEMO.md)** - Theory, best practices, troubleshooting
- **[Lakeflow Pipeline](../lakeflow/)** - Source data pipeline
- **[DABs Deployment Guide](../lakeflow/README.md)** - How the pipeline was deployed

---

## 🎉 Summary

**What This Demo Proves**:
- 🚀 **250-400x speedup** is achievable with production techniques
- ⚡ **80% improvement** comes from basic optimizations (ANALYZE + Z-ORDER)
- 🧠 **AQE is powerful** - runtime intelligence beats static plans
- 🎯 **Broadcast joins** eliminate expensive shuffles
- 🔥 **Photon** accelerates aggregations 2-10x

**Time Investment**:
- Setup: 5 minutes (import notebooks)
- Demo delivery: 15 minutes
- Total: 20 minutes to blow minds! 💥

**Key Message**:
> *"With the right techniques, any slow query can become fast. It's not magic - it's understanding Spark's execution model and giving it the information it needs to optimize."*

---

**Ready to blow their minds? Let's optimize! 🚀**
