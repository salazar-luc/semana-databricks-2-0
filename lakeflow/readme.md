# LakeFlow Pipelines

Databricks LakeFlow declarative pipelines implementing **Medallion Architecture** for UberEats data processing.

## Structure

```
lakeflow/
├── ingest/          # Bronze layer: Raw ingestion from multiple sources
├── transform/       # Silver layer: Cleaned dimensions + enriched domain tables
├── analytics/       # Gold layer: Business metrics and aggregations
└── queries/         # Validation queries
```

## Medallion Architecture Layers

### Bronze Layer (`ingest/`)
**Purpose**: Raw data ingestion with minimal transformations

- **dl-stg-raw-json-files.sql**: Streaming Auto Loader ingestion from JSON files
- **Sources**:
  - MSSQL (users)
  - PostgreSQL (drivers)
  - MySQL (restaurants)
  - Kafka (orders, status)
- **Quality**: `EXPECT (_rescued_data IS NULL)` - Warn on malformed JSON
- **Features**: Metadata tracking (source_file, ingestion_time)

**Tables**: `bronze_mssql_users`, `bronze_postgres_drivers`, `bronze_mysql_restaurants`, `bronze_kafka_orders`, `bronze_kafka_status`

---

### Silver Layer (`transform/`)
**Purpose**: Data cleaning, quality enforcement, and domain modeling

#### **Tier 1: Cleaned Dimensions (1:1 mapping)**
Individual source system tables with data quality and standardization:

- **silver_users.py**: User dimension
  - CPF validation, age calculation, phone number cleaning
  - Quality: `@dlt.expect_or_drop()` for null IDs and names

- **silver_drivers.py**: Driver dimension
  - License validation, vehicle standardization, age calculation
  - Quality: `@dlt.expect_or_drop()` for license, name, vehicle

- **silver_restaurants.py**: Restaurant dimension
  - CNPJ validation, rating categories, popularity flags
  - Quality: `@dlt.expect_or_drop()` for name and rating range (0-5)

- **silver_orders.py**: Order fact table (with keys only)
  - Foreign key validation (CPF, License, CNPJ), amount categories
  - Quality: `@dlt.expect_or_drop()` for keys, amounts, dates

- **silver_status.py**: Status event stream
  - Nested struct flattening, final status detection
  - Quality: `@dlt.expect_or_drop()` for status_id, status_name, timestamp

#### **Tier 2: Domain/Enriched Tables** 🆕
Denormalized tables joining multiple dimensions for analytics optimization:

- **silver_orders_enriched.py**: Complete order context
  - Joins: orders + users + drivers + restaurants
  - Denormalized attributes: customer demographics, driver details, restaurant info
  - Calculated fields: age groups, same-city deliveries, enrichment metadata
  - **Business Value**: Single table for analytics (no downstream joins needed)

---

### Gold Layer (`analytics/`)
**Purpose**: Business-ready aggregations and metrics

- **gold_restaurant_daily_metrics.py**: Restaurant performance analytics
  - Metrics: Order volume, revenue, avg order value, customer diversity
  - Dimensions: Date, restaurant, cuisine type, rating category
  - KPIs: Revenue per order, high-value %, customer retention proxy
  - Quality: `@dlt.expect_or_fail()` for positive orders and revenue

- **gold_driver_performance.py**: Driver efficiency analytics
  - Metrics: Deliveries, revenue generated, unique customers/restaurants served
  - Efficiency: Deliveries per hour, cross-city %, diversity scores
  - Dimensions: Date, driver, vehicle type, geography
  - KPIs: Performance tiers, working hours, cuisine diversity

---

## Pipeline Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 BRONZE LAYER (Raw Ingestion)                │
│  Auto Loader → 5 Bronze Tables (Streaming)                 │
│  Quality: WARN on _rescued_data                             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              SILVER LAYER (Cleaned & Enriched)              │
│  Tier 1: 5 Cleaned Dimensions (1:1)                         │
│  Quality: DROP invalid records                              │
│  ────────────────────────────────────                       │
│  Tier 2: 1 Enriched Domain Table (N:1)                      │
│  silver_orders_enriched (orders + all dimensions)           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│               GOLD LAYER (Business Metrics)                  │
│  2 Analytics Tables (Aggregations)                          │
│  Quality: FAIL on invalid metrics                           │
│  - Restaurant daily metrics                                 │
│  - Driver performance                                       │
└─────────────────────────────────────────────────────────────┘
```

## Data Quality Strategy

| Layer | Policy | Decorator | Business Logic |
|-------|--------|-----------|----------------|
| **Bronze** | WARN | `EXPECT` | Track malformed JSON, preserve all data |
| **Silver** | DROP | `@dlt.expect_or_drop()` | Remove invalid records (null keys, bad formats) |
| **Gold** | FAIL | `@dlt.expect_or_fail()` | Strict validation (metrics must be positive) |

## Key Features

✅ **Auto Loader** for incremental cloud storage ingestion
✅ **Stream-to-batch joins** for enrichment (streaming facts + batch dimensions)
✅ **Layered quality checks** (Warn → Drop → Fail)
✅ **Metadata tracking** (source files, ingestion timestamps)
✅ **Z-ordering** for query optimization
✅ **Domain modeling** in Silver for self-service analytics

## Queries (`queries/`)
- **get-ingest-tables.sql**: Validation queries for bronze tables

## Next Steps

- [ ] Add `silver_order_lifecycle` (orders + status timeline)
- [ ] Add Gold tables: `gold_user_behavior`, `gold_delivery_time_analysis`
- [ ] Implement SCD Type 2 for dimension history tracking
- [ ] Create pipeline configuration JSON for Databricks deployment
