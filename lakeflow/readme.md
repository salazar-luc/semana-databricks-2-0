# LakeFlow UberEats Analytics Pipeline
## Multi-Cloud Databricks Asset Bundles (DABs) Deployment

> **Enterprise-grade medallion architecture pipeline for UberEats analytics deployed across AWS and Azure using Databricks Asset Bundles**

---

## 📋 Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Project Structure](#project-structure)
5. [Environment Targets](#environment-targets)
6. [Deployment Guide](#deployment-guide)
7. [Multi-Cloud Schema Support](#multi-cloud-schema-support)
8. [Data Quality Strategy](#data-quality-strategy)
9. [Configuration](#configuration)
10. [Troubleshooting](#troubleshooting)
11. [Best Practices](#best-practices)

---

## 🎯 Overview

This project implements a **complete medallion architecture** (Bronze → Silver → Gold) for UberEats analytics using **Delta Live Tables (DLT)** and **Databricks Asset Bundles (DABs)** for infrastructure-as-code deployment.

### What Gets Deployed

- **13 Pipeline Files**:
  - 1 SQL file (Bronze layer: multi-source ingestion)
  - 6 Python files (Silver layer: cleaned dimensions + enriched tables)
  - 6 Python files (Gold layer: business analytics)

- **Single DLT Pipeline** that processes:
  - Multi-source data (MSSQL, PostgreSQL, MySQL, Kafka)
  - Multi-cloud schemas (AWS vs Azure data differences)
  - Data quality expectations with DLT constraints
  - Complete business analytics (restaurants, drivers, customers)

### Key Benefits

✅ **One-command deployment**: `databricks bundle deploy`
✅ **Multi-environment support**: Dev/Staging/Prod with different configs
✅ **Multi-cloud compatible**: Handles AWS and Azure schema differences
✅ **Version controlled**: Everything in Git
✅ **Repeatable**: No manual UI configuration
✅ **External locations**: Uses Azure ADLS Gen2 directly (no volume creation)

---

## 🏗️ Architecture

### Medallion Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                 BRONZE LAYER (Raw Ingestion)                │
│  Auto Loader → 5 Bronze Tables (Streaming)                 │
│  Sources: MSSQL, PostgreSQL, MySQL, Kafka                   │
│  Quality: WARN on _rescued_data                             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              SILVER LAYER (Cleaned & Enriched)              │
│  Tier 1: 5 Cleaned Dimensions (1:1 mapping)                 │
│  - silver_users, silver_drivers, silver_restaurants         │
│  - silver_orders, silver_status                             │
│  Quality: DROP invalid records                              │
│  Multi-Cloud: Schema detection for AWS vs Azure             │
│  ────────────────────────────────────                       │
│  Tier 2: 1 Enriched Domain Table (N:1 join)                 │
│  - silver_orders_enriched (denormalized analytics table)    │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│               GOLD LAYER (Business Metrics)                  │
│  6 Analytics Tables (Aggregations & KPIs)                   │
│  - Restaurant daily metrics                                 │
│  - Driver performance                                       │
│  - City market analytics                                    │
│  - Cuisine type analytics                                   │
│  - Customer behavior metrics                                │
│  - Hourly order patterns                                    │
│  Quality: FAIL on invalid metrics                           │
└─────────────────────────────────────────────────────────────┘
```

### Bronze Layer

**File**: `ingest/dl-stg-raw-json-files.sql`

**Purpose**: Raw data ingestion with minimal transformations using Auto Loader

**Sources**:
- **MSSQL**: Users data (`${source_volume_path}/mssql/*`)
- **PostgreSQL**: Drivers data (`${source_volume_path}/postgres/*`)
- **MySQL**: Restaurants data (`${source_volume_path}/mysql/*`)
- **Kafka**: Orders data (`${source_volume_path}/kafka/orders/*`)
- **Kafka**: Status events (`${source_volume_path}/kafka/status/*`)

**Quality**: `EXPECT (_rescued_data IS NULL)` - Warn on malformed JSON
**Features**: Metadata tracking (source_file, ingestion_time)

**Tables**: `bronze_mssql_users`, `bronze_postgres_drivers`, `bronze_mysql_restaurants`, `bronze_kafka_orders`, `bronze_kafka_status`

---

### Silver Layer

**Purpose**: Data cleaning, quality enforcement, and domain modeling

#### Tier 1: Cleaned Dimensions (1:1 mapping)

**silver_users.py** - User dimension
- CPF validation, age calculation, phone number cleaning
- Quality: `@dlt.expect_or_drop()` for null IDs and names

**silver_drivers.py** - Driver dimension
- License validation, vehicle standardization, age calculation
- Quality: `@dlt.expect_or_drop()` for license, name, vehicle

**silver_restaurants.py** - Restaurant dimension ⭐
- **Multi-cloud schema support**: Detects AWS vs Azure schemas
- AWS: CNPJ validation, rating categories, popularity flags
- Azure: Menu sections, active status, description mapping
- Quality: `@dlt.expect_or_drop()` for restaurant_id and name
- Schema detection: Uses `menu_section_id` presence to identify Azure data

**silver_orders.py** - Order fact table
- Foreign key validation (CPF, License, CNPJ), amount categories
- Quality: `@dlt.expect_or_drop()` for keys, amounts, dates

**silver_status.py** - Status event stream
- Nested struct flattening, final status detection
- Quality: `@dlt.expect_or_drop()` for status_id, status_name, timestamp

#### Tier 2: Domain/Enriched Tables

**silver_orders_enriched.py** - Complete order context
- Joins: orders + users + drivers + restaurants
- Denormalized attributes: customer demographics, driver details, restaurant info
- Calculated fields: age groups, same-city deliveries, enrichment metadata
- **Business Value**: Single table for analytics (no downstream joins needed)

---

### Gold Layer

**Purpose**: Business-ready aggregations and metrics

**gold_restaurant_daily_metrics.py** - Restaurant performance
- Metrics: Order volume, revenue, avg order value, customer diversity
- Dimensions: Date, restaurant, cuisine type, rating category
- KPIs: Revenue per order, high-value %, customer retention proxy

**gold_driver_performance.py** - Driver efficiency
- Metrics: Deliveries, revenue generated, unique customers/restaurants served
- Efficiency: Deliveries per hour, cross-city %, diversity scores
- Dimensions: Date, driver, vehicle type, geography

**gold_city_market_analytics.py** - Market analysis by city
**gold_cuisine_type_analytics.py** - Cuisine performance
**gold_customer_behavior_metrics.py** - Customer insights
**gold_hourly_order_patterns.py** - Temporal analysis

---

## ✅ Prerequisites

### 1. Databricks CLI (v0.213.0+)

```bash
# Install Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Verify installation
databricks --version
```

### 2. Authentication

Authenticate to both AWS and Azure workspaces:

```bash
# AWS Workspace
databricks auth login --host https://dbc-fa899e55-82f1.cloud.databricks.com

# Azure Workspace
databricks auth login --host https://adb-2090585310411504.4.azuredatabricks.net
```

### 3. Unity Catalog Access

Ensure you have access to:
- **Catalog**: `semana`
- **Schema**: `default`
- **AWS Volume**: `/Volumes/semana/default/vol-owshq-shadow-traffic`
- **Azure External Location**: `owshq-shadow-traffic` → `abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net`

---

## 📂 Project Structure

```
lakeflow/
├── README.md                          # This file (combined documentation)
├── databricks.yml                     # Main DABs configuration
├── ingest/
│   └── dl-stg-raw-json-files.sql     # Bronze: Raw data ingestion
├── transform/
│   ├── silver_users.py               # Silver: Clean user data
│   ├── silver_drivers.py             # Silver: Clean driver data
│   ├── silver_restaurants.py         # Silver: Clean restaurants (multi-cloud)
│   ├── silver_orders.py              # Silver: Clean orders
│   ├── silver_status.py              # Silver: Clean status events
│   └── silver_orders_enriched.py     # Silver: Enriched orders (joins all)
├── analytics/
│   ├── gold_restaurant_daily_metrics.py       # Gold: Restaurant KPIs
│   ├── gold_driver_performance.py             # Gold: Driver metrics
│   ├── gold_city_market_analytics.py          # Gold: City analysis
│   ├── gold_cuisine_type_analytics.py         # Gold: Cuisine insights
│   ├── gold_customer_behavior_metrics.py      # Gold: Customer behavior
│   └── gold_hourly_order_patterns.py          # Gold: Temporal patterns
└── queries/
    └── get-ingest-tables.sql          # Validation queries
```

---

## 🌍 Environment Targets

The `databricks.yml` defines three deployment targets:

### 1. AWS Development (`aws`)

**Purpose**: Existing AWS workspace for development

- **Host**: `https://dbc-fa899e55-82f1.cloud.databricks.com`
- **Mode**: `development`
- **Catalog**: `semana`
- **Data Source**: `/Volumes/semana/default/vol-owshq-shadow-traffic`
- **Pipeline Settings**: Development mode, Manual triggers, Serverless

### 2. Azure Development (`azure_dev`) - DEFAULT ⭐

**Purpose**: Azure workspace for development and testing

- **Host**: `https://adb-2090585310411504.4.azuredatabricks.net`
- **Mode**: `development`
- **Catalog**: `semana`
- **Data Source**: `abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net`
- **Storage**: Uses existing external location (no volume needed)
- **Pipeline Settings**: Development mode, Manual triggers, Serverless

### 3. Azure Production (`azure_prod`)

**Purpose**: Production environment with ADLS Gen2 storage

- **Host**: `https://adb-2090585310411504.4.azuredatabricks.net`
- **Mode**: `production`
- **Catalog**: `semana`
- **Data Source**: `abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net`
- **Pipeline Settings**: Production mode, Continuous processing, Serverless

---

## 🚀 Deployment Guide

### Quick Start

All commands should be run from the `lakeflow/` directory:

```bash
cd /path/to/semana-databricks-2-0/lakeflow
```

### Step 1: Validate Configuration

```bash
# Validate Azure dev deployment (default)
databricks bundle validate --target azure_dev

# Validate other targets
databricks bundle validate --target aws
databricks bundle validate --target azure_prod
```

### Step 2: Deploy Pipeline

```bash
# Deploy to Azure Development (default)
databricks bundle deploy --target azure_dev

# Deploy to other environments
databricks bundle deploy --target aws
databricks bundle deploy --target azure_prod
```

**Expected output:**
```
Uploading bundle files to /Workspace/Users/...
Deploying resources...
Deployment complete!
```

**Pipeline naming convention:**
- Development: `[dev your_username] lakeflow-elt-uber-eats-{target}`
- Production: `lakeflow-elt-uber-eats-{target}` (no dev prefix)

### Step 3: Run Pipeline

```bash
# Run on Azure dev
databricks bundle run lakeflow_uber_eats --target azure_dev

# Run on other environments
databricks bundle run lakeflow_uber_eats --target aws
databricks bundle run lakeflow_uber_eats --target azure_prod
```

**Alternative**: Trigger via Databricks UI:
1. Navigate to **Workflows → Delta Live Tables**
2. Find your pipeline: `lakeflow-elt-uber-eats-{target}`
3. Click **Start**

### Step 4: Monitor Pipeline

```bash
# Show deployment summary
databricks bundle summary --target azure_dev
```

---

## 🌐 Multi-Cloud Schema Support

### Challenge

AWS and Azure environments have **different source schemas** for the same logical data:

**AWS MySQL Restaurants Schema:**
```
restaurant_id, cnpj, name, cuisine_type, phone_number, city,
address, average_rating, num_reviews, opening_time, closing_time
```

**Azure MySQL Restaurants Schema:**
```
restaurant_id, name, active, description, dt_current_timestamp,
menu_section_id
```

### Solution

The `silver_restaurants.py` transformation uses **runtime schema detection**:

```python
# Detect schema by checking available columns
available_columns = set(bronze_df.columns)
is_azure_schema = "menu_section_id" in available_columns

# Build schema-specific transformations
if is_azure_schema:
    # Azure path: Map description → cuisine_type, set defaults
    return azure_transformation()
else:
    # AWS path: Full transformation with all AWS fields
    return aws_transformation()
```

**Key Features:**
- ✅ **No column reference errors**: Avoids PySpark's compile-time validation
- ✅ **Unified output schema**: Both paths produce same silver table structure
- ✅ **Data source tracking**: Adds `data_source` column ('aws' or 'azure')
- ✅ **Default values**: Azure gets sensible defaults (rating=4.0, city='Unknown')
- ✅ **Column mapping**: Azure's `description` → `cuisine_type`

---

## 🛡️ Data Quality Strategy

| Layer | Policy | Decorator | Business Logic |
|-------|--------|-----------|----------------|
| **Bronze** | WARN | `EXPECT` | Track malformed JSON, preserve all data |
| **Silver** | DROP | `@dlt.expect_or_drop()` | Remove invalid records (null keys, bad formats) |
| **Gold** | FAIL | `@dlt.expect_or_fail()` | Strict validation (metrics must be positive) |

### Quality Checks by Layer

**Bronze Layer:**
```sql
CONSTRAINT no_rescued_data
EXPECT (_rescued_data IS NULL)
```

**Silver Layer:**
```python
@dlt.expect_or_drop("valid_restaurant_id", "restaurant_id IS NOT NULL")
@dlt.expect_or_drop("valid_name", "name IS NOT NULL")
```

**Gold Layer:**
```python
@dlt.expect_or_fail("positive_orders", "total_orders > 0")
@dlt.expect_or_fail("positive_revenue", "total_revenue >= 0")
```

---

## ⚙️ Configuration

### Configuration Parameters

The pipeline receives these parameters from `databricks.yml`:

```yaml
configuration:
  source_volume_path: abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net
  catalog_name: semana
  schema_name: default
```

### Accessing in Code

**SQL files:**
```sql
FROM STREAM read_files(
  '${source_volume_path}/mssql/*',
  format => 'json'
);
```

**Python files:**
```python
source_path = spark.conf.get("source_volume_path")
```

### Environment-Specific Variables

Each target can override variables:

```yaml
targets:
  aws:
    variables:
      source_volume_path: /Volumes/semana/default/vol-owshq-shadow-traffic

  azure_dev:
    variables:
      source_volume_path: abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net
```

---

## 🐛 Troubleshooting

### Common Issues

**Authentication Error**
```bash
databricks auth login --host <workspace-url>
```

**Catalog Does Not Exist**
```bash
databricks catalogs list
# Update databricks.yml with existing catalog
```

**Schema Mismatch**
- Already handled by `silver_restaurants.py` schema detection
- Apply same pattern to other tables if needed

**Permission Denied**
```sql
GRANT USE CATALOG ON CATALOG semana TO `your.email@company.com`;
GRANT CREATE TABLE ON SCHEMA semana.default TO `your.email@company.com`;
```

---

## 💡 Best Practices

1. **Always validate before deploy**
   ```bash
   databricks bundle validate -t azure_dev
   databricks bundle deploy -t azure_dev
   ```

2. **Use version control**
   ```bash
   git add lakeflow/
   git commit -m "Update pipeline"
   git push
   ```

3. **Test in dev before prod**
   - Deploy to azure_dev
   - Run and verify results
   - Deploy to azure_prod

4. **Monitor pipeline health**
   - Check DLT lineage graph
   - Review data quality metrics
   - Monitor dropped records

5. **Parameterize environment values**
   - Use variables in `databricks.yml`
   - Never hardcode paths in code

---

## 📚 Command Reference

```bash
# Authentication
databricks auth login --host <workspace-url>

# Validation
databricks bundle validate -t azure_dev

# Deployment
databricks bundle deploy -t azure_dev

# Run pipeline
databricks bundle run lakeflow_uber_eats -t azure_dev

# View summary
databricks bundle summary -t azure_dev

# Catalog management
databricks catalogs list
databricks volumes list semana.default
```

---

## 🎉 Quick Start Summary

```bash
# 1. Navigate to lakeflow directory
cd lakeflow

# 2. Authenticate
databricks auth login --host https://adb-2090585310411504.4.azuredatabricks.net

# 3. Validate
databricks bundle validate --target azure_dev

# 4. Deploy
databricks bundle deploy --target azure_dev

# 5. Run
databricks bundle run lakeflow_uber_eats --target azure_dev
```

**Expected result:**
- ✅ Pipeline deployed to Azure Databricks
- ✅ All 13 pipeline files uploaded
- ✅ Multi-cloud schema support enabled
- ✅ External location configured (ADLS Gen2)
- ✅ Ready to process data!

---

## 📖 Resources

- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- [Delta Live Tables](https://docs.databricks.com/delta-live-tables/)
- [Unity Catalog External Locations](https://docs.databricks.com/sql/language-manual/sql-ref-external-locations.html)
- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/)

---

**Happy deploying! 🚀**
