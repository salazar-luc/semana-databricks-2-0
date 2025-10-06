# LakeFlow Pipelines

Databricks LakeFlow declarative pipelines implementing Medallion Architecture for UberEats data processing.

## Structure

```
lakeflow/
├── ingest/          # Bronze layer ingestion from multiple sources
├── transform/       # Silver layer transformations
└── queries/         # Validation queries
```

## Layers

### Bronze (`ingest/`)
- **dl-stg-raw-json-files.sql**: Streaming ingestion from JSON files
- Sources: MSSQL (users), PostgreSQL (drivers), MySQL (restaurants), Kafka (orders, status)
- No transformations, includes metadata tracking

### Silver (`transform/`)
- **silver_drivers.py**: Cleaned driver dimension (age, phone, vehicle info)
- **silver_users.py**: Standardized user data
- **silver_restaurants.py**: Restaurant dimension with location data
- **silver_orders.py**: Order facts with validations
- **silver_status.py**: Status events tracking

### Queries (`queries/`)
- **get-ingest-tables.sql**: Validation queries for bronze tables

## Pipeline Flow

```
JSON Files → Bronze (Streaming) → Silver (Cleaned) → Gold (Analytics)
```

Each layer includes:
- Data quality expectations
- Metadata tracking (source_file, ingestion_time)
- Table properties for optimization
