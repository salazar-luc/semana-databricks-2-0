# Shadow Traffic Data Generator

## Prerequisites

1. **Docker** installed and running
2. **Credentials** configured in `.env` file (already set up)

## Setup

### Pull ShadowTraffic Container
```bash
docker pull shadowtraffic/shadowtraffic:latest
```

## Running Data Generators

**Important:** Navigate to the `gen/` directory before running commands:
```bash
cd gen
```

### 1. Uber Eats → Azure Blob Storage
Generates comprehensive event data (orders, payments, GPS tracking, etc.) to Azure Data Lake:
```bash
docker run \
  --env-file .env \
  -v $(pwd)/azure/uber-eats.json:/home/config.json \
  shadowtraffic/shadowtraffic:latest \
  --config /home/config.json
```

**Data Generated:**
- Orders, payments, receipts, status transitions
- GPS tracking and delivery routes
- Restaurant data, products, menus, ratings
- Support tickets, recommendations, search events

### 2. Users → Azure SQL Database
Generates user profile data directly to MSSQL:
```bash
docker run \
  --env-file .env \
  -v $(pwd)/mssql/users.json:/home/config.json \
  shadowtraffic/shadowtraffic:latest \
  --config /home/config.json
```

**Data Generated:**
- User profiles with Brazilian locale (CPF, addresses)
- Account details, subscription info
- Login history and device tracking

### 3. Drivers → Railway Postgres
Generates delivery driver data directly to PostgreSQL:
```bash
docker run \
  --env-file .env \
  -v $(pwd)/postgres/drivers.json:/home/config.json \
  shadowtraffic/shadowtraffic:latest \
  --config /home/config.json
```

**Data Generated:**
- Driver profiles and contact info
- Vehicle details (license plates, types, models)
- Performance metrics (deliveries, earnings, ratings)

## Monitoring

To monitor data generation, check the container logs:
```bash
docker ps  # Get container ID
docker logs -f <container_id>
```

## Data Destinations

- **Azure Blob Storage:** `owshq-shadow-traffic` container
- **Azure SQL Database:** `owshq-mssql-dev` database, `users` table
- **Railway Postgres:** `railway` database, `drivers` table

## Troubleshooting

**Error: Connection refused**
- Verify credentials in `.env` file
- Check network connectivity to Azure/Railway

**Error: File not found**
- Ensure you're in the `gen/` directory when running commands
- Verify JSON config files exist

**Error: Permission denied**
- Check Docker is running
- Verify file permissions on config files

## Security Notes
- `.env` file is git-ignored and contains sensitive credentials
- Never commit `.env` or `st-key.env` to version control
- See `.env.template` for required environment variables structure