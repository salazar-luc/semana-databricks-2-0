#!/bin/bash
set -e

echo "Creating ShadowTraffic configuration files from templates..."

if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please create .env file with your credentials first."
    exit 1
fi

AZURE_STORAGE_CONNECTION_STRING=$(grep '^AZURE_STORAGE_CONNECTION_STRING=' .env | cut -d'=' -f2-)
MSSQL_HOST=$(grep '^MSSQL_HOST=' .env | cut -d'=' -f2-)
MSSQL_DB=$(grep '^MSSQL_DB=' .env | cut -d'=' -f2-)
MSSQL_USERNAME=$(grep '^MSSQL_USERNAME=' .env | cut -d'=' -f2-)
MSSQL_PASSWORD=$(grep '^MSSQL_PASSWORD=' .env | cut -d'=' -f2-)
POSTGRES_HOST=$(grep '^POSTGRES_HOST=' .env | cut -d'=' -f2-)
POSTGRES_DB=$(grep '^POSTGRES_DB=' .env | cut -d'=' -f2-)
POSTGRES_USERNAME=$(grep '^POSTGRES_USERNAME=' .env | cut -d'=' -f2-)
POSTGRES_PASSWORD=$(grep '^POSTGRES_PASSWORD=' .env | cut -d'=' -f2-)

echo "Copying template files..."
cp azure/uber-eats.json.template azure/uber-eats.json
cp mssql/users.json.template mssql/users.json
cp postgres/drivers.json.template postgres/drivers.json

echo "Configuring azure/uber-eats.json..."
sed -i.bak "s|REPLACE_WITH_AZURE_STORAGE_CONNECTION_STRING|${AZURE_STORAGE_CONNECTION_STRING}|g" azure/uber-eats.json && rm azure/uber-eats.json.bak

echo "Configuring mssql/users.json..."
sed -i.bak "s|REPLACE_WITH_MSSQL_HOST|${MSSQL_HOST}|g" mssql/users.json && rm mssql/users.json.bak
sed -i.bak "s|REPLACE_WITH_MSSQL_DB|${MSSQL_DB}|g" mssql/users.json && rm mssql/users.json.bak
sed -i.bak "s|REPLACE_WITH_MSSQL_USERNAME|${MSSQL_USERNAME}|g" mssql/users.json && rm mssql/users.json.bak
sed -i.bak "s|REPLACE_WITH_MSSQL_PASSWORD|${MSSQL_PASSWORD}|g" mssql/users.json && rm mssql/users.json.bak

echo "Configuring postgres/drivers.json..."
sed -i.bak "s|REPLACE_WITH_POSTGRES_HOST|${POSTGRES_HOST}|g" postgres/drivers.json && rm postgres/drivers.json.bak
sed -i.bak "s|REPLACE_WITH_POSTGRES_DB|${POSTGRES_DB}|g" postgres/drivers.json && rm postgres/drivers.json.bak
sed -i.bak "s|REPLACE_WITH_POSTGRES_USERNAME|${POSTGRES_USERNAME}|g" postgres/drivers.json && rm postgres/drivers.json.bak
sed -i.bak "s|REPLACE_WITH_POSTGRES_PASSWORD|${POSTGRES_PASSWORD}|g" postgres/drivers.json && rm postgres/drivers.json.bak

echo ""
echo "✅ Configuration files created successfully!"
echo ""
echo "Files created:"
echo "  - azure/uber-eats.json"
echo "  - mssql/users.json"
echo "  - postgres/drivers.json"
echo ""
echo "These files contain your credentials and are git-ignored."
echo "You can now run the ShadowTraffic generators."
