CREATE OR REFRESH STREAMING TABLE semana.default.bronze_mssql_users(
  CONSTRAINT no_rescued_data
  EXPECT (_rescued_data IS NULL)
)
COMMENT "Raw MSSQL users data from shadow traffic volume. Bronze layer - no transformations applied."
TBLPROPERTIES (
  "quality" = "bronze",
  "source_system" = "mssql"
)
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/semana/default/vol-owshq-shadow-traffic/mssql_users_*',
  format => 'json'
);

CREATE OR REFRESH STREAMING TABLE semana.default.bronze_postgres_drivers(
  CONSTRAINT no_rescued_data
  EXPECT (_rescued_data IS NULL)
)
COMMENT "Raw PostgreSQL drivers data from shadow traffic volume. Bronze layer - no transformations applied."
TBLPROPERTIES (
  "quality" = "bronze",
  "source_system" = "postgres"
)
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/semana/default/vol-owshq-shadow-traffic/postgres_drivers_*',
  format => 'json'
);

CREATE OR REFRESH STREAMING TABLE semana.default.bronze_mysql_restaurants(
  CONSTRAINT no_rescued_data
  EXPECT (_rescued_data IS NULL)
)
COMMENT "Raw MySQL restaurants data from shadow traffic volume. Bronze layer - no transformations applied."
TBLPROPERTIES (
  "quality" = "bronze",
  "source_system" = "mysql"
)
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/semana/default/vol-owshq-shadow-traffic/mysql_restaurants_*',
  format => 'json'
);

CREATE OR REFRESH STREAMING TABLE semana.default.bronze_kafka_orders(
  CONSTRAINT no_rescued_data
  EXPECT (_rescued_data IS NULL)
)
COMMENT "Raw Kafka orders data from shadow traffic volume. Bronze layer - no transformations applied."
TBLPROPERTIES (
  "quality" = "bronze",
  "source_system" = "kafka"
)
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/semana/default/vol-owshq-shadow-traffic/kafka_orders_*',
  format => 'json'
);

CREATE OR REFRESH STREAMING TABLE semana.default.bronze_kafka_status(
  CONSTRAINT no_rescued_data
  EXPECT (_rescued_data IS NULL)
)
COMMENT "Raw Kafka status data from shadow traffic volume. Bronze layer - no transformations applied."
TBLPROPERTIES (
  "quality" = "bronze",
  "source_system" = "kafka"
)
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/semana/default/vol-owshq-shadow-traffic/kafka_status_*',
  format => 'json'
);

