-- ============================================================================
-- BRONZE LAYER: Raw Orders Ingestion
-- ============================================================================
-- Purpose: Ingest raw Kafka order events with Auto Loader
-- Source: JSON files from Volumes
-- Target: bronze_orders
-- Demo: Shows streaming ingestion with basic quality tracking
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_orders(
  CONSTRAINT no_rescued_data
  EXPECT (_rescued_data IS NULL)
)
COMMENT "Raw Kafka orders from JSON files. Bronze layer - preserves all data as-is with metadata tracking."
TBLPROPERTIES (
  "quality" = "bronze",
  "source_system" = "kafka",
  "layer" = "raw_ingestion"
)
AS SELECT
  -- Raw order fields (preserved as-is)
  order_id,
  order_date,
  total_amount,
  user_key,
  driver_key,
  restaurant_key,
  payment_key,
  rating_key,
  dt_current_timestamp,

  -- Auto Loader metadata for lineage tracking
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS ingestion_time,

  -- Rescued data column (tracks malformed JSON)
  _rescued_data
FROM STREAM read_files(
  '/Volumes/semana/default/vol-owshq-shadow-traffic/kafka_orders_*',
  format => 'json'
);
