USE CATALOG ubereats;
USE SCHEMA default;

--
CREATE OR REPLACE TABLE kafka_search_pt AS
SELECT *
FROM read_files(
  '/Volumes/ubereats/default/vol-uber-eats-text-files/kafka_search_pt.json',
  format => 'json'          
);


--
CREATE OR REPLACE TABLE mongodb_items_pt AS
SELECT *
FROM read_files(
  '/Volumes/ubereats/default/vol-uber-eats-text-files/mongodb_items_pt.json',
  format => 'json'
);

--
CREATE OR REPLACE TABLE kafka_orders_pt AS
SELECT *
FROM read_files(
  '/Volumes/ubereats/default/vol-uber-eats-text-files/kafka_orders_pt.json',
  format => 'json'
);

--
SELECT COUNT(*) FROM kafka_search_pt;
SELECT COUNT(*) FROM mongodb_items_pt;
SELECT COUNT(*) FROM kafka_orders_pt;

SELECT * FROM kafka_search_pt LIMIT 5;
SELECT * FROM mongodb_items_pt LIMIT 5;
SELECT * FROM kafka_orders_pt LIMIT 5;