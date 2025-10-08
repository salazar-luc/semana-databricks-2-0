USE CATALOG ubereats;
USE SCHEMA default;

CREATE OR REPLACE TABLE items_for_vs AS
SELECT
  CAST(product_id AS STRING) AS id,
  product_name,
  cuisine_type,
  unit_price,
  modifiers,
  description_pt,
  CONCAT_WS(
    ' ',
    COALESCE(product_name, ''),
    COALESCE(cuisine_type, ''),
    COALESCE(ARRAY_JOIN(CAST(modifiers AS ARRAY<STRING>), ' '), ''),
    COALESCE(description_pt, '')
  ) AS text_for_embed
FROM mongodb_items_pt;

SELECT COUNT(*) AS n_registros FROM items_for_vs;
SELECT * FROM items_for_vs LIMIT 5;