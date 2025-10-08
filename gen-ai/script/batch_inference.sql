USE CATALOG ubereats;
USE SCHEMA default;

--
CREATE OR REPLACE TABLE gold_search_intents AS
SELECT
  search_id,
  user_id,
  query_text,
  ai_query(
    'databricks-gpt-oss-120b',
    'Classifique a intenção desta busca em UMA das categorias fixas: [tipo_de_comida, restricao_alimentar, preco, rapidez, promocao, saudavel, sobremesa, regional, brunch, outro]. Retorne JSON estrito: {"intent":"...","rationale":"<máx. 20 palavras>"}. Texto: ' || query_text,
    responseFormat => '{
      "type":"json_schema",
      "json_schema":{"name":"intent_schema","schema":{
        "type":"object",
        "properties":{
          "intent":{"type":"string"},
          "rationale":{"type":"string"}
        },
        "required":["intent"]
      }}
    }'
  ) AS intent_json
FROM kafka_search_pt
LIMIT 1000;

--
USE CATALOG ubereats;
USE SCHEMA default;

CREATE OR REPLACE TABLE gold_items_enriched AS
SELECT
  product_id,
  product_name,
  cuisine_type,
  unit_price,
  modifiers,
  description_pt,
  ai_query(
    'databricks-gpt-oss-120b',
    'Reescreva a descrição em PT-BR para cardápio digital (máx. 3 frases), gere 3 tags curtas e um aviso de alérgenos, se houver. Retorne JSON: {"descricao_marketing":"...", "tags":["...","...","..."], "aviso_alergenos":"..."}. Texto: ' || description_pt,
    responseFormat => '{
      "type":"json_schema",
      "json_schema":{"name":"menu_schema","schema":{
        "type":"object",
        "properties":{
          "descricao_marketing":{"type":"string"},
          "tags":{"type":"array","items":{"type":"string"}},
          "aviso_alergenos":{"type":"string"}
        },
        "required":["descricao_marketing","tags"]
      }}
    }'
  ) AS menu_json
FROM mongodb_items_pt
LIMIT 1000;

--
CREATE OR REPLACE TABLE gold_orders_normalized AS
SELECT
  order_id,
  order_date,
  payment,
  items,
  ai_query(
    'databricks-gpt-oss-120b',
    'A partir do status_history, mapeie o estado final para um destes enums: [PLACED, PAID, PREPARING, ON_ROUTE, DELIVERED, REFUNDED]. Retorne JSON: {"canonical_status":"...", "timeline_resumo":"<máx. 25 palavras>"}. Texto: ' || CAST(status_history AS STRING),
    responseFormat => '{
      "type":"json_schema",
      "json_schema":{"name":"status_schema","schema":{
        "type":"object",
        "properties":{
          "canonical_status":{"type":"string"},
          "timeline_resumo":{"type":"string"}
        },
        "required":["canonical_status"]
      }}
    }'
  ) AS status_json
FROM kafka_orders_pt
LIMIT 1000;

--
SELECT *
FROM gold_search_intents;

SELECT *
FROM mongodb_items_pt;

SELECT *
FROM gold_orders_normalized;