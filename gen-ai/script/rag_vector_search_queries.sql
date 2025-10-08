/*
1) Similaridade (texto → ANN)
Esse é o modo clássico de Approximate Nearest Neighbors (ANN). Você fornece um texto de consulta, 
o Databricks gera o embedding desse texto e procura no índice vetorial os itens mais próximos em 
termos de semântica. É ótimo para perguntas abertas como “pizza vegetariana sem glúten” porque vai 
trazer resultados que podem não bater exatamente nas palavras, mas que têm o mesmo significado ou contexto.
*/

SELECT
  i.id,
  i.product_name,
  i.cuisine_type,
  i.unit_price,
  vs.search_score AS similarity
FROM VECTOR_SEARCH(
  index => 'ubereats.default.idxitemspt',
  query_text => 'pizza vegetariana sem glúten barata',
  num_results => 5
) AS vs
JOIN items_for_vs i
  ON vs.id = i.id
ORDER BY similarity DESC;

/*
2) Hybrid (keyword + vetorial)
No modo híbrido, o Databricks combina dois mundos: a busca lexical (palavras-chave exatas, tipo um “ctrl+F inteligente”) 
com a busca vetorial (similaridade semântica). Isso é útil quando você quer garantir que termos específicos da 
query sejam respeitados, mas também quer aproveitar a flexibilidade da semântica. 
Exemplo: “pizza napolitana sem glúten” → garante que “napolitana” apareça, mas ainda assim avalia embeddings para 
pegar resultados relacionados.
*/
SELECT
  i.id, i.product_name, i.cuisine_type, i.unit_price,
  vs.search_score AS similarity
FROM VECTOR_SEARCH(
  index => 'ubereats.default.idxitemspt',
  query_text => 'pizza napolitana sem glúten',
  query_type => 'HYBRID',
  num_results => 5
) AS vs
JOIN items_for_vs i ON vs.id = i.id
ORDER BY similarity DESC;