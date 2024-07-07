SELECT 'dim_clube' AS tabela, COUNT(*) AS quantidade FROM dim_clube
UNION ALL
SELECT 'dim_tempo' AS tabela, COUNT(*) AS quantidade FROM dim_tempo
UNION ALL
SELECT 'fato_jogos' AS tabela, COUNT(*) AS quantidade FROM fato_jogos
UNION ALL
SELECT 'fato_desempenho' AS tabela, COUNT(*) AS quantidade FROM fato_desempenho
UNION ALL
SELECT 'fato_valor_mercado' AS tabela, COUNT(*) AS quantidade FROM fato_valor_mercado
UNION ALL
SELECT 'fato_idade' AS tabela, COUNT(*) AS quantidade FROM fato_idade;
