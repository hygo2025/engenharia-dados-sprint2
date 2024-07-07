-- Verificar duplicatas em 'dim_clube'
SELECT clube_id, COUNT(*)
FROM dim_clube
GROUP BY clube_id
HAVING COUNT(*) > 1;

-- Verificar duplicatas em 'dim_tempo'
SELECT tempo_id, COUNT(*)
FROM dim_tempo
GROUP BY tempo_id
HAVING COUNT(*) > 1;

-- Verificar duplicatas em 'fato_jogos'
SELECT jogo_id, COUNT(*)
FROM fato_jogos
GROUP BY jogo_id
HAVING COUNT(*) > 1;

-- Verificar duplicatas em 'fato_desempenho'
SELECT desempenho_id, COUNT(*)
FROM fato_desempenho
GROUP BY desempenho_id
HAVING COUNT(*) > 1;

-- Verificar duplicatas em 'fato_valor_mercado'
SELECT valor_mercado_id, COUNT(*)
FROM fato_valor_mercado
GROUP BY valor_mercado_id
HAVING COUNT(*) > 1;

-- Verificar duplicatas em 'fato_idade'
SELECT idade_id, COUNT(*)
FROM fato_idade
GROUP BY idade_id
HAVING COUNT(*) > 1;
