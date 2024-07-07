-- Verificar valores nulos em 'dim_clube'
SELECT COUNT(*) AS null_clube_id
FROM dim_clube
WHERE clube_id IS NULL;

-- Verificar valores nulos em 'dim_tempo'
SELECT COUNT(*) AS null_tempo_id
FROM dim_tempo
WHERE tempo_id IS NULL;

-- Verificar valores nulos em 'fato_jogos'
SELECT COUNT(*) AS null_jogo_id, COUNT(*) AS null_clube_mandante_id, COUNT(*) AS null_clube_visitante_id, COUNT(*) AS null_tempo_id
FROM fato_jogos
WHERE jogo_id IS NULL OR clube_mandante_id IS NULL OR clube_visitante_id IS NULL OR tempo_id IS NULL;

-- Verificar valores nulos em 'fato_desempenho'
SELECT COUNT(*) AS null_desempenho_id, COUNT(*) AS null_clube_id, COUNT(*) AS null_tempo_id
FROM fato_desempenho
WHERE desempenho_id IS NULL OR clube_id IS NULL OR tempo_id IS NULL;

-- Verificar valores nulos em 'fato_valor_mercado'
SELECT COUNT(*) AS null_valor_mercado_id, COUNT(*) AS null_clube_id, COUNT(*) AS null_tempo_id
FROM fato_valor_mercado
WHERE valor_mercado_id IS NULL OR clube_id IS NULL OR tempo_id IS NULL;

-- Verificar valores nulos em 'fato_idade'
SELECT COUNT(*) AS null_idade_id, COUNT(*) AS null_clube_id, COUNT(*) AS null_tempo_id
FROM fato_idade
WHERE idade_id IS NULL OR clube_id IS NULL OR tempo_id IS NULL;
