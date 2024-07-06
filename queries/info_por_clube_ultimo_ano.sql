WITH Ultima_Rodada AS (
    SELECT
        ano,
        MAX(rodada) AS ultima_rodada
    FROM
        gold.dim_tempo
    GROUP BY
        ano
),
Time_Dados AS (
    SELECT
        dt.ano,
        ur.ultima_rodada AS rodada,
        dc.nome AS clube_nome,
        SUM(fv.valor_mercado_euros) AS preco_total,
        AVG(fi.media_idade_time_titular) AS media_idade_time_titular,
        AVG(fi.media_idade) AS media_idade,
        fd.pontos,
        ROW_NUMBER() OVER (PARTITION BY dt.ano ORDER BY fd.pontos DESC) AS posicao
    FROM
        gold.dim_clube dc
    JOIN
        gold.fato_valor_mercado fv
    ON
        dc.clube_id = fv.clube_id
    JOIN
        gold.fato_idade fi
    ON
        dc.clube_id = fi.clube_id
    JOIN
        gold.fato_desempenho fd
    ON
        dc.clube_id = fd.clube_id
    JOIN
        gold.dim_tempo dt
    ON
        fv.tempo_id = dt.tempo_id
        AND fi.tempo_id = dt.tempo_id
        AND fd.tempo_id = dt.tempo_id
    JOIN
        Ultima_Rodada ur
    ON
        dt.ano = ur.ano
        AND dt.rodada = ur.ultima_rodada
    WHERE
        dt.ano = (SELECT MAX(ano) FROM gold.dim_tempo)
    GROUP BY
        dt.ano,
        ur.ultima_rodada,
        dc.nome,
        fd.pontos
)
SELECT
    ano,
    rodada,
    clube_nome,
    FLOOR(preco_total) AS preco_total,
    FLOOR(media_idade_time_titular) AS media_idade_time_titular,
    FLOOR(media_idade) AS media_idade,
    pontos,
    posicao
FROM
    Time_Dados
ORDER BY
    ano DESC,
    posicao ASC;
