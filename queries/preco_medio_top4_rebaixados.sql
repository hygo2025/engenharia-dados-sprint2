WITH Ultima_Rodada AS (
    SELECT
        ano,
        MAX(rodada) AS ultima_rodada
    FROM
        gold.dim_tempo
    GROUP BY
        ano
),
Clube_Classificado AS (
    SELECT
        dt.ano,
        fd.clube_id,
        fd.pontos,
        ROW_NUMBER() OVER (PARTITION BY dt.ano ORDER BY fd.pontos DESC) AS posicao
    FROM
        gold.fato_desempenho fd
    JOIN
        gold.dim_tempo dt
    ON
        fd.tempo_id = dt.tempo_id
    JOIN
        Ultima_Rodada ur
    ON
        dt.ano = ur.ano AND dt.rodada = ur.ultima_rodada
),
Clubes_Rebaixados AS (
    SELECT
        cr.ano,
        cr.clube_id,
        cr.pontos,
        cr.posicao,
        dc.nome
    FROM
        Clube_Classificado cr
    JOIN
        gold.dim_clube dc
    ON
        dc.clube_id = cr.clube_id
    WHERE
        cr.posicao >= 17
),
Top4_Clubes AS (
    SELECT
        cr.ano,
        cr.clube_id,
        cr.pontos,
        cr.posicao,
        dc.nome
    FROM
        Clube_Classificado cr
    JOIN
        gold.dim_clube dc
    ON
        dc.clube_id = cr.clube_id
    WHERE
        cr.posicao <= 4
)
SELECT
    cr.ano,
    'Rebaixados' AS tipo,
    FLOOR(AVG(fv.valor_mercado_euros)) AS preco_medio
FROM
    Clubes_Rebaixados cr
JOIN
    gold.fato_valor_mercado fv
ON
    cr.clube_id = fv.clube_id
JOIN
    gold.dim_tempo dt
ON
    fv.tempo_id = dt.tempo_id
    AND cr.ano = dt.ano
GROUP BY
    cr.ano
UNION ALL
SELECT
    cr.ano,
    'Top 4' AS tipo,
    FLOOR(AVG(fv.valor_mercado_euros)) AS preco_medio
FROM
    Top4_Clubes cr
JOIN
    gold.fato_valor_mercado fv
ON
    cr.clube_id = fv.clube_id
JOIN
    gold.dim_tempo dt
ON
    fv.tempo_id = dt.tempo_id
    AND cr.ano = dt.ano
GROUP BY
    cr.ano
ORDER BY
    ano, tipo;
