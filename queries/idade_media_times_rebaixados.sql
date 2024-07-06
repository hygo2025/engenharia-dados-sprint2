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
        `2025_hygo`.gold.dim_clube dc
    ON
        dc.clube_id = cr.clube_id
    WHERE
        cr.posicao >= 17
)
SELECT
    cr.ano,
    FLOOR(AVG(fi.media_idade_time_titular)) AS media_idade_time_titular,
    FLOOR(AVG(fi.media_idade)) AS media_idade
FROM
    Clubes_Rebaixados cr
JOIN
    `2025_hygo`.gold.fato_idade fi
ON
    cr.clube_id = fi.clube_id
JOIN
    gold.dim_tempo dt
ON
    fi.tempo_id = dt.tempo_id
    AND cr.ano = dt.ano
GROUP BY
    cr.ano
ORDER BY
    cr.ano;
