WITH Ultima_Rodada AS (SELECT ano,
                              MAX(rodada) AS ultima_rodada
                       FROM gold.dim_tempo
                       GROUP BY ano),
     Clube_Classificado AS (SELECT dt.ano,
                                   fd.clube_id,
                                   fd.pontos,
                                   ROW_NUMBER() OVER (PARTITION BY dt.ano ORDER BY fd.pontos DESC) AS posicao
                            FROM gold.fato_desempenho fd
                                     JOIN
                                 gold.dim_tempo dt
                                 ON
                                     fd.tempo_id = dt.tempo_id
                                     JOIN
                                 Ultima_Rodada ur
                                 ON
                                     dt.ano = ur.ano AND dt.rodada = ur.ultima_rodada)
SELECT ano,
       MIN(pontos) AS pontos_minimos_para_escapar
FROM Clube_Classificado
WHERE posicao = 16 -- 16º clube, ou seja, o primeiro clube fora da zona de rebaixamento
  and ano < 2024
GROUP BY ano
ORDER BY ano;
