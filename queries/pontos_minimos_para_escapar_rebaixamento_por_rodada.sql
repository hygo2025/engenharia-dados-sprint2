WITH Rodada_Info AS (SELECT ano,
                            rodada,
                            clube_id,
                            pontos,
                            ROW_NUMBER() OVER (PARTITION BY ano, rodada ORDER BY pontos DESC) AS posicao
                     FROM gold.fato_desempenho fd
                              JOIN
                          gold.dim_tempo dt
                          ON
                              fd.tempo_id = dt.tempo_id),
     Pontos_Minimos AS (SELECT ano,
                               rodada,
                               MIN(pontos) AS pontos_minimos_para_escapar
                        FROM Rodada_Info
                        WHERE posicao = 16 -- 16º clube, ou seja, o primeiro clube fora da zona de rebaixamento
                        GROUP BY ano,
                                 rodada)
SELECT ano,
       rodada,
       pontos_minimos_para_escapar
FROM Pontos_Minimos
WHERE ano < 2024
ORDER BY ano,
         rodada;
