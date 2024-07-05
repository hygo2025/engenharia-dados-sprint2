# Índice
1. [Definição do Problema](#definição-do-problema)
2. [Objetivo](#objetivo)
3. [O Projeto](#o-projeto)
   - [1. Pesquisa de Dados](#1-pesquisa-de-dados)
   - [2. Coleta de Dados](#2-coleta-de-dados)

## Definição do Problema

## Objetivo

## O Projeto

### 1. Pesquisa de Dados

A pesquisa de dados foi realizada utilizando informações disponíveis no site [Transfermarkt](https://www.transfermarkt.com), que oferece uma ampla gama de filtros para personalizar as estatísticas e classificações de clubes de futebol. Utilizei um conjunto de dados abrangente, extraído das temporadas do Campeonato Brasileiro Série A, cobrindo o período de 2006 a 2024.

**Modelagem**: Utilizei uma arquitetura do tipo [Medallion](https://www.databricks.com/glossary/medallion-architecture), onde na camada **Bronze** estão os dados crus, na camada **Silver** estão os dados processados, filtrando os campos necessários, e na camada **Gold** utilizei um modelo em esquema estrela para estruturar os dados, dada a sua eficácia na organização e análise de dados relacionais. Com a base de dados organizada em múltiplas tabelas inter-relacionadas, essa abordagem facilita a realização de consultas complexas e detalhadas, garantindo a integridade e a escalabilidade dos dados. Esse modelo permite uma análise mais profunda e estruturada das diversas métricas coletadas.

[![Medallion](images/medallion_schema.png)](https://www.databricks.com/glossary/medallion-architecture)

### 2. Coleta de Dados

A coleta de dados foi realizada acessando o site Transfermarkt e utilizando scripts personalizados para extrair informações detalhadas das temporadas do Campeonato Brasileiro Série A. Esta etapa foi essencial para garantir a precisão e a confiabilidade dos dados, uma vez que o Transfermarkt é amplamente reconhecido como uma fonte confiável e abrangente de estatísticas e informações sobre futebol.

O conjunto de dados abrange diversas métricas importantes, tais como:

- Idade média dos jogadores por clube e temporada
- Resultados dos jogos em casa e fora de casa por rodada
- Valor de mercado dos clubes por temporada
- Classificação e desempenho dos clubes por rodada

Essas métricas oferecem uma visão detalhada do desempenho e das características dos clubes ao longo das temporadas, facilitando análises comparativas e decisões informadas para treinadores, analistas esportivos, dirigentes de clubes e fãs de futebol.
