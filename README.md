# Índice
1. [Definição do Problema](#definição-do-problema)
2. [Objetivo](#objetivo)
3. [O Projeto](#o-projeto)
   - [1. Pesquisa de Dados](#1-pesquisa-de-dados)


## Definição do Problema

## Objetivo

## O Projeto
### 1. Pesquisa de Dados
A pesquisa de dados foi realizada utilizando informações disponíveis no site [Transfermarkt](https://www.transfermarkt.com), que oferece uma ampla gama de filtros para personalizar as estatísticas e classificações de clubes de futebol. Para garantir simplicidade e uniformidade na análise, utilizei um conjunto de dados coletado diretamente do site através de scrapers personalizados, que compilam informações detalhadas das temporadas do Campeonato Brasileiro Série A, entre 2006 até 2024.

**Modelagem**: Vou utilizar uma arquitetura do tipo [Medallion](https://www.databricks.com/glossary/medallion-architecture), onde na camada **Bronze** vão ter os dados crus, na camada **Silver** os dados processados filtrando os campos necessarios e na cama **Gold** vou utilizar um modelo em esquema estrela para estruturar os dados, dada a sua eficácia na organização e análise de dados relacionais. Com a base de dados organizada em múltiplas tabelas inter-relacionadas, essa abordagem facilita a realização de consultas complexas e detalhadas, garantindo a integridade e a escalabilidade dos dados. Esse modelo permite uma análise mais profunda e estruturada das diversas métricas coletadas.

[![Medallion](images/medallion_schema.png)](https://www.databricks.com/glossary/medallion-architecture)