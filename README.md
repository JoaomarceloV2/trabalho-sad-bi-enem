# Business Intelligence e Análise OLAP - ENEM 2023

Projeto final da disciplina de Sistemas de Apoio à Decisão. O objetivo foi construir um modelo multidimensional (OLAP) e um Dashboard interativo para analisar o desempenho dos candidatos do ENEM 2023.

## Arquitetura e Modelagem
Foi utilizado um processo ETL em Python para transformar os microdados brutos em um **Esquema Estrela (Star Schema)**, otimizando a performance no Power BI.

### Tabelas Dimensionais
* **FATO_Notas:** Tabela central com métricas de desempenho (Notas por área e Média Geral).
* **DIM_Participante:** Dados demográficos (Gênero, Raça, Renda, Idade).
* **DIM_Geografia:** Dados de localização normalizados (Município, UF, Região).

## Tecnologias
* **Python 3.12:** Script ETL para tratamento de dados volumosos.
* **Pandas:** Manipulação de DataFrames e exportação CSV.
* **Microsoft Power BI:** Modelagem de dados e visualização interativa.

## Indicadores Analisados
O dashboard responde a perguntas estratégicas sobre:
1. Disparidade de notas entre Regiões e Estados.
2. Correlação entre Renda Familiar e Desempenho.
3. Comparativo entre Ensino Público e Privado.
4. Diferenças de desempenho entre Capital e Interior.

## Como Executar
1. O script `etl_enem_bi.py` processa o arquivo bruto `MICRODADOS_ENEM_2023.csv` (não incluído neste repositório devido ao tamanho).
2. O arquivo `Dashboard_ENEM_2023.pbix` contém o relatório final pronto para visualização (dados já importados).
