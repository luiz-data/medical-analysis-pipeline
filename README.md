# Projeto de Pipeline de Dados: Arquitetura Medallion para Análise Financeira de Saúde

## 🚀 Visão Geral do Projeto

Este projeto implementa um pipeline de dados robusto usando a **Arquitetura Medallion** (Bronze, Silver, Gold) para processar e refinar dados financeiros de saúde. O objetivo é transformar dados brutos de diversas fontes CSV em informações estruturadas, limpas e prontas para análise, oferecendo insights sobre a performance de pagadoras, sumarizações de pacientes e detalhes de encontros clínicos.

A arquitetura Medallion garante que os dados passem por estágios progressivos de validação, limpeza e agregação, resultando em uma fonte de dados confiável e de alta qualidade para consumo por dashboards, relatórios e análises mais aprofundadas.

## ✨ Arquitetura Medallion Explicada

A arquitetura Medallion organiza os dados em três camadas distintas, cada uma com um propósito específico:

1.  **Bronze (Raw Data):**
    *   **Propósito:** Ingestão de dados brutos e inalterados diretamente das fontes.
    *   **Características:** Dados com formato original, incluindo metadados de ingestão. Foco principal em persistir os dados como vieram.
    *   **Implementação:** O script `1_bronze_layer_construction.py` é responsável por ler arquivos CSV, padronizar nomes de colunas para snake_case e adicionar metadados de execução antes de carregar para o schema `bronze` no PostgreSQL.

2.  **Silver (Cleaned & Conformed Data):**
    *   **Propósito:** Limpeza, padronização, validação e enriquecimento dos dados.
    *   **Características:** Dados de alta qualidade, prontos para análise detalhada. Problemas como inconsistências de tipo, valores nulos e dados duplicados são tratados.
    *   **Implementação:** O script `2_silver_layer_construction.py` extrai dados da camada Bronze, aplica regras de validação rigorosas usando **Pandera**, padroniza formatos, calcula campos derivados (e.g., idade, tempo de internação) e enriquece os dados (e.g., informações de pagadores aos pacientes), carregando-os no schema `silver` do PostgreSQL.

3.  **Gold (Curated & Aggregated Data):**
    *   **Propósito:** Agregação e sumarização dos dados para casos de uso de negócio específicos.
    *   **Características:** Dados otimizados para consumo por aplicações de BI, aprendizado de máquina e relatórios de alto nível.
    *   **Implementação:** O script `3_gold_layer_construction.py` consome os dados limpos da camada Silver para criar tabelas sumarizadas (e.g., resumo mensal de pacientes, performance de pagadoras, sumarização de encontros), aplicando novamente validações **Pandera** e carregando-as no schema `gold` do PostgreSQL.

## 📊 Estrutura das Tabelas

### Camada Bronze (`bronze` schema)

Esta camada contém os dados brutos dos arquivos CSV, com nomes de colunas padronizados para `snake_case` e metadados de ingestão adicionados.

### Camada Silver (`silver` schema)

### Camada Gold (`gold` schema)

Esta camada contém dados agregados e sumarizados, otimizados para consumo por aplicações de BI e relatórios de negócio.

## 🛠️ Tecnologias Utilizadas

*   **Python 3.x:** Linguagem de programação principal.
*   **Pandas:** Para manipulação e transformação de dados em DataFrames.
*   **Pandera:** Para validação de schemas de DataFrames, garantindo a qualidade e consistência dos dados em cada camada.
*   **SQLAlchemy:** Para ORM e abstração de conexão com o banco de dados.
*   **Psycopg2:** Adaptador PostgreSQL para Python.
*   **Loguru:** Para logging robusto e formatado.
*   **python-dotenv:** Para gerenciar variáveis de ambiente de forma segura.
*   **PostgreSQL:** Banco de dados relacional para armazenamento das camadas Bronze, Silver e Gold.
