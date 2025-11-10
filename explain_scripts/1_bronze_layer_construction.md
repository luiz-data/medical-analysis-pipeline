# Análise Detalhada do Script: `1_bronze_layer_construction.py`

Este documento fornece uma análise técnica, linha por linha, do script Python responsável pela construção da **Camada Bronze** do nosso Data Lakehouse.

## 1. Objetivo do Script

O objetivo principal deste script é automatizar a ingestão de dados brutos de múltiplos arquivos CSV para uma camada inicial no banco de dados PostgreSQL. Esta camada, conhecida como **Bronze**, serve como uma cópia fiel, padronizada e enriquecida dos dados de origem, garantindo rastreabilidade e consistência para as etapas futuras do pipeline de dados.

## 2. Estrutura do Código

### 2.1. Importações

O script começa importando as bibliotecas necessárias para sua execução.

```python
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from loguru import logger
```

-   `os`: Para interagir com o sistema operacional, principalmente para manipular caminhos de arquivos.
-   `pandas as pd`: A principal biblioteca para manipulação de dados. Usada para ler os CSVs e transformá-los em DataFrames.
-   `dotenv.load_dotenv`: Para carregar variáveis de ambiente (ex: credenciais do banco) de um arquivo `.env`, separando configuração de código.
-   `datetime`: Para gerar timestamps de execução, que são usados como metadados de auditoria.
-   `sqlalchemy`: Um toolkit de SQL para Python, usado para criar a conexão com o banco de dados (`create_engine`) e executar comandos SQL (`text`).
-   `loguru`: Uma biblioteca para logging que oferece uma formatação mais simples e poderosa que a biblioteca padrão do Python.

### 2.2. Classe `BronzeLayer`

Toda a lógica é encapsulada na classe `BronzeLayer` para manter o código organizado, coeso e reutilizável.

#### 2.2.1. Variáveis de Classe

As variáveis definidas no topo da classe servem como configurações centrais para o processo.

```python
class BronzeLayer:
    DB_SCHEMA = "bronze"
    FILES_MAP = {
        'bronze_patients': 'patients.csv',
        'bronze_claims': 'claims.csv',
        # ... e outros arquivos
    }
    COLUMN_MAP = {
        'bronze_patients': {
            'id': 'patient_id', 'birthdate': 'date_of_birth',
            # ... e outras colunas
        },
        # ... e outros mapeamentos
    }
```

-   `DB_SCHEMA`: Define o nome do *schema* no banco de dados onde as tabelas serão criadas.
-   `FILES_MAP`: Um dicionário que mapeia o nome da tabela de destino para o nome do arquivo CSV de origem. Funciona como um "roteiro" de ingestão.
-   `COLUMN_MAP`: Um dicionário que define as regras para renomear colunas específicas em cada tabela. Isso é fundamental para a padronização inicial do modelo de dados.

#### 2.2.2. Método Construtor (`__init__`)

Este método é executado quando a classe é instanciada e inicializa todos os componentes necessários.

```python
    def __init__(self, env_path: str = "./.env"):
        self._setup_logging()
        self._load_environment_variables(env_path)
        self.DATA_DIR = os.getenv("DATA_DIR")
        self.engine = self._get_db_engine()
        self.snapshot_date = datetime.today().strftime('%Y-%m-%d')
        self.execution_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.stats = {'loaded_tables': 0, 'total_records': 0, 'failed_files': []}
```

-   Inicializa o sistema de logging (`_setup_logging`).
-   Carrega as variáveis de ambiente do arquivo `.env` (`_load_environment_variables`).
-   Obtém o caminho do diretório dos dados (`DATA_DIR`).
-   Estabelece a conexão com o banco de dados (`_get_db_engine`).
-   Gera os timestamps (`snapshot_date` e `execution_timestamp`) que serão usados como metadados.
-   Cria um dicionário `stats` para coletar métricas da execução.

#### 2.2.3. Métodos de Configuração

-   `_setup_logging()`: Configura o Loguru para exibir logs no console e salvá-los em um arquivo.
-   `_load_environment_variables()`: Valida a existência do arquivo `.env` e carrega suas variáveis.
-   `_get_db_engine()`: Constrói a string de conexão do PostgreSQL e testa a conexão para garantir que o banco está acessível antes de prosseguir.

### 2.3. Funções de Carga e Validação

Esta seção contém a lógica principal de leitura, transformação e carga dos dados.

#### 2.3.1. `_read_csv_with_metadata()`

Este método lê um arquivo CSV, o padroniza e adiciona metadados.

```python
def _read_csv_with_metadata(self, filename: str, table_name: str) -> pd.DataFrame:
    # ...
    df = pd.read_csv(filepath, low_memory=False, dtype=str)
    
    # Padroniza nomes de colunas para snake_case.
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
    
    # Renomeia colunas com base no mapa definido.
    df = df.rename(columns=rename_map)
    
    # Adiciona metadados de ingestão.
    df['snapshot_date'] = pd.to_datetime(self.snapshot_date)
    df['execution_timestamp'] = pd.to_datetime(self.execution_timestamp)
    df['source_file'] = filename
    df['loaded_by'] = 'bronze_layer_script'
    # ...
```

-   **Leitura Segura**: `pd.read_csv(..., dtype=str)` lê todas as colunas como texto (`string`). Esta é uma prática defensiva crucial na camada bronze para evitar que o Pandas interprete tipos de dados de forma incorreta (ex: IDs numéricos com zeros à esquerda).
-   **Padronização de Nomes**: Converte todos os nomes de colunas para `snake_case` (minúsculas com sublinhado).
-   **Renomeação Específica**: Aplica as regras do `COLUMN_MAP` para padronizar nomes de colunas importantes.
-   **Enriquecimento com Metadados**: Adiciona quatro colunas a cada registro para garantir a rastreabilidade: data da carga, timestamp da execução, arquivo de origem e o nome do script que realizou a carga.

#### 2.3.2. `_load_dataframe_to_db()`

Este método carrega o DataFrame processado para o banco de dados.

```python
def _load_dataframe_to_db(self, df: pd.DataFrame, table_name: str, filename: str):
    # ...
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.DB_SCHEMA};"))
    # ...
    df.to_sql(table_name, self.engine, schema=self.DB_SCHEMA, if_exists='replace', index=False, chunksize=1000, method='multi')
    # ...
```

-   **Criação do Schema**: Garante que o schema `bronze` exista no banco de dados.
-   **Carregamento de Dados**: Utiliza o método `df.to_sql()` do Pandas, que é altamente eficiente para esta tarefa.
    -   `schema=self.DB_SCHEMA`: Garante que a tabela seja criada no schema correto.
    -   `if_exists='replace'`: Estratégia de carga. Se a tabela já existir, ela é totalmente apagada e recriada. Isso torna o processo idempotente, garantindo que cada execução seja uma "fotografia" limpa dos dados de origem.
    -   `index=False`: Evita que o índice do DataFrame seja salvo como uma coluna no banco.
    -   `chunksize=1000, method='multi'`: Otimizações de performance que inserem os dados em lotes de 1000 linhas, o que é significativamente mais rápido do que inserções linha a linha.

#### 2.3.3. `_validate_data_directory()`

Uma função de "pré-voo" que verifica se o diretório de dados e todos os arquivos CSV necessários existem antes de iniciar o processamento pesado, evitando falhas no meio do caminho.

### 2.4. Orquestração e Execução

#### 2.4.1. `load_bronze()`

Este é o método principal que orquestra todo o fluxo de trabalho.

```python
def load_bronze(self):
    logger.info("Iniciando processo de ingestão da Camada Bronze.")
    if not self._validate_data_directory():
        return

    for table_name, filename in self.FILES_MAP.items():
        logger.info(f"Processando arquivo: {filename} -> tabela: {self.DB_SCHEMA}.{table_name}")
        df = self._read_csv_with_metadata(filename, table_name)
        self._load_dataframe_to_db(df, table_name, filename)
    
    self._summarize_run()
    logger.info("Processo da Camada Bronze concluído.")
```

-   Ele chama os métodos auxiliares na ordem correta: validação, leitura, carga e, por fim, o resumo da execução.
-   O loop `for` itera sobre o `FILES_MAP`, processando cada arquivo de forma sequencial.

#### 2.4.2. Bloco de Execução Principal

```python
if __name__ == "__main__":
    try:
        bronze_layer = BronzeLayer(env_path="./.env")
        bronze_layer.load_bronze()
    except Exception as e:
        logger.critical(f"Ocorreu um erro fatal e inesperado: {e}", exc_info=True)
```

-   `if __name__ == "__main__":`: Garante que o código seja executado apenas quando o script é chamado diretamente.
-   `try...except`: Um bloco de tratamento de erros global que captura qualquer falha inesperada, registra um log crítico detalhado e encerra o programa de forma segura.
-   O bloco instancia a classe `BronzeLayer` e chama o método `load_bronze()` para iniciar todo o processo.
