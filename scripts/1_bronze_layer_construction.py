# 1_bronze_layer_construction.py

# 1_bronze_layer_construction.py

import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from loguru import logger

class BronzeLayer:
    """
    Orquestra a ingestão de dados brutos de arquivos CSV para a Camada Bronze,
    padronizando nomes de colunas e adicionando metadados de rastreabilidade.
    """

    # ==============================================================================
    # --- SEÇÃO 1: Variáveis e Funções de Conexão ---
    # ==============================================================================

    DB_SCHEMA = "bronze"
    FILES_MAP = {
        'bronze_patients': 'patients.csv',
        'bronze_claims': 'claims.csv',
        'bronze_claims_transactions': 'claims_transactions.csv',
        'bronze_encounters': 'encounters.csv',
        'bronze_payers': 'payers.csv'
    }
    COLUMN_MAP = {
        'bronze_patients': {
            'id': 'patient_id', 'birthdate': 'date_of_birth', 'first': 'first_name', 'last': 'last_name',
        },
        'bronze_claims': {
            'id': 'claim_id', 'patientid': 'patient_id', 'providerid': 'provider_id',
            'servicedate': 'claim_start_date', 'lastbilleddatep': 'claim_end_date',
            'outstanding1': 'outstanding_primary', 'outstanding2': 'outstanding_secondary', 'outstandingp': 'outstanding_patient',
        },
        'bronze_claims_transactions': {
            # Mapeia 'chargeid' (UUID) para 'transaction_id' para garantir unicidade.
            'chargeid': 'transaction_id', 'claimid': 'claim_id', 'patientid': 'patient_id', 'providerid': 'provider_id',
            'fromdate': 'transaction_date', 'amount': 'transaction_amount', 'procedurecode': 'procedure_code',
        },
        'bronze_encounters': {
            'id': 'encounter_id', 'start': 'encounter_date', 'stop': 'discharge_date',
            'patient': 'patient_id', 'provider': 'provider_id', 'payer': 'payer_id',
            'encounterclass': 'encounter_type', 'total_claim_cost': 'total_claim_cost', 'payer_coverage': 'payer_coverage',
        },
        'bronze_payers': {
            'id': 'payer_id', 'name': 'payer_name'
        }
    }

    def __init__(self, env_path: str = "./.env"):
        self._setup_logging()
        self._load_environment_variables(env_path)
        self.DATA_DIR = os.getenv("DATA_DIR")
        self.engine = self._get_db_engine()
        self.snapshot_date = datetime.today().strftime('%Y-%m-%d')
        self.execution_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.stats = {'loaded_tables': 0, 'total_records': 0, 'failed_files': []}
        
    def _setup_logging(self):
        logger.remove()
        logger.add("bronze_layer.log", rotation="500 MB", level="INFO")
        logger.add(lambda msg: print(msg, end=""), colorize=True, level="INFO", format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

    def _load_environment_variables(self, env_path: str):
        if not os.path.exists(env_path):
            raise FileNotFoundError(f"Arquivo .env não encontrado em: {env_path}")
        load_dotenv(dotenv_path=env_path, override=True)
        logger.info("Variáveis de ambiente carregadas.")

    def _get_db_engine(self):
        try:
            url = f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASS')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
            engine = create_engine(url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Conexão com banco de dados PostgreSQL estabelecida com sucesso.")
            return engine
        except Exception as e:
            logger.error(f"Falha ao conectar ao banco de dados: {e}")
            raise

    # ==============================================================================
    # --- SEÇÃO 2: Funções de Carga e Validação de Dados ---
    # ==============================================================================

    def _read_csv_with_metadata(self, filename: str, table_name: str) -> pd.DataFrame:
        filepath = os.path.join(self.DATA_DIR, filename)
        try:
            # Lê todas as colunas como string para máxima flexibilidade.
            df = pd.read_csv(filepath, low_memory=False, dtype=str)
            
            # Padroniza nomes de colunas para snake_case.
            df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
            
            # Renomeia colunas com base no mapa definido.
            rename_map = self.COLUMN_MAP.get(table_name, {})
            df = df.rename(columns=rename_map)
            logger.info(f"Colunas para '{table_name}' padronizadas.")
            
            # Adiciona metadados de ingestão.
            df['snapshot_date'] = pd.to_datetime(self.snapshot_date)
            df['execution_timestamp'] = pd.to_datetime(self.execution_timestamp)
            df['source_file'] = filename
            df['loaded_by'] = 'bronze_layer_script'
            
            logger.info(f"Arquivo '{filename}' lido. Registros: {len(df):,}, Colunas: {len(df.columns)}.")
            return df
        except Exception as e:
            logger.error(f"Erro ao ler ou processar o arquivo '{filename}': {e}")
            self.stats['failed_files'].append(filename)
            return pd.DataFrame()

    def _load_dataframe_to_db(self, df: pd.DataFrame, table_name: str, filename: str):
        if df.empty:
            logger.warning(f"DataFrame vazio para '{filename}'. Pulando carga.")
            return
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.DB_SCHEMA};"))
                conn.commit()
            
            df.to_sql(table_name, self.engine, schema=self.DB_SCHEMA, if_exists='replace', index=False, chunksize=1000, method='multi')
            logger.info(f"Dados de '{filename}' carregados para {self.DB_SCHEMA}.{table_name}.")
            self.stats['loaded_tables'] += 1
            self.stats['total_records'] += len(df)
        except Exception as e:
            logger.error(f"Erro inesperado ao carregar '{filename}': {e}", exc_info=True)
            self.stats['failed_files'].append(filename)

    def _validate_data_directory(self) -> bool:
        if not self.DATA_DIR or not os.path.exists(self.DATA_DIR):
            logger.error(f"Diretório de dados inválido ou não encontrado: {self.DATA_DIR}")
            return False
        missing_files = [f for _, f in self.FILES_MAP.items() if not os.path.exists(os.path.join(self.DATA_DIR, f))]
        if missing_files:
            logger.error(f"Arquivo(s) CSV faltando: {', '.join(missing_files)}")
            return False
        logger.info(f"Diretório de dados e arquivos validados em: {self.DATA_DIR}.")
        return True

    # ==============================================================================
    # --- SEÇÃO 3: Função Principal de Carga da Camada Bronze ---
    # ==============================================================================

    def load_bronze(self):
        """
        Orquestra todo o processo da camada Bronze: validação,
        leitura dos CSVs, padronização e carga para o banco de dados.
        """
        logger.info("Iniciando processo de ingestão da Camada Bronze.")
        if not self._validate_data_directory():
            return

        for table_name, filename in self.FILES_MAP.items():
            logger.info(f"Processando arquivo: {filename} -> tabela: {self.DB_SCHEMA}.{table_name}")
            df = self._read_csv_with_metadata(filename, table_name)
            self._load_dataframe_to_db(df, table_name, filename)
        
        self._summarize_run()
        logger.info("Processo da Camada Bronze concluído.")

    def _summarize_run(self):
        logger.info("=" * 60)
        logger.info("RESUMO DA CARGA BRONZE")
        logger.info("=" * 60)
        logger.info(f"Tabelas carregadas com sucesso: {self.stats['loaded_tables']}/{len(self.FILES_MAP)}")
        logger.info(f"Total de registros carregados: {self.stats['total_records']:,}")
        if self.stats['failed_files']:
            logger.warning(f"Arquivos com falha: {', '.join(self.stats['failed_files'])}")
        else:
            logger.info("Todos os arquivos processados com sucesso!")
        logger.info("=" * 60)

# ==============================================================================
# --- Bloco de Execução Principal ---
# ==============================================================================

if __name__ == "__main__":
    try:
        bronze_layer = BronzeLayer(env_path="./.env")
        bronze_layer.load_bronze()
    except Exception as e:
        logger.critical(f"Ocorreu um erro fatal e inesperado: {e}", exc_info=True)