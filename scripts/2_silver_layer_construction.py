# 2_silver_layer_construction.py

import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from loguru import logger
import pandera.pandas as pa
from pandera.typing import DataFrame, Series

# ==============================================================================
# --- SCHEMAS PANDERA PARA A CAMADA SILVER ---
# ==============================================================================

class SilverPatientSchema(pa.DataFrameModel):
    patient_id: Series[str] = pa.Field(unique=True)
    first_name: Series[str] = pa.Field(nullable=False)
    last_name: Series[str] = pa.Field(nullable=False)
    date_of_birth: Series[pa.DateTime] = pa.Field(nullable=False)
    gender: Series[str] = pa.Field(isin=['Male', 'Female', 'Other', 'Unknown'])
    age: Series[int] = pa.Field(ge=0, le=120, coerce=True)
    payer_id: Series[str] = pa.Field(nullable=True)
    payer_name: Series[str] = pa.Field(nullable=False)
    silver_processing_timestamp: Series[pa.DateTime] = pa.Field(nullable=False, coerce=True)

class SilverClaimsSchema(pa.DataFrameModel):
    claim_id: Series[str] = pa.Field(unique=True)
    patient_id: Series[str] = pa.Field()
    provider_id: Series[str] = pa.Field(nullable=True)
    claim_start_date: Series[pa.DateTime] = pa.Field(nullable=False, coerce=True)
    claim_end_date: Series[pa.DateTime] = pa.Field(nullable=True, coerce=True)
    total_billed_amount: Series[float] = pa.Field(ge=0, coerce=True)
    total_paid_amount: Series[float] = pa.Field(ge=0, coerce=True)
    patient_responsibility_amount: Series[float] = pa.Field(ge=0, coerce=True)
    silver_processing_timestamp: Series[pa.DateTime] = pa.Field(nullable=False, coerce=True)

class SilverClaimsTransactionsSchema(pa.DataFrameModel):
    transaction_id: Series[str] = pa.Field(unique=True)
    claim_id: Series[str] = pa.Field()
    transaction_date: Series[pa.DateTime] = pa.Field(nullable=False, coerce=True)
    transaction_amount: Series[float] = pa.Field(coerce=True)
    procedure_code: Series[str] = pa.Field(nullable=True)
    transaction_type: Series[str] = pa.Field(nullable=True)
    silver_processing_timestamp: Series[pa.DateTime] = pa.Field(nullable=False, coerce=True)

class SilverEncountersSchema(pa.DataFrameModel):
    encounter_id: Series[str] = pa.Field(unique=True)
    patient_id: Series[str] = pa.Field()
    provider_id: Series[str] = pa.Field(nullable=True)
    payer_id: Series[str] = pa.Field(nullable=True)
    encounter_date: Series[pa.DateTime] = pa.Field(nullable=False, coerce=True)
    discharge_date: Series[pa.DateTime] = pa.Field(nullable=True, coerce=True)
    encounter_type: Series[str] = pa.Field(nullable=True)
    length_of_stay_days: Series[int] = pa.Field(ge=0, coerce=True)
    total_claim_cost: Series[float] = pa.Field(ge=0, coerce=True)
    payer_coverage: Series[float] = pa.Field(ge=0, coerce=True)
    silver_processing_timestamp: Series[pa.DateTime] = pa.Field(nullable=False, coerce=True)

class SilverPayersSchema(pa.DataFrameModel):
    payer_id: Series[str] = pa.Field(unique=True)
    payer_name: Series[str] = pa.Field()
    silver_processing_timestamp: Series[pa.DateTime] = pa.Field(nullable=False, coerce=True)


class SilverLayer:
    """
    Orquestra a criação da Camada Silver, consumindo dados da Bronze
    para gerar tabelas limpas, validadas e enriquecidas.
    """
    
    # ==============================================================================
    # --- SEÇÃO 1: Variáveis e Funções de Conexão ---
    # ==============================================================================
    
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    DEFAULT_STRING = "Unknown"
    DEFAULT_PAYER = "Self-Pay / Unspecified"

    def __init__(self, env_path: str = "./.env"):
        self._setup_logging()
        self._load_environment_variables(env_path)
        self.engine = self._get_db_engine()
        self.processing_timestamp = datetime.now()
        self.stats = {'loaded_tables': 0, 'total_records': 0, 'failed_tables': []}
        logger.info("Camada Silver inicializada.")

    def _setup_logging(self):
        logger.remove()
        logger.add("silver_layer.log", rotation="500 MB", level="INFO")
        logger.add(lambda msg: print(msg, end=""), colorize=True, level="INFO", format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

    def _load_environment_variables(self, env_path: str):
        load_dotenv(dotenv_path=env_path, override=True)
        logger.info("Variáveis de ambiente carregadas.")

    def _get_db_engine(self):
        try:
            url = f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASS')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
            return create_engine(url, pool_pre_ping=True)
        except Exception as e:
            logger.error(f"Falha ao conectar ao banco de dados: {e}")
            raise

    def _extract_from_bronze(self, table_name: str) -> pd.DataFrame:
        try:
            return pd.read_sql(f"SELECT * FROM {self.BRONZE_SCHEMA}.{table_name};", self.engine)
        except Exception as e:
            logger.error(f"Erro ao extrair de '{table_name}': {e}")
            return pd.DataFrame()

    def _load_to_silver(self, df: pd.DataFrame, table_name: str, schema: pa.DataFrameModel):
        if df.empty:
            logger.warning(f"DataFrame vazio para '{table_name}'. Pulando carga.")
            self.stats['failed_tables'].append(table_name)
            return
        try:
            df['silver_processing_timestamp'] = self.processing_timestamp
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.SILVER_SCHEMA};"))
                conn.commit()
            
            df_validated = schema.validate(df, lazy=True)
            df_validated.to_sql(table_name, self.engine, schema=self.SILVER_SCHEMA, if_exists='replace', index=False)
            
            logger.info(f"Dados carregados para {self.SILVER_SCHEMA}.{table_name}. Registros: {len(df_validated):,}")
            self.stats['loaded_tables'] += 1
            self.stats['total_records'] += len(df_validated)
        except pa.errors.SchemaErrors as err:
            logger.error(f"Erros de validação para '{table_name}':\n{err.failure_cases.head()}")
            self.stats['failed_tables'].append(f"{table_name} (Schema Validation)")
        except Exception as e:
            logger.error(f"Erro inesperado ao carregar '{table_name}': {e}", exc_info=True)
            self.stats['failed_tables'].append(table_name)

    # ==============================================================================
    # --- SEÇÃO 2: Funções de Transformação para a Camada Silver ---
    # ==============================================================================

    def _transform_patients(self, df_patients: pd.DataFrame, df_encounters: pd.DataFrame, df_payers: pd.DataFrame) -> pd.DataFrame:
        logger.info("Iniciando transformação de 'silver_patients_dim'.")
        if df_patients.empty: return pd.DataFrame()
        try:
            df = df_patients.dropna(subset=['patient_id', 'date_of_birth', 'first_name', 'last_name']).copy()
            df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
            df['age'] = ((self.processing_timestamp - df['date_of_birth']).dt.days / 365.25).astype(int)
            df['gender'] = df['gender'].fillna(self.DEFAULT_STRING).replace({'M': 'Male', 'F': 'Female', 'O': 'Other'})
            df['first_name'] = df['first_name'].str.title()
            df['last_name'] = df['last_name'].str.title()
            
            if not df_encounters.empty:
                patient_payer_map = df_encounters[['patient_id', 'payer_id', 'encounter_date']].copy()
                patient_payer_map['encounter_date'] = pd.to_datetime(patient_payer_map['encounter_date'], errors='coerce')
                patient_payer_map = patient_payer_map.sort_values('encounter_date').drop_duplicates('patient_id', keep='last')
                df = df.merge(patient_payer_map[['patient_id', 'payer_id']], on='patient_id', how='left')
            
            if not df_payers.empty:
                payers_clean = df_payers[['payer_id', 'payer_name']].dropna()
                df = df.merge(payers_clean, on='payer_id', how='left')
                df['payer_name'] = df['payer_name'].fillna(self.DEFAULT_PAYER)
            else:
                df['payer_name'] = self.DEFAULT_PAYER
                df['payer_id'] = None
                
            final_cols = [c.name for c in SilverPatientSchema.to_schema().columns.values() if c.name in df.columns]
            return df[final_cols]
        except Exception as e:
            logger.error(f"Falha na transformação de 'patients': {e}", exc_info=True)
            return pd.DataFrame()

    def _transform_claims(self, df_claims: pd.DataFrame, df_transactions: pd.DataFrame) -> pd.DataFrame:
        logger.info("Iniciando transformação de 'silver_claims_fact'.")
        if df_claims.empty: return pd.DataFrame()
        try:
            df = df_claims.copy()
            df['claim_start_date'] = pd.to_datetime(df['claim_start_date'], errors='coerce')
            df['claim_end_date'] = pd.to_datetime(df['claim_end_date'], errors='coerce')
            df = df.dropna(subset=['claim_id', 'patient_id', 'claim_start_date'])
            
            if not df_transactions.empty:
                trans_clean = df_transactions[['claim_id', 'transaction_amount', 'payments', 'outstanding']].copy()
                trans_clean['transaction_amount'] = pd.to_numeric(trans_clean['transaction_amount'], errors='coerce').fillna(0)
                trans_clean['payments'] = pd.to_numeric(trans_clean['payments'], errors='coerce').fillna(0)
                trans_clean['outstanding'] = pd.to_numeric(trans_clean['outstanding'], errors='coerce').fillna(0)
                
                claim_financials = trans_clean.groupby('claim_id').agg(total_billed_amount=('transaction_amount', 'sum'), total_paid_amount=('payments', 'sum'), patient_responsibility_amount=('outstanding', 'sum')).reset_index()
                df = df.merge(claim_financials, on='claim_id', how='left')
                df[['total_billed_amount', 'total_paid_amount', 'patient_responsibility_amount']] = df[['total_billed_amount', 'total_paid_amount', 'patient_responsibility_amount']].fillna(0)
            else:
                df['total_billed_amount'] = 0
                df['total_paid_amount'] = 0
                df['patient_responsibility_amount'] = 0
                
            final_cols = [c.name for c in SilverClaimsSchema.to_schema().columns.values() if c.name in df.columns]
            return df[final_cols]
        except Exception as e:
            logger.error(f"Falha na transformação de 'claims': {e}", exc_info=True)
            return pd.DataFrame()
    
    def _transform_claims_transactions(self, df_trans: pd.DataFrame) -> pd.DataFrame:
        logger.info("Iniciando transformação de 'silver_claims_transactions_fact'.")
        if df_trans.empty: return pd.DataFrame()
        try:
            df = df_trans.copy()
            df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
            df['transaction_amount'] = pd.to_numeric(df['transaction_amount'], errors='coerce').fillna(0)
            df = df.dropna(subset=['transaction_id', 'claim_id', 'transaction_date'])
            df['transaction_type'] = df['type'].str.upper()
            
            final_cols = [c.name for c in SilverClaimsTransactionsSchema.to_schema().columns.values() if c.name in df.columns]
            return df[final_cols]
        except Exception as e:
            logger.error(f"Falha na transformação de 'transactions': {e}", exc_info=True)
            return pd.DataFrame()
    
    def _transform_encounters(self, df_encounters: pd.DataFrame) -> pd.DataFrame:
        logger.info("Iniciando transformação de 'silver_encounters_fact'.")
        if df_encounters.empty: return pd.DataFrame()
        try:
            df = df_encounters.copy()
            df['encounter_date'] = pd.to_datetime(df['encounter_date'], errors='coerce')
            df['discharge_date'] = pd.to_datetime(df['discharge_date'], errors='coerce')
            df = df.dropna(subset=['encounter_id', 'patient_id', 'encounter_date'])
            
            for col in ['total_claim_cost', 'payer_coverage']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            df['length_of_stay_days'] = (df['discharge_date'] - df['encounter_date']).dt.days
            df.loc[df['length_of_stay_days'] < 0, 'length_of_stay_days'] = 0
            df['length_of_stay_days'] = df['length_of_stay_days'].fillna(0).astype(int)
            df['encounter_type'] = df['encounter_type'].fillna(self.DEFAULT_STRING).str.upper()
            
            final_cols = [c.name for c in SilverEncountersSchema.to_schema().columns.values() if c.name in df.columns]
            return df[final_cols]
        except Exception as e:
            logger.error(f"Falha na transformação de 'encounters': {e}", exc_info=True)
            return pd.DataFrame()

    def _transform_payers(self, df_payers: pd.DataFrame) -> pd.DataFrame:
        logger.info("Iniciando transformação de 'silver_payers_dim'.")
        if df_payers.empty: return pd.DataFrame()
        try:
            df = df_payers[['payer_id', 'payer_name']].copy().dropna()
            
            final_cols = [c.name for c in SilverPayersSchema.to_schema().columns.values() if c.name in df.columns]
            return df[final_cols]
        except Exception as e:
            logger.error(f"Falha na transformação de 'payers': {e}", exc_info=True)
            return pd.DataFrame()

    # ==============================================================================
    # --- SEÇÃO 3: Função Principal de Carga da Camada Silver ---
    # ==============================================================================

    def load_silver(self):
        """
        Orquestra todo o processo da camada Silver: extração da Bronze,
        transformação e carga das tabelas limpas e enriquecidas.
        """
        logger.info("Iniciando processo de transformação da Camada Silver.")
        
        logger.info("Extraindo dados da Camada Bronze...")
        bronze_dfs = {
            'patients': self._extract_from_bronze('bronze_patients'),
            'claims': self._extract_from_bronze('bronze_claims'),
            'claims_transactions': self._extract_from_bronze('bronze_claims_transactions'),
            'encounters': self._extract_from_bronze('bronze_encounters'),
            'payers': self._extract_from_bronze('bronze_payers')
        }
        
        logger.info("Aplicando transformações para a Camada Silver...")
        
        # Dimensão de Pacientes
        silver_patients = self._transform_patients(bronze_dfs['patients'], bronze_dfs['encounters'], bronze_dfs['payers'])
        self._load_to_silver(silver_patients, 'silver_patients_dim', SilverPatientSchema)
        
        # Fato de Reivindicações
        silver_claims = self._transform_claims(bronze_dfs['claims'], bronze_dfs['claims_transactions'])
        self._load_to_silver(silver_claims, 'silver_claims_fact', SilverClaimsSchema)
        
        # Fato de Transações de Reivindicações
        silver_transactions = self._transform_claims_transactions(bronze_dfs['claims_transactions'])
        self._load_to_silver(silver_transactions, 'silver_claims_transactions_fact', SilverClaimsTransactionsSchema)
        
        # Fato de Encontros
        silver_encounters = self._transform_encounters(bronze_dfs['encounters'])
        self._load_to_silver(silver_encounters, 'silver_encounters_fact', SilverEncountersSchema)
        
        # Dimensão de Pagadores
        silver_payers = self._transform_payers(bronze_dfs['payers'])
        self._load_to_silver(silver_payers, 'silver_payers_dim', SilverPayersSchema)
        
        self._summarize_run()
        logger.info("Processo da Camada Silver concluído.")

    def _summarize_run(self):
        logger.info("=" * 60)
        logger.info("RESUMO DA CARGA SILVER")
        logger.info("=" * 60)
        logger.info(f"Tabelas carregadas: {self.stats['loaded_tables']}")
        logger.info(f"Total de registros: {self.stats['total_records']:,}")
        if self.stats['failed_tables']:
            logger.warning(f"Tabelas com falha: {', '.join(self.stats['failed_tables'])}")
        else:
            logger.info("Todas as 5 transformações Silver concluídas com sucesso!")
        logger.info("=" * 60)


# ==============================================================================
# --- Bloco de Execução Principal ---
# ==============================================================================

if __name__ == "__main__":
    try:
        silver_layer = SilverLayer(env_path="./.env")
        silver_layer.load_silver()
    except Exception as e:
        logger.critical(f"Ocorreu um erro fatal e inesperado: {e}", exc_info=True)