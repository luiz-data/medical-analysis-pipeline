# 3_gold_layer_construction.py

import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from loguru import logger
import numpy as np
import pandera as pa
from pandera.typing import DataFrame, Series

# ==============================================================================
# --- SCHEMAS PANDERA PARA A CAMADA GOLD ---
# ==============================================================================

class GoldPatientMonthlySummarySchema(pa.DataFrameModel):
    patient_id: Series[str] = pa.Field()
    first_name: Series[str] = pa.Field(nullable=False)
    last_name: Series[str] = pa.Field(nullable=False)
    year_month: Series[str] = pa.Field(nullable=False, regex=r"^\d{4}-\d{2}$")
    total_claims_count: Series[int] = pa.Field(ge=0, coerce=True)
    total_billed_amount_month: Series[float] = pa.Field(ge=0, coerce=True)
    total_paid_amount_month: Series[float] = pa.Field(ge=0, coerce=True)
    avg_claim_value_month: Series[float] = pa.Field(ge=0, coerce=True)
    gold_processing_timestamp: Series[pa.DateTime] = pa.Field(nullable=False, coerce=True)

class GoldPayerPerformanceSchema(pa.DataFrameModel):
    payer_id: Series[str] = pa.Field()
    payer_name: Series[str] = pa.Field(nullable=False)
    total_claims_count: Series[int] = pa.Field(ge=0, coerce=True)
    total_billed_amount: Series[float] = pa.Field(ge=0, coerce=True)
    total_paid_amount: Series[float] = pa.Field(ge=0, coerce=True)
    avg_paid_per_claim: Series[float] = pa.Field(ge=0, coerce=True)
    avg_patient_responsibility: Series[float] = pa.Field(ge=0, coerce=True)
    gold_processing_timestamp: Series[pa.DateTime] = pa.Field(nullable=False, coerce=True)

class GoldEncounterSummarySchema(pa.DataFrameModel):
    encounter_id: Series[str] = pa.Field(unique=True)
    patient_id: Series[str] = pa.Field()
    encounter_date: Series[pa.DateTime] = pa.Field(nullable=False, coerce=True)
    discharge_date: Series[pa.DateTime] = pa.Field(nullable=True, coerce=True)
    provider_id: Series[str] = pa.Field(nullable=True)
    encounter_type: Series[str] = pa.Field(nullable=True)
    length_of_stay_days: Series[int] = pa.Field(ge=0, coerce=True)
    total_billed_amount_encounter: Series[float] = pa.Field(ge=0, coerce=True)
    gold_processing_timestamp: Series[pa.DateTime] = pa.Field(nullable=False, coerce=True)

class GoldProcedureAnalysisSchema(pa.DataFrameModel):
    procedure_code: Series[str] = pa.Field()
    transaction_count: Series[int] = pa.Field(ge=0)
    total_amount: Series[float] = pa.Field()
    avg_amount: Series[float] = pa.Field()
    gold_processing_timestamp: Series[pa.DateTime] = pa.Field(nullable=False, coerce=True)

class GoldProviderActivitySchema(pa.DataFrameModel):
    provider_id: Series[str] = pa.Field()
    total_patients_seen: Series[int] = pa.Field(ge=0)
    total_encounters: Series[int] = pa.Field(ge=0)
    total_billed_from_encounters: Series[float] = pa.Field(ge=0)
    avg_billed_per_encounter: Series[float] = pa.Field(ge=0)
    gold_processing_timestamp: Series[pa.DateTime] = pa.Field(nullable=False, coerce=True)


class GoldLayer:
    """
    Orquestra a criação da Camada Gold, consumindo dados da Silver
    para gerar tabelas analíticas agregadas.
    """
    
    # ==============================================================================
    # --- SEÇÃO 1: Variáveis e Funções de Conexão ---
    # ==============================================================================
    
    SILVER_SCHEMA = "silver"
    GOLD_SCHEMA = "gold"
    UNASSIGNED_ID = "UNASSIGNED"

    def __init__(self, env_path: str = "./.env"):
        self._setup_logging()
        self._load_environment_variables(env_path)
        self.engine = self._get_db_engine()
        self.processing_timestamp = datetime.now()
        self.stats = {'loaded_tables': 0, 'total_records': 0, 'failed_tables': []}
        logger.info("Camada Gold inicializada.")

    def _setup_logging(self):
        logger.remove()
        logger.add("gold_layer.log", rotation="500 MB", level="INFO")
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

    def _extract_from_silver(self, table_name: str) -> pd.DataFrame:
        try:
            df = pd.read_sql(f"SELECT * FROM {self.SILVER_SCHEMA}.{table_name};", self.engine)
            logger.info(f"Dados de '{table_name}' extraídos. Registros: {len(df):,}")
            return df
        except Exception as e:
            logger.error(f"Erro ao extrair de '{table_name}': {e}")
            return pd.DataFrame()

    def _load_to_gold(self, df: pd.DataFrame, table_name: str, schema: pa.DataFrameModel):
        if df.empty:
            logger.warning(f"DataFrame vazio para '{table_name}'. Pulando carga.")
            self.stats['failed_tables'].append(table_name)
            return
        try:
            df['gold_processing_timestamp'] = self.processing_timestamp
            df = schema.validate(df, lazy=True)
            logger.info(f"Schema validado para '{table_name}'.")
            
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.GOLD_SCHEMA};"))
                conn.commit()
            
            df.to_sql(table_name, self.engine, schema=self.GOLD_SCHEMA, if_exists='replace', index=False)
            logger.info(f"Dados carregados para {self.GOLD_SCHEMA}.{table_name}. Registros: {len(df):,}")
            self.stats['loaded_tables'] += 1
            self.stats['total_records'] += len(df)
        except pa.errors.SchemaErrors as err:
            logger.error(f"Erros de validação para '{table_name}':\n{err.failure_cases.head()}")
            self.stats['failed_tables'].append(f"{table_name} (Schema Validation)")
        except Exception as e:
            logger.error(f"Erro ao carregar '{table_name}': {e}", exc_info=True)
            self.stats['failed_tables'].append(table_name)

    # ==============================================================================
    # --- SEÇÃO 2: Funções de Transformação para a Camada Gold ---
    # ==============================================================================

    def _transform_patient_monthly_summary(self, df_patients: pd.DataFrame, df_claims: pd.DataFrame) -> pd.DataFrame:
        logger.info("Iniciando 'gold_patient_monthly_summary'.")
        if df_patients.empty or df_claims.empty: return pd.DataFrame()
        
        df_claims_filtered = df_claims[df_claims['claim_start_date'].notna()].copy()
        df_claims_filtered['year_month'] = df_claims_filtered['claim_start_date'].dt.to_period('M').astype(str)
        
        patient_summary = df_claims_filtered.groupby(['patient_id', 'year_month']).agg(total_claims_count=('claim_id', 'nunique'), total_billed_amount_month=('total_billed_amount', 'sum'), total_paid_amount_month=('total_paid_amount', 'sum')).reset_index()
        patient_summary['avg_claim_value_month'] = (patient_summary['total_billed_amount_month'] / patient_summary['total_claims_count']).replace([np.inf, -np.inf], 0).fillna(0)
        
        patient_info = df_patients[['patient_id', 'first_name', 'last_name']].drop_duplicates()
        return patient_summary.merge(patient_info, on='patient_id', how='left')

    def _transform_payer_performance(self, df_claims: pd.DataFrame, df_patients: pd.DataFrame) -> pd.DataFrame:
        logger.info("Iniciando 'gold_payer_performance'.")
        if df_claims.empty or df_patients.empty: return pd.DataFrame()
        
        df_merged = df_claims.merge(df_patients[['patient_id', 'payer_id', 'payer_name']], on='patient_id', how='left')
        df_merged['payer_id'] = df_merged['payer_id'].fillna(self.UNASSIGNED_ID)
        df_merged['payer_name'] = df_merged['payer_name'].fillna('Self-Pay / Unspecified')
        
        payer_perf = df_merged.groupby(['payer_id', 'payer_name']).agg(total_claims_count=('claim_id', 'nunique'), total_billed_amount=('total_billed_amount', 'sum'), total_paid_amount=('total_paid_amount', 'sum'), resp_sum=('patient_responsibility_amount', 'sum')).reset_index()
        payer_perf['avg_paid_per_claim'] = (payer_perf['total_paid_amount'] / payer_perf['total_claims_count']).replace([np.inf, -np.inf], 0).fillna(0)
        payer_perf['avg_patient_responsibility'] = (payer_perf['resp_sum'] / payer_perf['total_claims_count']).replace([np.inf, -np.inf], 0).fillna(0)
        
        return payer_perf.drop(columns=['resp_sum'])

    def _transform_encounter_summary(self, df_encounters: pd.DataFrame, df_claims: pd.DataFrame) -> pd.DataFrame:
        logger.info("Iniciando 'gold_encounter_summary'.")
        if df_encounters.empty or df_claims.empty: return pd.DataFrame()
        
        df_encounters['encounter_date'] = pd.to_datetime(df_encounters['encounter_date'])
        df_encounters['discharge_date'] = pd.to_datetime(df_encounters['discharge_date'])
        df_claims['claim_start_date'] = pd.to_datetime(df_claims['claim_start_date'])
        df_encounters['discharge_date_filled'] = df_encounters['discharge_date'].fillna(self.processing_timestamp)
        
        df_merged = pd.merge(df_encounters, df_claims, on='patient_id', how='left')
        claims_in_encounter = df_merged[(df_merged['claim_start_date'] >= df_merged['encounter_date']) & (df_merged['claim_start_date'] <= df_merged['discharge_date_filled'])].copy()
        
        encounter_costs = claims_in_encounter.groupby('encounter_id').agg(total_billed_amount_encounter=('total_billed_amount', 'sum')).reset_index()
        encounter_summary = df_encounters.merge(encounter_costs, on='encounter_id', how='left')
        encounter_summary['total_billed_amount_encounter'] = encounter_summary['total_billed_amount_encounter'].fillna(0)
        
        return encounter_summary.drop(columns=['discharge_date_filled'])

    def _transform_procedure_analysis(self, df_transactions: pd.DataFrame) -> pd.DataFrame:
        logger.info("Iniciando 'gold_procedure_analysis'.")
        if df_transactions.empty: return pd.DataFrame()
        
        charges = df_transactions[df_transactions['transaction_type'] == 'CHARGE'].copy()
        procedure_summary = charges.groupby('procedure_code').agg(transaction_count=('transaction_id', 'nunique'), total_amount=('transaction_amount', 'sum')).reset_index()
        procedure_summary['avg_amount'] = (procedure_summary['total_amount'] / procedure_summary['transaction_count']).fillna(0)
        
        return procedure_summary.sort_values('total_amount', ascending=False)

    def _transform_provider_activity(self, df_encounters: pd.DataFrame) -> pd.DataFrame:
        logger.info("Iniciando 'gold_provider_activity_summary'.")
        if df_encounters.empty: return pd.DataFrame()
        
        providers = df_encounters.dropna(subset=['provider_id']).copy()
        provider_summary = providers.groupby('provider_id').agg(total_patients_seen=('patient_id', 'nunique'), total_encounters=('encounter_id', 'nunique'), total_billed_from_encounters=('total_claim_cost', 'sum')).reset_index()
        provider_summary['avg_billed_per_encounter'] = (provider_summary['total_billed_from_encounters'] / provider_summary['total_encounters']).replace([np.inf, -np.inf], 0).fillna(0)
        
        return provider_summary.sort_values('total_billed_from_encounters', ascending=False)

    # ==============================================================================
    # --- SEÇÃO 3: Função Principal de Carga da Camada Gold ---
    # ==============================================================================

    def load_gold(self):
        """
        Orquestra todo o processo da camada Gold: extração da Silver,
        transformação e carga das tabelas analíticas.
        """
        logger.info("Iniciando processo de transformação da Camada Gold.")
        
        logger.info("Extraindo dados da Camada Silver...")
        silver_dfs = {
            'patients_dim': self._extract_from_silver('silver_patients_dim'),
            'claims_fact': self._extract_from_silver('silver_claims_fact'),
            'encounters_fact': self._extract_from_silver('silver_encounters_fact'),
            'transactions_fact': self._extract_from_silver('silver_claims_transactions_fact'),
        }
        
        logger.info("Aplicando transformações para a Camada Gold...")
        
        # Tabela 1: Resumo Mensal por Paciente
        gold_patient_summary = self._transform_patient_monthly_summary(silver_dfs['patients_dim'], silver_dfs['claims_fact'])
        self._load_to_gold(gold_patient_summary, 'gold_patient_monthly_summary', GoldPatientMonthlySummarySchema)
        
        # Tabela 2: Performance por Pagador
        gold_payer_perf = self._transform_payer_performance(silver_dfs['claims_fact'], silver_dfs['patients_dim'])
        self._load_to_gold(gold_payer_perf, 'gold_payer_performance', GoldPayerPerformanceSchema)
        
        # Tabela 3: Resumo por Encontro
        gold_encounter_summary = self._transform_encounter_summary(silver_dfs['encounters_fact'], silver_dfs['claims_fact'])
        self._load_to_gold(gold_encounter_summary, 'gold_encounter_summary', GoldEncounterSummarySchema)
        
        # Tabela 4: Análise de Procedimentos
        gold_procedure_analysis = self._transform_procedure_analysis(silver_dfs['transactions_fact'])
        self._load_to_gold(gold_procedure_analysis, 'gold_procedure_analysis', GoldProcedureAnalysisSchema)
        
        # Tabela 5: Atividade por Prestador
        gold_provider_activity = self._transform_provider_activity(silver_dfs['encounters_fact'])
        self._load_to_gold(gold_provider_activity, 'gold_provider_activity_summary', GoldProviderActivitySchema)

        self._summarize_run()
        logger.info("Processo da Camada Gold concluído.")

    def _summarize_run(self):
        logger.info("=" * 60)
        logger.info("RESUMO DA CARGA GOLD")
        logger.info("=" * 60)
        logger.info(f"Tabelas carregadas: {self.stats['loaded_tables']}")
        logger.info(f"Total de registros: {self.stats['total_records']:,}")
        if self.stats['failed_tables']:
            logger.warning(f"Tabelas com falha: {', '.join(self.stats['failed_tables'])}")
        else:
            logger.info("Todas as 5 transformações Gold concluídas com sucesso!")
        logger.info("=" * 60)

# ==============================================================================
# --- Bloco de Execução Principal ---
# ==============================================================================

if __name__ == "__main__":
    try:
        gold_layer = GoldLayer(env_path="./.env")
        gold_layer.load_gold()
    except Exception as e:
        logger.critical(f"Ocorreu um erro fatal e inesperado: {e}", exc_info=True)