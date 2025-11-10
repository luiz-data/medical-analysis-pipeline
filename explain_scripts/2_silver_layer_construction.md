# Análise Detalhada do Script: `2_silver_layer_construction.py`

## 1. Objetivo do Script

O objetivo deste script é construir a **Camada Silver**, que representa uma versão limpa, validada e enriquecida dos dados provenientes da Camada Bronze. Enquanto a Bronze é uma cópia fiel da origem, a Silver é onde os dados são transformados em um formato confiável e consistente, pronto para consumo analítico.

As principais responsabilidades do script são:
1.  **Extrair** os dados brutos da Camada Bronze.
2.  **Limpar** os dados, tratando valores nulos, corrigindo tipos de dados e padronizando valores.
3.  **Enriquecer** os dados, criando novas colunas com informações de negócio (ex: `idade`, `tempo de internação`).
4.  **Validar** a qualidade e a estrutura dos dados contra um "contrato de dados" pré-definido usando a biblioteca **Pandera**.
5.  **Carregar** os dados validados em um novo schema (`silver`) no banco de dados, com uma nomenclatura que distingue tabelas de dimensão (`_dim`) e fato (`_fact`).

## 2. Principais Componentes e Conceitos

### 2.1. Validação de Dados com Pandera

A característica mais importante deste script é o uso da biblioteca `pandera`. Antes de carregar qualquer dado na Camada Silver, ele é validado contra um *schema* rigoroso.

```python
import pandera.pandas as pa
from pandera.typing import DataFrame, Series

class SilverPatientSchema(pa.DataFrameModel):
    patient_id: Series[str] = pa.Field(unique=True)
    first_name: Series[str] = pa.Field(nullable=False)
    # ... outras colunas e regras
```

-   **O que é um Schema Pandera?** É como um "contrato de qualidade" para os dados. Cada classe (`SilverPatientSchema`, `SilverClaimsSchema`, etc.) define exatamente como a tabela final deve ser.
-   **Quais regras são aplicadas?**
    -   **Tipos de Dados:** Garante que uma coluna seja `string`, `DateTime`, `int`, `float`, etc. (ex: `date_of_birth: Series[pa.DateTime]`).
    -   **Restrições:** Impõe regras de negócio, como valores únicos (`unique=True`), não nulos (`nullable=False`), ou dentro de um intervalo (`ge=0` - maior ou igual a zero).
    -   **Coerção (`coerce=True`):** Tenta converter os dados para o tipo correto automaticamente (ex: a string "123" para o número 123).

**Por que isso é importante?** Pandera atua como um **portão de qualidade automático**. Se os dados transformados não cumprirem o contrato, a carga daquela tabela falha e um erro detalhado é registrado. Isso impede que dados "sujos" ou corrompidos contaminem a Camada Silver, garantindo a confiabilidade das análises futuras.

## 3. Análise do Fluxo de Dados (Passo a Passo)

O script segue um fluxo lógico de extração, transformação e carga (ETL).

### 3.1. Extração da Camada Bronze

O método `_extract_from_bronze` é o ponto de partida.

```python
def _extract_from_bronze(self, table_name: str) -> pd.DataFrame:
    try:
        return pd.read_sql(f"SELECT * FROM {self.BRONZE_SCHEMA}.{table_name};", self.engine)
    # ...
```

-   Ele se conecta ao banco de dados e executa um `SELECT *` para buscar todos os dados de uma tabela específica no schema `bronze`.
-   O script carrega todas as tabelas da Bronze para DataFrames em memória no início do processo.

### 3.2. Lógica de Transformação (Tabela por Tabela)

Esta é a seção central do script, onde os dados brutos são convertidos em informações de valor.

#### `_transform_patients()` -> `silver_patients_dim`
Cria uma **tabela de dimensão** com informações limpas sobre os pacientes.
-   **Limpeza:** Remove registros onde campos essenciais (`patient_id`, `date_of_birth`) são nulos.
-   **Conversão de Tipos:** Converte a coluna de data de nascimento para o tipo `datetime`.
-   **Engenharia de Features:** Cria a coluna `age` (idade) calculando a diferença entre a data atual e a data de nascimento.
-   **Padronização:** Normaliza a coluna `gender` (ex: 'F' para 'Female') e capitaliza nomes próprios (`first_name`, `last_name`).
-   **Enriquecimento:** Realiza um `merge` (join) com os dados de `encounters` e `payers` para encontrar o pagador (`payer_name`) mais recente associado a cada paciente.

#### `_transform_claims()` -> `silver_claims_fact`
Cria uma **tabela fato** com métricas financeiras agregadas por cobrança.
-   **Limpeza:** Converte datas e remove cobranças sem dados essenciais.
-   **Agregação (Regra de Negócio Chave):** A parte mais importante desta função é o `groupby('claim_id').agg(...)`. Ela pega todas as transações individuais da tabela `bronze_claims_transactions` e as resume para calcular três métricas essenciais por `claim_id`:
    -   `total_billed_amount`: A soma de todos os valores de transação.
    -   `total_paid_amount`: A soma de todos os pagamentos.
    -   `patient_responsibility_amount`: A soma dos valores pendentes.
-   **Junção:** O resultado agregado é juntado de volta à tabela de `claims`.

#### `_transform_claims_transactions()` -> `silver_claims_transactions_fact`
Cria uma tabela fato detalhada com transações limpas.
-   **Limpeza:** Converte tipos de dados (`datetime`, `numeric`) e trata valores nulos.
-   **Padronização:** Cria uma nova coluna `transaction_type` baseada na coluna `type` original e a converte para maiúsculas para consistência.

#### `_transform_encounters()` -> `silver_encounters_fact`
Cria uma tabela fato com dados sobre os encontros (consultas, internações).
-   **Limpeza:** Converte datas e tipos numéricos, preenchendo nulos com `0`.
-   **Engenharia de Features:** Calcula o `length_of_stay_days` (dias de internação) subtraindo a data de alta da data de entrada. Garante que o valor nunca seja negativo.
-   **Padronização:** Normaliza o `encounter_type` para maiúsculas e preenche valores nulos com "Unknown".

#### `_transform_payers()` -> `silver_payers_dim`
Cria uma **tabela de dimensão** simples para os pagadores.
-   **Limpeza:** Apenas seleciona as colunas relevantes (`payer_id`, `payer_name`) e remove quaisquer registros nulos para garantir uma tabela de dimensão limpa.

### 3.3. Validação e Carga na Camada Silver

O método `_load_to_silver` é o portão de qualidade final antes de salvar os dados.

```python
def _load_to_silver(self, df: pd.DataFrame, table_name: str, schema: pa.DataFrameModel):
    # ...
    try:
        # ...
        df_validated = schema.validate(df, lazy=True)
        df_validated.to_sql(table_name, self.engine, schema=self.SILVER_SCHEMA, if_exists='replace', index=False)
    # ...
```

1.  **Adição de Metadados:** Adiciona a coluna `silver_processing_timestamp` para rastrear quando o dado foi processado.
2.  **Validação:** A linha `df_validated = schema.validate(df, lazy=True)` executa a validação do Pandera. Se o DataFrame `df` estiver em conformidade com o `schema` passado, o código continua. Caso contrário, ele lança uma exceção `pa.errors.SchemaErrors`.
3.  **Tratamento de Erro:** O bloco `except pa.errors.SchemaErrors as err:` captura a falha de validação, registra as primeiras linhas de dados que falharam (`err.failure_cases.head()`), e impede que a tabela seja carregada.
4.  **Carga:** Se a validação for bem-sucedida, `df_validated.to_sql(...)` carrega os dados limpos e validados na tabela correspondente no schema `silver`, substituindo a versão anterior (`if_exists='replace'`).

### 4. Orquestração e Execução

O método `load_silver()` organiza todo o processo:
1.  **Extrai** todos os DataFrames da camada Bronze de uma só vez.
2.  **Chama** cada função de transformação na ordem correta, passando os DataFrames necessários como argumentos.
3.  **Chama** `_load_to_silver` para cada DataFrame transformado, aplicando a validação e carregando os dados.
4.  **Imprime** um resumo final da execução.

O bloco `if __name__ == "__main__":` garante que o script possa ser executado de forma autônoma e inclui um tratamento de erro geral para capturar qualquer falha inesperada durante a execução.
