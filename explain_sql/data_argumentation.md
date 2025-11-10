# Data Augmentation

## 📋 Visão Geral

Este script realiza a **augmentação (multiplicação) de dados** das tabelas da camada Silver para simular volumes maiores de dados. O objetivo principal é testar escalabilidade, performance de queries e validar a arquitetura do Data Warehouse em cenários de alta volumetria.

---

## 🎯 Por Que Fazer Isso?

### Problemas que Resolve:
1. **Testes de Performance**: Avaliar como queries se comportam com milhões de registros
2. **Validação de Escalabilidade**: Garantir que a arquitetura suporta crescimento de dados
3. **Testes de Integridade Referencial**: Validar JOINs e relacionamentos entre tabelas em larga escala
4. **Simulação Realista**: Criar ambientes de testes sem necessidade de dados reais massivos
5. **Otimização de Queries**: Identificar gargalos de performance antes de ir para produção

### Quando Usar:
- Ambientes de **desenvolvimento** e **homologação**
- Testes de **stress** do banco de dados
- Validação de **índices** e **particionamento**
- Treinamento de modelos de ML que precisam de mais dados

---

## 🏗️ Estrutura do Script

### 1️⃣ Augmentação da Dimensão de Pagadores (`_payers_augmentation`)

```sql
CREATE TABLE _payers_augmentation AS
SELECT * FROM silver_payers_dim LIMIT 0;

INSERT INTO _payers_augmentation (...)
SELECT
    (s.i || '_' || p.payer_id) AS payer_id,
    p.payer_name,
    p.silver_processing_timestamp
FROM silver_payers_dim AS p
JOIN generate_series(1, 10) AS s(i) ON TRUE;
```

**O que faz:**
- Cria **10 cópias** de cada pagador original
- Gera novos `payer_id` únicos usando concatenação: `1_PAY001`, `2_PAY001`, etc.

**Por que 10 cópias?**
- Pagadores são entidades dimensionais que mudam menos frequentemente
- Multiplicação menor mantém proporção realista com outras tabelas

---

### 2️⃣ Augmentação da Dimensão de Pacientes (`_patients_augmentation`)

```sql
INSERT INTO _patients_augmentation (...)
SELECT
    (s.i || '_' || p.patient_id) AS patient_id,
    p.first_name, p.last_name, p.full_name, p.date_of_birth, p.gender, p.age,
    (s.i || '_' || p.payer_id) AS payer_id,
    pa.payer_name,
    p.silver_processing_timestamp
FROM silver_patients_dim AS p
JOIN generate_series(1, 10000) AS s(i) ON TRUE
LEFT JOIN _payers_augmentation AS pa ON (s.i || '_' || p.payer_id) = pa.payer_id;
```

**O que faz:**
- Cria **10.000 cópias** de cada paciente
- Mantém **integridade referencial** com `_payers_augmentation`
- Usa `LEFT JOIN` para preservar pacientes sem pagador (self-pay)

**Por que 10.000 cópias?**
- Pacientes são a entidade central do modelo
- Volume massivo permite testes realistas de queries analíticas
- Simula crescimento de décadas de dados hospitalares

**Decisão Técnica - LEFT JOIN:**
- Garante que pacientes sem `payer_id` ou com pagadores não augmentados sejam incluídos
- Mantém comportamento original da Silver Layer (ex: "Self-Pay / Unspecified")

---

### 3️⃣ Augmentação da Fato de Encounters (`_encounters_augmentation`)

```sql
INSERT INTO _encounters_augmentation (...)
SELECT
    (s.i || '_' || e.encounter_id) AS encounter_id,
    (s.i || '_' || e.patient_id) AS patient_id,
    e.provider_id,
    (s.i || '_' || e.payer_id) AS payer_id,
    e.encounter_date, e.discharge_date, e.encounter_type,
    e.length_of_stay_days, e.total_claim_cost, e.payer_coverage,
    e.silver_processing_timestamp
FROM silver_encounters_fact AS e
JOIN generate_series(1, 100) AS s(i) ON TRUE;
```

**O que faz:**
- Cria **100 cópias** de cada encounter
- Mantém relacionamento com pacientes e pagadores augmentados

**Por que não fazer JOIN explícito?**
- A lógica de concatenação `s.i || '_'` garante correspondência automática
- **Mais performático**: evita JOINs desnecessários durante a inserção
- IDs já são gerados para "bater" com as tabelas relacionadas

---

### 4️⃣ Augmentação da Fato de Claims (`_claims_augmentation`)

```sql
INSERT INTO _claims_augmentation (...)
SELECT
    (s.i || '_' || c.claim_id) AS claim_id,
    (s.i || '_' || c.patient_id) AS patient_id,
    c.provider_id, c.claim_start_date, c.claim_end_date,
    c.total_billed_amount, c.total_paid_amount, c.patient_responsibility_amount,
    c.silver_processing_timestamp
FROM silver_claims_fact AS c
JOIN generate_series(1, 100) AS s(i) ON TRUE;
```

**O que faz:**
- Cria **100 cópias** de cada claim
- Vincula automaticamente aos pacientes augmentados

**Proporção com Encounters:**
- Mesma multiplicação (100x) mantém cardinalidade realista entre encounters e claims

---

### 5️⃣ Augmentação da Fato de Transactions (`_claims_transactions_augmentation`)

```sql
INSERT INTO _claims_transactions_augmentation (...)
SELECT
    (s.i || '_' || ct.transaction_id) AS transaction_id,
    (s.i || '_' || ct.claim_id) AS claim_id,
    ct.transaction_date, ct.transaction_amount, ct.procedure_code,
    ct.transaction_type, ct.silver_processing_timestamp
FROM silver_claims_transactions_fact AS ct
JOIN generate_series(1, 100) AS s(i) ON TRUE;
```

**O que faz:**
- Cria **100 cópias** de cada transação
- Mantém granularidade de transações por claim

---

## 🔗 Exemplos de JOINs com Dados Augmentados

### Exemplo 1: Pacientes e seus Encounters
```sql
CREATE TABLE _patient_encounters_join_augmented AS
SELECT
    pa.patient_id, pa.first_name, pa.last_name,
    ea.encounter_id, ea.encounter_date, ea.encounter_type, ea.total_claim_cost
FROM _patients_augmentation AS pa
INNER JOIN _encounters_augmentation AS ea ON pa.patient_id = ea.patient_id;
```

**Por que validar JOINs?**
- Confirma que a integridade referencial foi mantida após augmentação
- Testa performance de queries comuns em produção

---

### Exemplo 2: Pacientes e seus Claims
**Uso:** Análises de faturamento por paciente

### Exemplo 3: Claims com suas Transactions
**Uso:** Rastreamento detalhado de procedimentos e custos

### Exemplo 4: Encounters com Informações de Pagadores
**Uso:** Análises de cobertura por convênio

### Exemplo 5: Pacientes com Pagador Primário
**Uso:** Segmentação de pacientes por tipo de cobertura (usa `LEFT JOIN` para incluir self-pay)

---

## ⚙️ Técnicas Utilizadas

### 1. `generate_series()` - Multiplicação Cartesiana Controlada
```sql
JOIN generate_series(1, 10000) AS s(i) ON TRUE
```
- Gera sequência de 1 a N
- `ON TRUE` cria produto cartesiano (cada linha original × N cópias)
- Funciona como um "loop" sem necessidade de procedures

### 2. Concatenação de IDs para Unicidade
```sql
(s.i || '_' || p.patient_id) AS patient_id
```
- Garante IDs únicos: `1_PAT001`, `2_PAT001`, ..., `10000_PAT001`
- Mantém rastreabilidade do dado original

### 3. Integridade Referencial Implícita
- Mesmo padrão de concatenação em todas as tabelas
- IDs "se encontram" automaticamente nos JOINs
- Evita complexidade de FKs temporárias

---

## 📊 Volumetria Esperada

| Tabela Original | Fator de Multiplicação | Volume Aproximado |
|----------------|------------------------|-------------------|
| Payers | 10x | ~100 registros |
| Patients | 10.000x | ~1 milhão de registros |
| Encounters | 100x | ~10 milhões de registros |
| Claims | 100x | ~5 milhões de registros |
| Transactions | 100x | ~50 milhões de registros |

**Total estimado:** ~66 milhões de registros augmentados

---

## ⚠️ Considerações Importantes

### Limitações:
1. **Não cria variabilidade**: Dados são cópias exatas (mesmas datas, valores, etc.)
2. **Apenas para testes**: NUNCA usar em produção
3. **Consome espaço**: Tabelas augmentadas ocupam muito storage
4. **Não substitui dados reais**: Para testes de qualidade, use dados sintéticos mais sofisticados

### Boas Práticas:
- ✅ Executar em schema separado (`silver` neste caso)
- ✅ Usar prefixo `_` para tabelas temporárias
- ✅ Documentar fator de multiplicação para cada tabela
- ✅ Validar com `COUNT(*)` após cada augmentação
- ✅ Dropar tabelas augmentadas após testes

### Melhorias Possíveis:
1. **Variabilidade temporal**: Adicionar offsets aleatórios em datas
2. **Perturbação de valores**: Adicionar ruído em valores numéricos
3. **Particionamento**: Criar tabelas já particionadas por data
4. **Índices**: Criar índices após inserção para testes mais realistas

---

## 🚀 Como Executar

```bash
# 1. Conectar ao banco
psql -h <host> -U <user> -d <database>

# 2. Executar script
\i data_augmentation_silver.sql

# 3. Validar resultados
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'silver' 
  AND tablename LIKE '%augmentation%';
```

---

## 📚 Referências

- PostgreSQL `generate_series()`: https://www.postgresql.org/docs/current/functions-srf.html
- Data Warehouse Testing Best Practices
- Synthetic Data Generation for Healthcare Analytics

---
