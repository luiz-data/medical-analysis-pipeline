----------------------------------------------------------------------------------
-- Aumento de Dados para Tabelas da Silver Layer
-- Este script gera versões aumentadas das tabelas da Silver Layer
-- para simular conjuntos de dados maiores, visando testar a escalabilidade
-- e o desempenho de consultas. Ele garante que os IDs gerados mantenham
-- a integridade referencial.
----------------------------------------------------------------------------------

-- Define o esquema alvo
SET search_path TO silver;

-- --- 1. Aumentar SILVER_PAYERS_DIM ---
-- Cria a tabela de dimensão de pagadores aumentada
CREATE TABLE _payers_augmentation AS
SELECT * FROM silver_payers_dim LIMIT 0;

-- Insere 10 cópias dos pagadores originais, gerando novos payer_ids únicos
-- Esta é uma aumentação menor, pois os pagadores tipicamente não mudam tanto.
INSERT INTO _payers_augmentation (
    payer_id, payer_name, silver_processing_timestamp
)
SELECT
    (s.i || '_' || p.payer_id) AS payer_id,
    p.payer_name,
    p.silver_processing_timestamp
FROM silver_payers_dim AS p
JOIN generate_series(1, 10) AS s(i) ON TRUE;

SELECT count(*) AS total_augmented_payers FROM _payers_augmentation;


-- --- 2. Aumentar SILVER_PATIENTS_DIM ---
-- Cria a tabela de dimensão de pacientes aumentada
CREATE TABLE _patients_augmentation AS
SELECT * FROM silver_patients_dim LIMIT 0;

-- Insere 10.000 cópias dos pacientes originais, gerando novos patient_ids únicos.
-- Garante que o payer_id também corresponda aos pagadores aumentados.
INSERT INTO _patients_augmentation (
    patient_id, first_name, last_name, full_name, date_of_birth, gender, age,
    payer_id, payer_name, silver_processing_timestamp
)
SELECT
    (s.i || '_' || p.patient_id) AS patient_id,
    p.first_name, p.last_name, p.full_name, p.date_of_birth, p.gender, p.age,
    (s.i || '_' || p.payer_id) AS payer_id, -- Link para pagadores aumentados
    pa.payer_name, -- Obtém o nome do pagador aumentado
    p.silver_processing_timestamp
FROM silver_patients_dim AS p
JOIN generate_series(1, 10000) AS s(i) ON TRUE
LEFT JOIN _payers_augmentation AS pa ON (s.i || '_' || p.payer_id) = pa.payer_id; -- Join com pagadores aumentados para obter o nome
-- Nota: Um LEFT JOIN é usado aqui para garantir que pacientes sem um payer_id ou com um payer_id não aumentado ainda sejam incluídos.
-- O payer_name será NULL se não encontrado em pagadores aumentados, espelhando o comportamento original para 'Self-Pay / Unspecified'.

SELECT count(*) AS total_augmented_patients FROM _patients_augmentation;


-- --- 3. Aumentar SILVER_ENCOUNTERS_FACT ---
-- Cria a tabela fato de atendimentos aumentada
CREATE TABLE _encounters_augmentation AS
SELECT * FROM silver_encounters_fact LIMIT 0;

-- Insere 100 cópias dos atendimentos originais, gerando novos encounter_ids e patient_ids únicos.
-- Garante que patient_id e payer_id se liguem às suas respectivas tabelas aumentadas.
INSERT INTO _encounters_augmentation (
    encounter_id, patient_id, provider_id, payer_id, encounter_date, discharge_date,
    encounter_type, length_of_stay_days, total_claim_cost, payer_coverage,
    silver_processing_timestamp
)
SELECT
    (s.i || '_' || e.encounter_id) AS encounter_id,
    (s.i || '_' || e.patient_id) AS patient_id, -- Link para pacientes aumentados
    e.provider_id,
    (s.i || '_' || e.payer_id) AS payer_id, -- Link para pagadores aumentados
    e.encounter_date, e.discharge_date, e.encounter_type,
    e.length_of_stay_days, e.total_claim_cost, e.payer_coverage,
    e.silver_processing_timestamp
FROM silver_encounters_fact AS e
JOIN generate_series(1, 100) AS s(i) ON TRUE
-- Não há JOIN com _patients_augmentation ou _payers_augmentation aqui, pois os IDs já são gerados para corresponder
-- através da mesma lógica de concatenação 's.i || _'. Isso é mais performático.
;

SELECT count(*) AS total_augmented_encounters FROM _encounters_augmentation;


-- --- 4. Aumentar SILVER_CLAIMS_FACT ---
-- Cria a tabela fato de sinistros aumentada
CREATE TABLE _claims_augmentation AS
SELECT * FROM silver_claims_fact LIMIT 0;

-- Insere 100 cópias dos sinistros originais, gerando novos claim_ids e patient_ids únicos.
INSERT INTO _claims_augmentation (
    claim_id, patient_id, provider_id, claim_start_date, claim_end_date,
    total_billed_amount, total_paid_amount, patient_responsibility_amount,
    silver_processing_timestamp
)
SELECT
    (s.i || '_' || c.claim_id) AS claim_id,
    (s.i || '_' || c.patient_id) AS patient_id, -- Link para pacientes aumentados
    c.provider_id, c.claim_start_date, c.claim_end_date,
    c.total_billed_amount, c.total_paid_amount, c.patient_responsibility_amount,
    c.silver_processing_timestamp
FROM silver_claims_fact AS c
JOIN generate_series(1, 100) AS s(i) ON TRUE;

SELECT count(*) AS total_augmented_claims FROM _claims_augmentation;


-- --- 5. Aumentar SILVER_CLAIMS_TRANSACTIONS_FACT ---
-- Cria a tabela fato de transações de sinistros aumentada
CREATE TABLE _claims_transactions_augmentation AS
SELECT * FROM silver_claims_transactions_fact LIMIT 0;

-- Insere 100 cópias das transações originais, gerando novos transaction_ids e claim_ids únicos.
INSERT INTO _claims_transactions_augmentation (
    transaction_id, claim_id, transaction_date, transaction_amount,
    procedure_code, transaction_type, silver_processing_timestamp
)
SELECT
    (s.i || '_' || ct.transaction_id) AS transaction_id,
    (s.i || '_' || ct.claim_id) AS claim_id, -- Link para sinistros aumentados
    ct.transaction_date, ct.transaction_amount, ct.procedure_code,
    ct.transaction_type, ct.silver_processing_timestamp
FROM silver_claims_transactions_fact AS ct
JOIN generate_series(1, 100) AS s(i) ON TRUE;

SELECT count(*) AS total_augmented_claims_transactions FROM _claims_transactions_augmentation;


----------------------------------------------------------------------------------
-- Demonstração de JOINs com Tabelas Aumentadas da Silver Layer
----------------------------------------------------------------------------------

-- --- Exemplo 1: Paciente e seus Atendimentos ---
CREATE TABLE _patient_encounters_join_augmented AS
SELECT
    pa.patient_id,
    pa.first_name,
    pa.last_name,
    ea.encounter_id,
    ea.encounter_date,
    ea.encounter_type,
    ea.total_claim_cost
FROM
    _patients_augmentation AS pa
INNER JOIN
    _encounters_augmentation AS ea ON pa.patient_id = ea.patient_id;

SELECT count(*) AS total_patient_encounters_join FROM _patient_encounters_join_augmented;

-- --- Exemplo 2: Paciente e seus Sinistros ---
CREATE TABLE  _patient_claims_join_augmented  AS
SELECT
    pa.patient_id,
    pa.first_name,
    pa.last_name,
    ca.claim_id,
    ca.total_billed_amount,
    ca.total_paid_amount
FROM
    _patients_augmentation AS pa
INNER JOIN
    _claims_augmentation AS ca ON pa.patient_id = ca.patient_id;

SELECT count(*) AS total_patient_claims_join FROM _patient_claims_join_augmented;

-- --- Exemplo 3: Sinistros com suas Transações ---
CREATE TABLE _claim_transactions_join_augmented AS
SELECT
    ca.claim_id,
    ca.total_billed_amount,
    cta.transaction_id,
    cta.transaction_date,
    cta.transaction_amount,
    cta.procedure_code
FROM
    _claims_augmentation AS ca
INNER JOIN
    _claims_transactions_augmentation AS cta ON ca.claim_id = cta.claim_id;

SELECT count(*) AS total_claim_transactions_join FROM _claim_transactions_join_augmented;


-- --- Exemplo 4: Atendimentos com Informações do Pagador ---
CREATE TABLE _encounter_payer_join_augmented AS
SELECT
    ea.encounter_id,
    ea.encounter_date,
    ea.encounter_type,
    ea.total_claim_cost,
    ea.payer_coverage,
    pa.payer_name
FROM
    _encounters_augmentation AS ea
INNER JOIN
    _payers_augmentation AS pa ON ea.payer_id = pa.payer_id;

SELECT count(*) AS total_encounter_payer_join FROM _encounter_payer_join_augmented;


-- --- Exemplo 5: Pacientes com Informações do Pagador (Pagador Primário conforme Lógica da Silver Layer) ---
CREATE TABLE _patient_payer_join_augmented AS
SELECT
    pa.patient_id,
    pa.first_name,
    pa.last_name,
    pa.age,
    p_aug.payer_name AS primary_payer_name
FROM
    _patients_augmentation AS pa
LEFT JOIN
    _payers_augmentation AS p_aug ON pa.payer_id = p_aug.payer_id;

SELECT count(*) AS total_patient_payer_join FROM _patient_payer_join_augmented;
