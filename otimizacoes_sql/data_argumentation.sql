----------------------------------------------------------------------------------
-- Data Augmentation for Silver Layer Tables
-- This script generates augmented versions of the Silver Layer tables
-- to simulate larger datasets for testing scalability and query performance.
-- It ensures that generated IDs maintain referential integrity.
----------------------------------------------------------------------------------

-- Define the target schema
SET search_path TO silver;

-- --- 1. Augment SILVER_PAYERS_DIM ---
-- Create augmented payers dimension table
CREATE TABLE _payers_augmentation AS
SELECT * FROM silver_payers_dim LIMIT 0;

-- Insert 10 copies of original payers, generating new unique payer_ids
-- This is a smaller augmentation as payers typically don't change as much.
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


-- --- 2. Augment SILVER_PATIENTS_DIM ---
-- Create augmented patients dimension table
CREATE TABLE _patients_augmentation AS
SELECT * FROM silver_patients_dim LIMIT 0;

-- Insert 10.000 copies of original patients, generating new unique patient_ids.
-- Ensure payer_id also matches augmented payers.
INSERT INTO _patients_augmentation (
    patient_id, first_name, last_name, full_name, date_of_birth, gender, age,
    payer_id, payer_name, silver_processing_timestamp
)
SELECT
    (s.i || '_' || p.patient_id) AS patient_id,
    p.first_name, p.last_name, p.full_name, p.date_of_birth, p.gender, p.age,
    (s.i || '_' || p.payer_id) AS payer_id, -- Link to augmented payers
    pa.payer_name, -- Get augmented payer name
    p.silver_processing_timestamp
FROM silver_patients_dim AS p
JOIN generate_series(1, 10000) AS s(i) ON TRUE
LEFT JOIN _payers_augmentation AS pa ON (s.i || '_' || p.payer_id) = pa.payer_id; -- Join with augmented payers to get name
-- Note: A LEFT JOIN is used here to ensure patients without a payer_id or with an unaugmented payer_id are still included.
-- The payer_name will be NULL if not found in augmented payers, mirroring original behavior for 'Self-Pay / Unspecified'.

SELECT count(*) AS total_augmented_patients FROM _patients_augmentation;


-- --- 3. Augment SILVER_ENCOUNTERS_FACT ---
-- Create augmented encounters fact table
CREATE TABLE _encounters_augmentation AS
SELECT * FROM silver_encounters_fact LIMIT 0;

-- Insert 100 copies of original encounters, generating new unique encounter_ids and patient_ids.
-- Ensure patient_id and payer_id link to their respective augmented tables.
INSERT INTO _encounters_augmentation (
    encounter_id, patient_id, provider_id, payer_id, encounter_date, discharge_date,
    encounter_type, length_of_stay_days, total_claim_cost, payer_coverage,
    silver_processing_timestamp
)
SELECT
    (s.i || '_' || e.encounter_id) AS encounter_id,
    (s.i || '_' || e.patient_id) AS patient_id, -- Link to augmented patients
    e.provider_id,
    (s.i || '_' || e.payer_id) AS payer_id, -- Link to augmented payers
    e.encounter_date, e.discharge_date, e.encounter_type,
    e.length_of_stay_days, e.total_claim_cost, e.payer_coverage,
    e.silver_processing_timestamp
FROM silver_encounters_fact AS e
JOIN generate_series(1, 100) AS s(i) ON TRUE
-- No JOIN com _patients_augmentation ou _payers_augmentation aqui, pois os IDs já são gerados para corresponder
-- através da mesma lógica de concatenação 's.i || _'. Isso é mais performático.
;

SELECT count(*) AS total_augmented_encounters FROM _encounters_augmentation;


-- --- 4. Augment SILVER_CLAIMS_FACT ---
-- Create augmented claims fact table
CREATE TABLE _claims_augmentation AS
SELECT * FROM silver_claims_fact LIMIT 0;

-- Insert 100 copies of original claims, generating new unique claim_ids and patient_ids.
INSERT INTO _claims_augmentation (
    claim_id, patient_id, provider_id, claim_start_date, claim_end_date,
    total_billed_amount, total_paid_amount, patient_responsibility_amount,
    silver_processing_timestamp
)
SELECT
    (s.i || '_' || c.claim_id) AS claim_id,
    (s.i || '_' || c.patient_id) AS patient_id, -- Link to augmented patients
    c.provider_id, c.claim_start_date, c.claim_end_date,
    c.total_billed_amount, c.total_paid_amount, c.patient_responsibility_amount,
    c.silver_processing_timestamp
FROM silver_claims_fact AS c
JOIN generate_series(1, 100) AS s(i) ON TRUE;

SELECT count(*) AS total_augmented_claims FROM _claims_augmentation;


-- --- 5. Augment SILVER_CLAIMS_TRANSACTIONS_FACT ---
-- Create augmented claims transactions fact table
CREATE TABLE _claims_transactions_augmentation AS
SELECT * FROM silver_claims_transactions_fact LIMIT 0;

-- Insert 100 copies of original transactions, generating new unique transaction_ids and claim_ids.
INSERT INTO _claims_transactions_augmentation (
    transaction_id, claim_id, transaction_date, transaction_amount,
    procedure_code, transaction_type, silver_processing_timestamp
)
SELECT
    (s.i || '_' || ct.transaction_id) AS transaction_id,
    (s.i || '_' || ct.claim_id) AS claim_id, -- Link to augmented claims
    ct.transaction_date, ct.transaction_amount, ct.procedure_code,
    ct.transaction_type, ct.silver_processing_timestamp
FROM silver_claims_transactions_fact AS ct
JOIN generate_series(1, 100) AS s(i) ON TRUE;

SELECT count(*) AS total_augmented_claims_transactions FROM _claims_transactions_augmentation;


----------------------------------------------------------------------------------
-- Demonstrating JOINs with Augmented Silver Layer Tables
----------------------------------------------------------------------------------

-- --- Example 1: Patient and their Encounters ---
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


-- --- Example 2: Patient and their Claims ---
CREATE TABLE _patient_claims_join_augmented AS
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


-- --- Example 3: Claims with their Transactions ---
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


-- --- Example 4: Encounters with Payer Information ---
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


-- --- Example 5: Patients with Payer Information (Primary Payer as per Silver Layer Logic) ---
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