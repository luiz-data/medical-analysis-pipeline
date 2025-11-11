-- optimization.sql

-- Este script aplica e demonstra técnicas de otimização em tabelas de exemplo (camada Silver)
-- no PostgreSQL. Ele cria índices e compara o desempenho de consultas antes e depois.

-- As tabelas de exemplo são:
-- _patients_augmentation (supõe-se que tenha 'id', 'gender' e outras colunas)
-- _encounters_augmentation (supõe-se que tenha 'id', 'patient' (FK para patients.id), 'description', 'encounterclass' e outras colunas)

----------------------------------------------------------------------------------------------------
--                                       CENÁRIO SEM OTIMIZAÇÃO (Opcional: Apenas para comparação)
--
-- Se você quiser ver os planos de execução sem otimização novamente, execute estas consultas
-- ANTES de criar os índices abaixo. Caso contrário, pode pular esta seção.
----------------------------------------------------------------------------------------------------

-- Consulta de Junção Lenta (Inicial)
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM _patients_augmentation p
JOIN _encounters_augmentation e ON p.id = e.patient;

-- Consulta de Filtro Lenta (Inicial)
EXPLAIN ANALYZE
SELECT *
FROM _encounters_augmentation
WHERE description = 'Patient-initiated visit' AND encounterclass = 'outpatient';

-- Consulta de Texto Lenta (Inicial com LIKE)
EXPLAIN ANALYZE
SELECT id
FROM _encounters_augmentation
WHERE description LIKE '%Hospitalization%';


----------------------------------------------------------------------------------------------------
--                                      CONFIGURAÇÕES DE OTIMIZAÇÃO
--
-- Criação de índices para melhorar o desempenho das consultas.
----------------------------------------------------------------------------------------------------

-- GARANTIR QUE A EXTENSÃO pg_trgm ESTEJA ATIVADA (necessária para índices GIN de texto)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- NOTA: Se você não tiver permissão para criar extensões, precisará que um superusuário faça isso.
-- Ex: psql -U postgres -c "CREATE EXTENSION pg_trgm;" -d seu_banco_de_dados

-- 1. Índices para Chaves Primárias e Estrangeiras (essenciais para JOINS)

-- Índice na chave primária da tabela _patients_augmentation
-- Verifica se o índice já existe antes de tentar criá-lo
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = '_patients_augmentation' AND indexname = 'idx_patients_id') THEN
        CREATE INDEX idx_patients_id ON _patients_augmentation (id);
        RAISE NOTICE 'Índice idx_patients_id criado.';
    ELSE
        RAISE NOTICE 'Índice idx_patients_id já existe.';
    END IF;
END $$;


-- Índice na chave estrangeira da tabela _encounters_augmentation (coluna 'patient')
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = '_encounters_augmentation' AND indexname = 'idx_encounters_patient') THEN
        CREATE INDEX idx_encounters_patient ON _encounters_augmentation (patient);
        RAISE NOTICE 'Índice idx_encounters_patient criado.';
    ELSE
        RAISE NOTICE 'Índice idx_encounters_patient já existe.';
    END IF;
END $$;


-- 2. Índices para Colunas Usadas em Cláusulas WHERE (filtros)

-- Índice na coluna 'description' da tabela _encounters_augmentation
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = '_encounters_augmentation' AND indexname = 'idx_encounters_description') THEN
        CREATE INDEX idx_encounters_description ON _encounters_augmentation (description);
        RAISE NOTICE 'Índice idx_encounters_description criado.';
    ELSE
        RAISE NOTICE 'Índice idx_encounters_description já existe.';
    END IF;
END $$;

-- Índice na coluna 'encounterclass' da tabela _encounters_augmentation
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = '_encounters_augmentation' AND indexname = 'idx_encounters_class') THEN
        CREATE INDEX idx_encounters_class ON _encounters_augmentation (encounterclass);
        RAISE NOTICE 'Índice idx_encounters_class já existe.';
    ELSE
        RAISE NOTICE 'Índice idx_encounters_class criado.';
    END IF;
END $$;

-- 3. Índice Especializado para Busca de Texto (LIKE '%texto%')
-- Usa a extensão pg_trgm com o tipo de índice GIN

-- Índice GIN na coluna 'description' para buscas LIKE eficientes
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = '_encounters_augmentation' AND indexname = 'idx_encounters_description_gin') THEN
        CREATE INDEX idx_encounters_description_gin ON _encounters_augmentation USING GIN (description gin_trgm_ops);
        RAISE NOTICE 'Índice idx_encounters_description_gin criado.';
    ELSE
        RAISE NOTICE 'Índice idx_encounters_description_gin já existe.';
    END IF;
END $$;


-- 4. Índice para a Coluna 'gender' (usado na otimização de UNION)
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = '_patients_augmentation' AND indexname = 'idx_patients_gender') THEN
        CREATE INDEX idx_patients_gender ON _patients_augmentation (gender);
        RAISE NOTICE 'Índice idx_patients_gender criado.';
    ELSE
        RAISE NOTICE 'Índice idx_patients_gender já existe.';
    END IF;
END $$;


----------------------------------------------------------------------------------------------------
--                                 CONSULTAS OTIMIZADAS COM EXPLAIN ANALYZE
--
-- Executa as mesmas consultas iniciais, mas agora com os índices ativos para comparar o desempenho.
----------------------------------------------------------------------------------------------------

RAISE NOTICE '---------------------------------------------';
RAISE NOTICE '       Executando Consultas Otimizadas       ';
RAISE NOTICE '---------------------------------------------';


-- Consulta de Junção Otimizada
RAISE NOTICE '--- Consulta de Junção Otimizada ---';
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM _patients_augmentation p
JOIN _encounters_augmentation e ON p.id = e.patient;

-- Consulta de Filtro Otimizada
RAISE NOTICE '--- Consulta de Filtro Otimizada ---';
EXPLAIN ANALYZE
SELECT *
FROM _encounters_augmentation
WHERE description = 'Patient-initiated visit' AND encounterclass = 'outpatient';

-- Consulta de Texto Otimizada
RAISE NOTICE '--- Consulta de Texto Otimizada ---';
EXPLAIN ANALYZE
SELECT id
FROM _encounters_augmentation
WHERE description LIKE '%Hospitalization%';


----------------------------------------------------------------------------------------------------
--                                      OUTRAS OTIMIZAÇÕES: UNION vs. UNION ALL
--
-- Compara o desempenho de UNION (remove duplicatas) e UNION ALL (mantém duplicatas).
----------------------------------------------------------------------------------------------------

RAISE NOTICE '---------------------------------------------';
RAISE NOTICE '      Comparando UNION vs. UNION ALL       ';
RAISE NOTICE '---------------------------------------------';

-- Otimização de UNION (remove duplicatas)
RAISE NOTICE '--- Consulta com UNION (remove duplicatas) ---';
EXPLAIN ANALYZE
(
    SELECT id, 'patients' AS source
    FROM _patients_augmentation
    WHERE gender = 'M'
)
UNION
(
    SELECT patient AS id, 'encounters' AS source
    FROM _encounters_augmentation
    WHERE encounterclass = 'emergency'
);

-- Otimização de UNION ALL (mantém duplicatas)
RAISE NOTICE '--- Consulta com UNION ALL (mantém duplicatas) ---';
EXPLAIN ANALYZE
(
    SELECT id, 'patients' AS source
    FROM _patients_augmentation
    WHERE gender = 'M'
)
UNION ALL
(
    SELECT patient AS id, 'encounters' AS source
    FROM _encounters_augmentation
    WHERE encounterclass = 'emergency'
);

RAISE NOTICE '---------------------------------------------';
RAISE NOTICE '     Otimização de Consultas Concluída.      ';
RAISE NOTICE '---------------------------------------------';
