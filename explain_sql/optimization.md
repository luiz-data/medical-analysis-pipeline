# Otimização de Queries PostgreSQL

## 📋 Visão Geral

Este script demonstra **técnicas de otimização de performance** em bancos de dados PostgreSQL, aplicadas nas tabelas augmentadas da camada Silver. O foco está em criar índices estratégicos e comparar o desempenho de queries antes e depois das otimizações.

---

## 🎯 Por Que Fazer Isso?

### Problemas que Resolve:
1. **Queries Lentas**: JOINs e filtros sem índices fazem Seq Scans (leitura completa da tabela)
2. **Timeout em Produção**: Consultas críticas que demoram minutos ou horas
3. **Uso Excessivo de CPU/Memória**: Banco de dados sobrecarregado por falta de otimização
4. **Experiência do Usuário**: Dashboards e relatórios com carregamento lento
5. **Custos de Infraestrutura**: Necessidade de hardware mais robusto sem otimizar o código

### Quando Usar:
- **Antes de ir para produção**: Validar performance com dados volumosos
- **Troubleshooting**: Quando usuários reportam lentidão
- **Maintenance Windows**: Criar índices em horários de baixo uso
- **Análise de Plano de Execução**: Identificar gargalos com `EXPLAIN ANALYZE`

---

## 🏗️ Estrutura do Script

### 0️⃣ Cenário Sem Otimização (Baseline)

```sql
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM _patients_augmentation p
JOIN _encounters_augmentation e ON p.id = e.patient;
```

**O que faz:**
- Executa queries **sem índices** para estabelecer baseline de performance
- Usa `EXPLAIN ANALYZE` para capturar tempo de execução real

**Por que comentado no script?**
- Evita executar queries lentas automaticamente
- Permite comparação manual antes/depois
- Em produção, queries sem índice podem travar o banco

---

## ⚙️ Técnicas de Otimização Implementadas

### 1️⃣ Extensão `pg_trgm` - Busca de Texto Avançada

```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
```

**O que faz:**
- Habilita índices **GIN (Generalized Inverted Index)** para buscas com `LIKE`
- Suporta busca de texto fuzzy (similaridade)

**Por que é necessário?**
- PostgreSQL **não usa índices B-tree** para `LIKE '%texto%'` (wildcards em ambos os lados)
- GIN com trigramas permite buscas parciais eficientes
- Comum em queries de autocomplete, busca de logs, análise de texto clínico

**Requisitos:**
- Necessita permissões de superusuário
- Se não tiver, solicitar ao DBA: `psql -U postgres -c "CREATE EXTENSION pg_trgm;"`

---

### 2️⃣ Índices em Chaves Primárias e Estrangeiras

```sql
CREATE INDEX idx_patients_id ON _patients_augmentation (id);
CREATE INDEX idx_encounters_patient ON _encounters_augmentation (patient);
```

**O que faz:**
- Cria índices B-tree nas colunas usadas em **JOINs**
- `id` é PK (Primary Key) de pacientes
- `patient` é FK (Foreign Key) que referencia pacientes

**Por que é crítico?**
- JOINs sem índice fazem **Nested Loop com Seq Scan** = O(n²) de complexidade
- Com índice: **Index Scan** = O(n log n) ou melhor
- Em tabelas com milhões de registros, diferença pode ser de **segundos vs horas**

**Padrão de Verificação:**
```sql
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE ...) THEN
        CREATE INDEX ...;
    END IF;
END $$;
```
- Evita erro se índice já existir
- **Idempotente**: pode re-executar o script sem problemas
- Boas práticas de scripts de deployment

---

### 3️⃣ Índices em Colunas de Filtro (WHERE)

```sql
CREATE INDEX idx_encounters_description ON _encounters_augmentation (description);
CREATE INDEX idx_encounters_class ON _encounters_augmentation (encounterclass);
```

**O que faz:**
- Cria índices em colunas usadas em **cláusulas WHERE**
- `description`: tipo de visita (ex: "Patient-initiated visit")
- `encounterclass`: classe do atendimento (ex: "outpatient", "emergency")

**Por que funciona?**
- Sem índice: PostgreSQL lê **todas as linhas** (Seq Scan)
- Com índice: Acessa diretamente as linhas que satisfazem o filtro (Index Scan)

**Exemplo de Impacto:**
```sql
-- SEM índice: Seq Scan em 10M linhas = ~5 segundos
-- COM índice: Index Scan em 50K linhas = ~50ms
WHERE description = 'Patient-initiated visit' AND encounterclass = 'outpatient';
```

---

### 4️⃣ Índice GIN para Busca de Texto com LIKE

```sql
CREATE INDEX idx_encounters_description_gin 
ON _encounters_augmentation 
USING GIN (description gin_trgm_ops);
```

**O que faz:**
- Cria índice especializado para queries com **wildcards**: `LIKE '%texto%'`
- Usa **trigramas** (sequências de 3 caracteres) para indexar o texto

**Como Funciona o pg_trgm?**
```
Texto: "Hospitalization"
Trigramas: ["hos", "osp", "spi", "pit", "ita", "tal", "ali", "liz", "iza", "zat", "ati", "tio", "ion"]
```
- Índice GIN armazena todos os trigramas
- Query `LIKE '%Hospitalization%'` procura interseção de trigramas

**Diferença de Performance:**
- **Sem GIN**: Seq Scan em 10M linhas = 8-10 segundos
- **Com GIN**: Bitmap Index Scan = 100-500ms

**Trade-offs:**
- ✅ Queries muito mais rápidas
- ❌ Índice ocupa mais espaço (~30-50% do tamanho da coluna)
- ❌ INSERTs/UPDATEs ficam um pouco mais lentos

---

### 5️⃣ Índice para Otimização de UNION

```sql
CREATE INDEX idx_patients_gender ON _patients_augmentation (gender);
```

**O que faz:**
- Índice na coluna `gender` para filtros de segmentação

**Por que neste contexto?**
- Usado na comparação **UNION vs UNION ALL**
- Permite que ambas as queries sejam executadas de forma otimizada
- Isola o efeito da operação de união (não confunde com Seq Scan)

---

## 📊 Consultas de Validação com EXPLAIN ANALYZE

### Query 1: JOIN Otimizado

```sql
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM _patients_augmentation p
JOIN _encounters_augmentation e ON p.id = e.patient;
```

**O que Validar:**
- **Antes**: Nested Loop + Seq Scan
- **Depois**: Hash Join ou Merge Join com Index Scan
- **Métrica**: Tempo de execução (deve reduzir 10x-100x)

---

### Query 2: Filtro Multi-Coluna Otimizado

```sql
EXPLAIN ANALYZE
SELECT *
FROM _encounters_augmentation
WHERE description = 'Patient-initiated visit' AND encounterclass = 'outpatient';
```

**O que Validar:**
- **Antes**: Seq Scan em toda a tabela
- **Depois**: Index Scan ou Bitmap Index Scan
- **Observação**: PostgreSQL pode usar **um** dos índices e filtrar o resto

**Dica de Otimização Avançada:**
Se ambas as colunas forem sempre filtradas juntas:
```sql
CREATE INDEX idx_encounters_desc_class ON _encounters_augmentation (description, encounterclass);
```
Índice composto é ainda mais eficiente!

---

### Query 3: Busca de Texto com LIKE

```sql
EXPLAIN ANALYZE
SELECT id
FROM _encounters_augmentation
WHERE description LIKE '%Hospitalization%';
```

**O que Validar:**
- **Antes**: Seq Scan (único jeito sem pg_trgm)
- **Depois**: Bitmap Index Scan usando GIN
- **Impacto**: Redução de segundos para milissegundos

---

## 🔄 Comparação: UNION vs UNION ALL

### UNION (Remove Duplicatas)

```sql
(SELECT id, 'patients' AS source FROM _patients_augmentation WHERE gender = 'M')
UNION
(SELECT patient AS id, 'encounters' AS source FROM _encounters_augmentation WHERE encounterclass = 'emergency');
```

**Como Funciona:**
1. Executa ambas as queries
2. **Ordena** os resultados (Sort)
3. Remove duplicatas (Unique)
4. Retorna resultado final

**Custo Computacional:**
- Operação de **Sort**: O(n log n)
- Comparação de duplicatas: O(n)
- Uso de memória temporária (work_mem)

---

### UNION ALL (Mantém Duplicatas)

```sql
(SELECT id, 'patients' AS source FROM _patients_augmentation WHERE gender = 'M')
UNION ALL
(SELECT patient AS id, 'encounters' AS source FROM _encounters_augmentation WHERE encounterclass = 'emergency');
```

**Como Funciona:**
1. Executa ambas as queries
2. **Concatena** os resultados diretamente
3. Retorna resultado final (sem processamento adicional)

**Custo Computacional:**
- Apenas Append: O(n)
- Sem uso de memória extra

---

### Quando Usar Cada Um?

| Cenário | Usar | Motivo |
|---------|------|--------|
| Sabe que não há duplicatas | `UNION ALL` | Performance superior |
| Duplicatas são esperadas e indesejadas | `UNION` | Garantir unicidade |
| Combinando tabelas disjuntas | `UNION ALL` | Ex: logs de 2024 + logs de 2025 |
| Agregando dados sem controle | `UNION` | Segurança contra duplicatas |

**Regra de Ouro:**
> Se você **não precisa** remover duplicatas, **sempre use `UNION ALL`**. É mais rápido e usa menos recursos.

---

## 📈 Métricas de Impacto Esperadas

### Exemplo de Ganhos de Performance:

| Query | Sem Índice | Com Índice | Melhoria |
|-------|-----------|-----------|----------|
| JOIN (10M x 100M) | 45s | 2.3s | **19x mais rápido** |
| Filtro Multi-Coluna | 8.5s | 0.15s | **56x mais rápido** |
| LIKE '%texto%' | 12s | 0.4s | **30x mais rápido** |
| UNION vs UNION ALL | 3.2s | 1.1s | **3x mais rápido** |

**Nota:** Valores variam conforme:
- Volume de dados
- Seletividade dos filtros (quantos % das linhas satisfazem)
- Hardware (RAM, CPU, disco)
- Configurações do PostgreSQL (shared_buffers, work_mem)

---

## 🛠️ Como Interpretar EXPLAIN ANALYZE

### Principais Componentes:

```sql
EXPLAIN ANALYZE
SELECT ...
```

**Output Importante:**
1. **Seq Scan**: ❌ Lendo tabela inteira (ruim para tabelas grandes)
2. **Index Scan**: ✅ Usando índice (ótimo)
3. **Bitmap Index Scan**: ✅ Usando índice + bitmap (bom para múltiplas condições)
4. **Hash Join / Merge Join**: ✅ JOINs eficientes
5. **Nested Loop**: ⚠️ Pode ser lento se não houver índices

**Métricas Chave:**
- **Execution Time**: Tempo real de execução
- **Planning Time**: Tempo que o optimizer levou para escolher o plano
- **Rows**: Quantas linhas foram processadas
- **Buffers**: Leituras de disco vs cache

---

## ⚠️ Considerações Importantes

### Quando NÃO Criar Índices:

1. **Colunas com baixa cardinalidade**:
   - Ex: `status` com apenas 3 valores ("active", "inactive", "pending")
   - Índice pode ser ignorado pelo optimizer

2. **Tabelas pequenas** (< 10.000 linhas):
   - Seq Scan pode ser mais rápido que Index Scan

3. **Colunas raramente usadas em filtros**:
   - Índices ocupam espaço e tornam writes mais lentos

4. **Tabelas com muitos INSERTs**:
   - Cada índice adicional torna INSERTs mais lentos
   - Balancear read vs write performance

### Manutenção de Índices:

```sql
-- Reindexar índices fragmentados (executar periodicamente)
REINDEX INDEX idx_encounters_patient;

-- Ou reindexar tabela inteira
REINDEX TABLE _encounters_augmentation;

-- Analisar estatísticas (permite ao optimizer escolher melhor plano)
ANALYZE _encounters_augmentation;

-- Vacuum para limpar registros mortos
VACUUM ANALYZE _encounters_augmentation;
```

**Quando Fazer:**
- **REINDEX**: Após muitos UPDATEs/DELETEs (mensal ou trimestral)
- **ANALYZE**: Após cargas grandes de dados
- **VACUUM**: Semanalmente em tabelas com alta rotatividade

---

## 🚀 Como Executar

### 1. Preparação

```bash
# Conectar ao banco
psql -h <host> -U <user> -d <database>

# Garantir que você tem as tabelas augmentadas
\dt _*augmentation
```

### 2. Execução do Script

```bash
# Executar script completo
\i optimization.sql

# Ou copiar/colar seções específicas no psql
```

### 3. Monitoramento

```sql
-- Verificar índices criados
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename LIKE '%augmentation%'
ORDER BY tablename, indexname;

-- Verificar tamanho dos índices
SELECT
    indexrelname AS index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
  AND relname LIKE '%augmentation%'
ORDER BY pg_relation_size(indexrelid) DESC;
```

---

## 💡 Boas Práticas de Otimização

### 1. **Sempre Use EXPLAIN ANALYZE**
- Não otimize no escuro
- Meça antes e depois
- Valide em dados de produção (ou similares)

### 2. **Índices Compostos para Queries Comuns**
```sql
-- Se sempre filtrar por (data + status), criar índice composto:
CREATE INDEX idx_encounters_date_status ON encounters (encounter_date, status);
```

### 3. **Ordem das Colunas em Índices Compostos Importa**
```sql
-- Correto: coluna mais seletiva primeiro
CREATE INDEX idx ON table (high_cardinality_col, low_cardinality_col);

-- Incorreto: coluna menos seletiva primeiro
CREATE INDEX idx ON table (low_cardinality_col, high_cardinality_col);
```

### 4. **Partial Indexes para Filtros Comuns**
```sql
-- Se 90% das queries filtram por status = 'active':
CREATE INDEX idx_active_encounters ON encounters (encounter_date) 
WHERE status = 'active';
```

### 5. **Evite SELECT ***
```sql
-- Ruim: traz todas as colunas
SELECT * FROM encounters WHERE ...;

-- Bom: traz apenas o necessário
SELECT encounter_id, encounter_date, patient_id FROM encounters WHERE ...;
```

---

## 📚 Referências

- PostgreSQL Documentation - Indexes: https://www.postgresql.org/docs/current/indexes.html
- pg_trgm Extension: https://www.postgresql.org/docs/current/pgtrgm.html
- EXPLAIN ANALYZE Tutorial: https://www.postgresql.org/docs/current/using-explain.html
- PostgreSQL Performance Tuning Guide

---

## 🔍 Troubleshooting

### Problema: Índice GIN não está sendo usado

**Solução:**
```sql
-- Verificar se extensão está ativa
SELECT * FROM pg_extension WHERE extname = 'pg_trgm';

-- Verificar se índice existe
\d _encounters_augmentation

-- Forçar uso de índice (apenas para teste)
SET enable_seqscan = off;
```

### Problema: Queries ainda lentas após criar índices

**Investigar:**
1. Verificar se índice está sendo usado: `EXPLAIN ANALYZE`
2. Atualizar estatísticas: `ANALYZE table_name;`
3. Aumentar work_mem temporariamente: `SET work_mem = '256MB';`
4. Verificar seletividade: índice só ajuda se filtra < 10-15% das linhas

---
