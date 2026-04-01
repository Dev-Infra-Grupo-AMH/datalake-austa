# Camada Bronze — padrão de código (Tasy / CDC)

Documentação das convenções dos modelos em `dbt/models/bronze/`, alinhadas ao ingest **Avro no S3** (Kafka/Debezium) e materialização **Iceberg** incremental.

**Onde ler o quê**

| Documento | Conteúdo |
|-----------|----------|
| Este arquivo (`docs/bronze.md`) | SQL, CTEs CDC, `config`, `dt_`→`dh_`, `partition_path`, formatação, `schema.yml` em detalhe |
| `docs/dbt_camadas.md` | Inventário de modelos, pseudo-código bronze/silver/context e checklist para juniors |
| `dbt/docs/DATA_CONTRACTS.md` | Contrato por camada (append na bronze, dedup na silver) |
| `CLAUDE.md` (Seção 2 — dbt) | Nomenclatura de camadas, regras resumidas da bronze stream, testes por camada, **templates** `schema.yml` (stream + legado), silver/gold |

---

## Papel da camada

- Ler o stream CDC em **Avro** (`avro.\`s3a://.../tasy.TASY.{TABELA}/\``).
- Aplicar **janela incremental** por `__ts_ms` (watermark + lookback/reprocess via `vars`).
- **Implementação atual neste repositório:** materializar **incremental** com **`incremental_strategy='append'`** no Iceberg — ou seja, a bronze pode conter **várias linhas por chave de negócio** ao longo do tempo; a **deduplicação “estado atual”** é feita na **camada silver** (`ROW_NUMBER` + `merge`).
- **Sem joins** e **sem lógica de negócio** — só padronização de nomes, exposição de metadados e convenção de datas.

---

## Nomenclatura de modelos

| Item | Padrão |
|------|--------|
| Arquivo / modelo dbt | `bronze_tasy_{entidade}` (ex.: `bronze_tasy_atendimento_paciente`) |
| Pasta | `dbt/models/bronze/` |
| Documentação e testes | `dbt/models/bronze/schema.yml` (nomes de colunas **iguais** ao `SELECT` final) |

---

## Configuração dbt (`config`)

Padrão recorrente nos modelos stream:

```jinja
{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}
```

- **`append`**: cada execução grava **novas linhas** da janela CDC; não há `unique_key` na bronze. Testes de unicidade da chave de negócio aplicam-se ao grão **deduplicado na silver**, não à bronze eventizada.
- **Variante com `merge` na bronze** (se o time adotar no futuro): aí sim `unique_key` deve alinhar ao `PARTITION BY` do `ROW_NUMBER()` interno.

---

## Estrutura SQL (CTEs)

Ordem na **implementação atual** dos `bronze_tasy_*`:

1. **`target_watermark`** — em incremental, `MAX(_cdc_ts_ms)` de `{{ this }}`; senão `0`. (Persistido na tabela a partir das cargas anteriores.)
2. **`params`** — calcula `wm_start_ms` (reprocess em horas ou lookback sobre o watermark).
3. **`raw_incremental`** — `SELECT *` do `avro.\`{{ raw_path }}\`` com filtro `CAST(COALESCE(__ts_ms, 0) AS BIGINT) >= wm_start_ms`.
4. **`SELECT` final** — lista explícita de colunas de negócio (`dt_*` → `dh_*` onde aplicável) + `{{ bronze_audit_columns(raw_path, lista_colunas_negocio) }}` + `__source_txid` (para ordenação na silver).

**Opcional / legado em documentação antiga:** CTE `latest_by_pk` com `ROW_NUMBER()` **antes** do `SELECT` final — **não** é o padrão dos arquivos atuais; a deduplicação por PK fica na **silver**.

Variáveis no topo do arquivo:

- `{% set raw_path = "s3a://.../tasy.TASY.{TABELA}/" %}`
- `cdc_lookback_hours` / `cdc_reprocess_hours` via `var(...)`.

---

## Nomes de colunas no resultado

- **snake_case em minúsculas** para colunas de negócio (`nr_atendimento`, `cd_convenio`, `vl_conta`, …).
- Metadados CDC com prefixo **`__`**: `__op`, `__ts_ms`, `__deleted`, `__source_table` (quando existirem no stream).

---

## Convenção `dt_` → `dh_` (datas)

- Campos de data/hora vindos do Tasy como **`dt_*`** (ou `DT_*` no Avro) devem ser expostos com alias **`dh_*`**, **mesmo sufixo**, só troca do prefixo:
  - Ex.: `dt_entrada AS dh_entrada`, `DT_ATUALIZACAO AS dh_atualizacao`.
- **`create_at`**, **`update_at`**, **`source`** não seguem essa regra (são metadados de carga, não colunas `dt_` de negócio).

Auditoria no `SELECT` final (padrão atual): usar a macro **`bronze_audit_columns`** (ver `dbt/docs/MACROS.md`), que define `_bronze_loaded_at`, `_cdc_ts_ms`, `_is_deleted`, `_row_hash`, etc. Modelos legados podem ainda expor `create_at` / `update_at` / `source` — alinhar ao modelo de referência do time.

---

## Coluna `partition_path`

Expressão padrão no `SELECT` final:

```sql
, '{{ raw_path }}' || '/' || year || '/' || month || '/' || day || '/' || hour || '/' AS partition_path
```

(ajustar se o layout de partição do path mudar.)

---

## Formatação SQL

- **Vírgula à esquerda** da coluna seguinte no `SELECT` e nas listas do `config`:
  - Ex.: ` , nr_sequencia` e ` , schema='bronze'`.
- Palavras-chave SQL em uso misto no projeto (`select`/`end` minúsculos em alguns arquivos) — preferível padronizar em um estilo por PR; o **obrigatório** é vírgula inicial nas listas longas.

---

## O que não fazer na bronze

- `ref()` para substituir leitura do raw Avro (a fonte é o path/stream, não outro modelo dbt para o mesmo raw).
- Joins entre entidades.
- Macros novas “só para bronze” se o time quiser manter SQL explícito legível no arquivo.
- Lógica de negócio (filtros de regra clínica, agregações analíticas) — belongs to **silver** ou acima.

---

## `schema.yml` (bronze)

- Nomes das colunas no YAML = **nomes exatos** do `SELECT` (minúsculas).
- Documentar colunas do `SELECT` final com **nomes exatos**; para CDC: `_cdc_op` (`accepted_values`), `_cdc_ts_ms` (`not_null`), `_is_deleted`, chave de negócio com `not_null` (unicidade **no grão silver**, não na bronze append).
- Para **valor** (`PROC_PACIENTE_VALOR`), chave composta de negócio: documentar ambas com `not_null`.
- Para **convenio**, documentar a chave de negócio usada na silver (`unique_key`).

---

## Referências no repositório

- Modelos: `dbt/models/bronze/*.sql`
- Testes/documentação: `dbt/models/bronze/schema.yml`
- Variáveis CDC: `dbt_project.yml` (`cdc_lookback_hours`, `cdc_reprocess_hours`)
- Contexto geral (Airflow, infra, dbt): `CLAUDE.md`
- Templates YAML bronze (stream e legado) e checklist de testes: `CLAUDE.md` → **Seção 2: dbt**
