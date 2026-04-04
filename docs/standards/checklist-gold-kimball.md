# Checklist Gold — Kimball Compliance
# Lakehouse Austa · Hospital Austa

Aplique este checklist manualmente a cada cubo dimensional gerado pelo Engineer Gold Agent.

---

## 1. Inventário e Reuso (PASSO 0.5)

- [ ] Inventário Gold executado antes de qualquer SQL
- [ ] Relatório de Inventário apresentado ao usuário e confirmado
- [ ] Dimensões existentes reutilizadas via `{{ ref('dim_{x}') }}` — não recriadas
- [ ] Nenhuma dimensão criada se já existia em `/dimensions/shared/`
- [ ] Grain do novo cubo não duplica grain de fato já existente
- [ ] Se grain coincide com fato existente: extensão proposta ao invés de novo modelo

---

## 2. Fonte de Dados

- [ ] TODAS as referências são `{{ ref('{entidade}') }}` — Silver-Context
- [ ] NENHUMA referência a `silver_tasy_*` diretamente
- [ ] NENHUMA referência a `bronze_tasy_*` ou `source('bronze', ...)`
- [ ] Entidade consumida existe e está disponível em silver_context (Glue)

---

## 3. Tabela Fato

- [ ] Grain declarado como comentário na **linha 1** do arquivo `.sql`
- [ ] SELECT final contém apenas: FKs surrogate (`sk_`) + métricas numéricas + audit
- [ ] Nenhum atributo descritivo (nome, descrição, flag de texto) na tabela fato
- [ ] Surrogate key da fato via `dbt_utils.generate_surrogate_key()`
- [ ] Natural keys preservadas com prefixo `nk_`
- [ ] Métricas semi-aditivas documentadas com aviso ⚠️ no `schema.yml`
- [ ] Filtro `WHERE _is_deleted = FALSE` aplicado
- [ ] `sk_data` em formato inteiro YYYYMMDD
- [ ] `_gold_loaded_at` via `FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')`

---

## 4. Dimensão

- [ ] SCD Type (1 ou 2) declarado no comentário do modelo
- [ ] Surrogate key gerada via `dbt_utils.generate_surrogate_key()`
- [ ] Natural key preservada com prefixo `nk_`
- [ ] **SCD2**: colunas `_valid_from`, `_valid_to`, `_is_current` presentes
- [ ] **SCD2**: `LEAD()` com `PARTITION BY {pk_col} ORDER BY _context_processed_at`
- [ ] Dimensão criada em `/dimensions/shared/` (não em `/facts/`)
- [ ] Não duplica dimensão já existente no constellation schema

---

## 5. schema.yml

- [ ] Grain documentado na `description` da tabela fato
- [ ] Tests `unique` + `not_null` na surrogate key de cada modelo
- [ ] Tests `relationships` nas FKs do fato apontando para as dimensões corretas
- [ ] Tags presentes: `["gold", "tasy", "{processo}"]`
- [ ] Colunas de métricas semi-aditivas com aviso de restrição na `description`
- [ ] Dimensões SCD2: colunas `_valid_from`, `_valid_to`, `_is_current` documentadas

---

## 6. Código

- [ ] Snake case em tudo (colunas, CTEs, aliases)
- [ ] Sem `SELECT *` no modelo final
- [ ] Vírgulas à esquerda (estilo dbt)
- [ ] CTEs nomeadas semanticamente (`source`, `final`)
- [ ] Timestamp com `FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')`
- [ ] Nenhuma credencial hardcoded
- [ ] Kyuubi read-only — sem DDL manual

---

## 7. Git e Deploy

- [ ] Branch criada com prefixo `feat/gold-`
- [ ] Commit message segue: `feat(gold): add {nome} — grain: {grain}`
- [ ] Nenhum commit direto em `main`
- [ ] PR aberto para revisão antes do merge
- [ ] `dbt deps` rodado após adição de dependência no `packages.yml`
- [ ] `dbt compile` validado localmente antes do merge

---

## Dimensões Conformadas — Mapa de Reuso

| Dimensão             | SCD | Fonte Silver-Context  | Fatos que devem usar |
|----------------------|-----|-----------------------|----------------------|
| dim_paciente         | 2   | paciente (PENDENTE)   | todos os cubos clínicos |
| dim_medico           | 2   | procedimento.cd_medico| fct_producao_medica, fct_atendimento |
| dim_convenio         | 2   | procedimento.cd_convenio | fct_producao_medica, fct_faturamento |
| dim_unidade          | 1   | movimentacao_paciente | fct_internacao, fct_movimentacao |
| dim_tempo            | 1   | (gerada a partir de datas) | todos |
| dim_procedimento     | 1   | procedimento.cd_procedimento | fct_producao_medica |
