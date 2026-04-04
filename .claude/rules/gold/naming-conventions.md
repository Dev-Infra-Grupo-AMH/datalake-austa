# Gold — Nomenclatura

## Modelos

| Prefixo    | Tipo               | Exemplo                        |
|------------|--------------------|--------------------------------|
| fct_       | Tabela fato        | fct_producao_medica            |
| dim_       | Dimensão           | dim_paciente, dim_medico       |
| bridge_    | Tabela ponte (M:N) | bridge_atendimento_procedimento|

## Colunas

| Prefixo | Tipo                      | Exemplo                          |
|---------|---------------------------|----------------------------------|
| sk_     | Surrogate key             | sk_paciente, sk_medico, sk_fato  |
| nk_     | Natural / business key    | nk_paciente, nk_medico           |
| _       | Metadados de auditoria    | _gold_loaded_at, _valid_from     |

## Convenção de branch Git

- Gold:           `feat/gold-{nome-do-cubo}`
- Ex:             `feat/gold-producao-medica`

## Convenção de commit

- `feat(gold): add fct_producao_medica — grain: 1 linha por procedimento por médico/dia`
- `feat(gold): add dim_medico (SCD2) — compartilhada no constellation schema`

## Schemas no Glue / Spark

| Camada         | Schema no Glue      |
|----------------|---------------------|
| Gold           | gold                |
| Silver-Context | silver_context      |
| Silver         | silver              |
| Bronze         | bronze              |

## Arquivos dbt

- Modelo SQL: `{prefixo}_{nome}.sql`
- Documentação: `schema.yml` por pasta (não um por modelo)
