# Gold — Regras de Tabelas Fato (Kimball)

## Obrigatório

- Grain declarado como comentário na **linha 1** do modelo SQL
- SELECT final: apenas FKs surrogate (sk_) + métricas numéricas + auditoria
- Surrogate key própria da fato: `dbt_utils.generate_surrogate_key([lista_colunas])`
- Natural keys preservadas com prefixo `nk_`
- Filtro `WHERE _is_deleted = FALSE` obrigatório (soft deletes do Silver-Context)
- `_gold_loaded_at` via `FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')`

## Proibido em tabelas fato

- Atributos descritivos (textos, nomes, descrições) — pertencem às dimensões
- Flags de status como atributos — use surrogate key para a dimensão de status
- `SELECT *` no modelo final
- Referências diretas a Silver ou Bronze

## Métricas

- **Aditivas**: podem ser somadas em qualquer dimensão (ex.: vl_procedimento, qt_procedimentos)
- **Semi-aditivas**: válidas apenas em algumas dimensões (ex.: saldo, estoque)
  → Documentar restrição explícita no schema.yml com aviso ⚠️
- **Não-aditivas**: nunca somar (ex.: percentuais, médias)
  → Documentar no schema.yml e não incluir sem justificativa

## Chave de tempo

- Surrogate key de data: `CAST(DATE_FORMAT({col_data}, 'yyyyMMdd') AS INT) AS sk_data`
- Formato: inteiro YYYYMMDD para compatibilidade com dim_tempo
