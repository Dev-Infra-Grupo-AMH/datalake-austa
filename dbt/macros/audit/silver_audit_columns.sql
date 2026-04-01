{#
  Colunas de auditoria padronizadas para modelos Silver (Austa lakehouse, Spark + Iceberg).

  Colunas herdadas da Bronze (prefixo d. — o FROM do shaped deve ser `latest_by_pk d` ou equivalente):
    - _is_deleted: tombstone CDC como BOOLEAN
    - _cdc_op: operação CDC (c/u/d/r)
    - _cdc_event_at: instante do evento CDC
    - _cdc_ts_ms: timestamp CDC em milissegundos
    - _row_hash (alias _bronze_row_hash): hash de conteúdo negocial gravado na Bronze
    - _bronze_loaded_at: instante em que o job dbt gravou o evento na Bronze

  Colunas geradas na Silver:
    - _silver_processed_at: primeiro processamento nesta camada; usa apenas
      CONVERT_TZ(CURRENT_TIMESTAMP(), 'UTC', 'America/Sao_Paulo') (nunca CURRENT_TIMESTAMP() solto).
      Imutabilidade após o primeiro insert: garantida omitindo esta coluna de merge_update_columns
      no config do modelo, não dentro desta macro.
    - _dbt_invocation_id: identificador da execução dbt (`invocation_id`)

  Não inclui _cdc_source_table nem _source_path (somente Bronze).

  Convenção: todas as colunas vindas do upstream usam o alias de tabela `d.` no SQL gerado.
#}
{% macro silver_audit_columns() %}
  , d._is_deleted AS _is_deleted
  , d._cdc_op AS _cdc_op
  , d._cdc_event_at AS _cdc_event_at
  , d._cdc_ts_ms AS _cdc_ts_ms
  , d._row_hash AS _bronze_row_hash
  , d._bronze_loaded_at AS _bronze_loaded_at
  , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS _silver_processed_at
  , '{{ invocation_id }}' AS _dbt_invocation_id
{% endmacro %}
