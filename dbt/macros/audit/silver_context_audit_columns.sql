{#
  Colunas de auditoria padronizadas para modelos Silver-Context (Austa lakehouse, Spark + Iceberg).

  Nesta camada os dados já passaram por bronze (CDC) e silver (dedup + padronização).
  Apenas registra o instante de processamento e a invocação dbt.

  Colunas geradas:
    - _context_processed_at: timestamp do processamento nesta camada
      (FROM_UTC_TIMESTAMP → America/Sao_Paulo, consistente com _silver_processed_at).
    - _dbt_invocation_id: identificador da execução dbt.
#}
{% macro silver_context_audit_columns() %}
  , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS _context_processed_at
  , '{{ invocation_id }}' AS _dbt_invocation_id
{% endmacro %}
