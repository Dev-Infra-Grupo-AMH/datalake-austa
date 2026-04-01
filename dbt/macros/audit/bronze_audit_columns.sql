{#
  Gera colunas de auditoria padronizadas para modelos bronze alimentados por CDC
  Debezium (Oracle → Kafka → S3 Sink em Avro → Spark/Iceberg no lakehouse Austa).

  Inclui: flag de tombstone, operação e instantes CDC, tabela de origem, caminho raw,
  hash de conteúdo negocial (MD5 + JSON do STRUCT) e timestamp de carga na bronze.

  Argumentos:
    raw_path (string): caminho S3 do dataset Avro bruto (ex.: s3a://bucket/.../).
    business_columns (list[str]): nomes das colunas negociais presentes no FROM
      (ex.: latest_by_pk) usadas em STRUCT para _row_hash.

  Retorno:
    Fragmento SQL com vírgula à esquerda em cada coluna; não inclui colunas de negócio.
#}
{% macro bronze_audit_columns(raw_path, business_columns) %}
  , CAST((__deleted = 'true') AS BOOLEAN) AS _is_deleted
  , __op AS _cdc_op
  , CAST(COALESCE(__ts_ms, 0) AS BIGINT) AS _cdc_ts_ms
  , CAST(FROM_UNIXTIME(CAST(COALESCE(__ts_ms, 0) AS DOUBLE) / 1000.0) AS TIMESTAMP) AS _cdc_event_at
  , __source_table AS _cdc_source_table
  , '{{ raw_path | replace("'", "''") }}' AS _source_path
  , MD5(
      TO_JSON(
        STRUCT(
          {% for col in business_columns -%}
          {{ col }}{% if not loop.last %}, {% endif %}
          {% endfor %}
        )
      )
    ) AS _row_hash
  , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS _bronze_loaded_at
{% endmacro %}
