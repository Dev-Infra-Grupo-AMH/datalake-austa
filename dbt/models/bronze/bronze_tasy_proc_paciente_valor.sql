-- Bronze PROC_PACIENTE_VALOR: Raw Avro (stream) → Iceberg
-- CDC do Oracle via Kafka. Colunas de controle obrigatórias.
{{
  config(
    materialized='table',
    schema='bronze',
    file_format='iceberg',
  )
}}
{% set raw_path = "s3a://" ~ var('datalake_bucket') ~ "/" ~ var('raw_tasy_stream_prefix') ~ var('raw_tasy_stream_topic_proc_paciente_valor') ~ "/" %}

SELECT
  *,
  CURRENT_TIMESTAMP() AS dt_criacao,
  CURRENT_TIMESTAMP() AS dt_atualizacao_lakehouse,
  '{{ var("fonte_sistema") }}' AS fonte_sistema
FROM avro.`{{ raw_path }}`
