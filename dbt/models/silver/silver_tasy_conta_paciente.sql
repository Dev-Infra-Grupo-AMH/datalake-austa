{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='nr_atendimento',
    incremental_strategy='merge',
    merge_update_columns=[
      'dh_acerto_conta', 'ie_status_acerto', 'dh_periodo_inicial', 'dh_periodo_final', 'dh_atualizacao',
      'nm_usuario', 'cd_convenio_parametro', 'nr_protocolo', 'dh_mesano_referencia', 'dh_mesano_contabil',
      'cd_convenio_calculo', 'cd_categoria_calculo', 'nr_interno_conta', 'nr_seq_protocolo', 'cd_categoria_parametro',
      'ds_inconsistencia', 'dh_recalculo', 'cd_estabelecimento', 'ie_cancelamento', 'nr_lote_contabil',
      'nr_seq_apresent', 'ie_complexidade', 'ie_tipo_guia', 'cd_autorizacao', 'vl_conta', 'vl_desconto',
      'nr_seq_conta_origem', 'ie_tipo_nascimento', 'dh_cancelamento', 'cd_proc_princ', 'nr_conta_convenio',
      'dh_conta_definitiva', 'dh_conta_protocolo', 'nr_seq_conta_prot', 'nr_seq_pq_protocolo', 'ds_observacao',
      'ie_tipo_atend_tiss', 'nr_seq_saida_consulta', 'nr_seq_saida_int', 'nr_seq_saida_spsadt', 'dh_geracao_tiss',
      'nr_lote_repasse', 'ie_reapresentacao_sus', 'nr_seq_ret_glosa', 'dh_prev_protocolo', 'nr_conta_pacote_exced',
      'dh_alta_tiss', 'dh_entrada_tiss', 'ie_tipo_fatur_tiss', 'ie_tipo_consulta_tiss', 'cd_especialidade_conta',
      'cd_plano_retorno_conv', 'ie_tipo_atend_conta', 'qt_dias_conta', 'cd_responsavel', 'nr_seq_estagio_conta',
      'nm_usuario_original', 'nr_seq_audit_hist_glosa', 'vl_repasse_conta', 'nr_fechamento', 'nr_seq_tipo_fatura',
      'nr_seq_apresentacao', 'nr_seq_motivo_cancel', 'dh_conferencia_sus', 'cd_medico_conta', 'qt_dias_periodo',
      'dh_geracao_resumo', 'vl_conta_relat', 'ie_faec', 'nr_seq_apresent_sus', 'nr_seq_status_fat',
      'nr_seq_status_mob', 'nr_seq_regra_fluxo', 'nr_conta_orig_desdob', 'dh_atual_conta_conv', 'pr_coseguro_hosp',
      'pr_coseguro_honor', 'vl_deduzido', 'vl_base_conta', 'dh_definicao_conta', 'nr_seq_ordem', 'ie_complex_aih_orig',
      'vl_coseguro_honor', 'nr_seq_tipo_cobranca', 'vl_coseguro_hosp', 'pr_coseg_nivel_hosp', 'vl_coseg_nivel_hosp',
      'vl_maximo_coseguro', 'ie_claim_type', 'nr_codigo_controle', 'nr_seq_categoria_iva', 'dh_geracao_rel_conv',
      'nr_conta_lei_prov', 'ie_integration', 'nr_guia_prestador', 'id_delivery', 'ie_consist_prot',
      'ie_status', 'cd_plano', 'dh_geracao_anexos_tiss', 'dh_fim_ger_doc_tiss_relats', 'dh_fim_ger_doc_tiss_laudos',
      'dh_fim_ger_doc_tiss_exames', '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms', '_bronze_row_hash',
      '_bronze_loaded_at', '_dbt_invocation_id',
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=['silver', 'tasy', 'conta_paciente'],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_conta_paciente', 'nr_atendimento', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_conta_paciente') }} b
  {% if is_incremental() %}
  WHERE b._bronze_loaded_at > (
      SELECT COALESCE(MAX(s._silver_processed_at), CAST('1900-01-01' AS TIMESTAMP))
      FROM {{ this }} s
  )
  {% endif %}
)
, latest_by_pk AS (
  SELECT *
  FROM (
    SELECT
        b.*
      , ROW_NUMBER() OVER (
          PARTITION BY b.nr_atendimento
          ORDER BY
              CAST(COALESCE(b._cdc_ts_ms, 0) AS BIGINT) DESC
            , CAST(COALESCE(b.__source_txid, 0) AS BIGINT) DESC
        ) AS _rn
    FROM base b
  ) x
  WHERE _rn = 1
)
,
shaped AS (
  SELECT
      d.nr_atendimento AS nr_atendimento
    , {{ standardize_timestamp('d.dh_acerto_conta') }} AS dh_acerto_conta
    , {{ standardize_enum('d.ie_status_acerto', "'i'") }} AS ie_status_acerto
    , {{ standardize_timestamp('d.dh_periodo_inicial') }} AS dh_periodo_inicial
    , {{ standardize_timestamp('d.dh_periodo_final') }} AS dh_periodo_final
    , {{ standardize_timestamp('d.dh_atualizacao') }} AS dh_atualizacao
    , {{ standardize_text_initcap('d.nm_usuario', "'indefinido'") }} AS nm_usuario
    , {{ fill_null_bigint('d.cd_convenio_parametro', -1) }} AS cd_convenio_parametro
    , {{ fill_null_bigint('d.nr_protocolo', -1) }} AS nr_protocolo
    , {{ standardize_timestamp('d.dh_mesano_referencia') }} AS dh_mesano_referencia
    , {{ standardize_timestamp('d.dh_mesano_contabil') }} AS dh_mesano_contabil
    , {{ fill_null_bigint('d.cd_convenio_calculo', -1) }} AS cd_convenio_calculo
    , {{ fill_null_bigint('d.cd_categoria_calculo', -1) }} AS cd_categoria_calculo
    , {{ fill_null_bigint('d.nr_interno_conta', -1) }} AS nr_interno_conta
    , {{ fill_null_bigint('d.nr_seq_protocolo', -1) }} AS nr_seq_protocolo
    , {{ fill_null_bigint('d.cd_categoria_parametro', -1) }} AS cd_categoria_parametro
    , {{ standardize_text_initcap('d.ds_inconsistencia', "'indefinido'") }} AS ds_inconsistencia
    , {{ standardize_timestamp('d.dh_recalculo') }} AS dh_recalculo
    , {{ fill_null_bigint('d.cd_estabelecimento', -1) }} AS cd_estabelecimento
    , {{ standardize_enum('d.ie_cancelamento', "'i'") }} AS ie_cancelamento
    , {{ fill_null_bigint('d.nr_lote_contabil', -1) }} AS nr_lote_contabil
    , {{ fill_null_bigint('d.nr_seq_apresent', -1) }} AS nr_seq_apresent
    , {{ standardize_enum('d.ie_complexidade', "'i'") }} AS ie_complexidade
    , {{ standardize_enum('d.ie_tipo_guia', "'i'") }} AS ie_tipo_guia
    , {{ fill_null_bigint('d.cd_autorizacao', -1) }} AS cd_autorizacao
    , {{ normalize_decimal('d.vl_conta', 2, -1) }} AS vl_conta
    , {{ normalize_decimal('d.vl_desconto', 2, -1) }} AS vl_desconto
    , {{ fill_null_bigint('d.nr_seq_conta_origem', -1) }} AS nr_seq_conta_origem
    , {{ standardize_enum('d.ie_tipo_nascimento', "'i'") }} AS ie_tipo_nascimento
    , {{ standardize_timestamp('d.dh_cancelamento') }} AS dh_cancelamento
    , {{ fill_null_bigint('d.cd_proc_princ', -1) }} AS cd_proc_princ
    , {{ fill_null_bigint('d.nr_conta_convenio', -1) }} AS nr_conta_convenio
    , {{ standardize_timestamp('d.dh_conta_definitiva') }} AS dh_conta_definitiva
    , {{ standardize_timestamp('d.dh_conta_protocolo') }} AS dh_conta_protocolo
    , {{ fill_null_bigint('d.nr_seq_conta_prot', -1) }} AS nr_seq_conta_prot
    , {{ fill_null_bigint('d.nr_seq_pq_protocolo', -1) }} AS nr_seq_pq_protocolo
    , {{ standardize_text_initcap('d.ds_observacao', "'indefinido'") }} AS ds_observacao
    , {{ standardize_enum('d.ie_tipo_atend_tiss', "'i'") }} AS ie_tipo_atend_tiss
    , {{ fill_null_bigint('d.nr_seq_saida_consulta', -1) }} AS nr_seq_saida_consulta
    , {{ fill_null_bigint('d.nr_seq_saida_int', -1) }} AS nr_seq_saida_int
    , {{ fill_null_bigint('d.nr_seq_saida_spsadt', -1) }} AS nr_seq_saida_spsadt
    , {{ standardize_timestamp('d.dh_geracao_tiss') }} AS dh_geracao_tiss
    , {{ fill_null_bigint('d.nr_lote_repasse', -1) }} AS nr_lote_repasse
    , {{ standardize_enum('d.ie_reapresentacao_sus', "'i'") }} AS ie_reapresentacao_sus
    , {{ fill_null_bigint('d.nr_seq_ret_glosa', -1) }} AS nr_seq_ret_glosa
    , {{ standardize_timestamp('d.dh_prev_protocolo') }} AS dh_prev_protocolo
    , {{ fill_null_bigint('d.nr_conta_pacote_exced', -1) }} AS nr_conta_pacote_exced
    , {{ standardize_timestamp('d.dh_alta_tiss') }} AS dh_alta_tiss
    , {{ standardize_timestamp('d.dh_entrada_tiss') }} AS dh_entrada_tiss
    , {{ standardize_enum('d.ie_tipo_fatur_tiss', "'i'") }} AS ie_tipo_fatur_tiss
    , {{ standardize_enum('d.ie_tipo_consulta_tiss', "'i'") }} AS ie_tipo_consulta_tiss
    , {{ fill_null_bigint('d.cd_especialidade_conta', -1) }} AS cd_especialidade_conta
    , {{ fill_null_bigint('d.cd_plano_retorno_conv', -1) }} AS cd_plano_retorno_conv
    , {{ standardize_enum('d.ie_tipo_atend_conta', "'i'") }} AS ie_tipo_atend_conta
    , {{ normalize_decimal('d.qt_dias_conta', 2, -1) }} AS qt_dias_conta
    , {{ fill_null_bigint('d.cd_responsavel', -1) }} AS cd_responsavel
    , {{ fill_null_bigint('d.nr_seq_estagio_conta', -1) }} AS nr_seq_estagio_conta
    , {{ standardize_text_initcap('d.nm_usuario_original', "'indefinido'") }} AS nm_usuario_original
    , {{ fill_null_bigint('d.nr_seq_audit_hist_glosa', -1) }} AS nr_seq_audit_hist_glosa
    , {{ normalize_decimal('d.vl_repasse_conta', 2, -1) }} AS vl_repasse_conta
    , {{ fill_null_bigint('d.nr_fechamento', -1) }} AS nr_fechamento
    , {{ fill_null_bigint('d.nr_seq_tipo_fatura', -1) }} AS nr_seq_tipo_fatura
    , {{ fill_null_bigint('d.nr_seq_apresentacao', -1) }} AS nr_seq_apresentacao
    , {{ fill_null_bigint('d.nr_seq_motivo_cancel', -1) }} AS nr_seq_motivo_cancel
    , {{ standardize_timestamp('d.dh_conferencia_sus') }} AS dh_conferencia_sus
    , {{ fill_null_bigint('d.cd_medico_conta', -1) }} AS cd_medico_conta
    , {{ normalize_decimal('d.qt_dias_periodo', 2, -1) }} AS qt_dias_periodo
    , {{ standardize_timestamp('d.dh_geracao_resumo') }} AS dh_geracao_resumo
    , {{ normalize_decimal('d.vl_conta_relat', 2, -1) }} AS vl_conta_relat
    , {{ standardize_enum('d.ie_faec', "'i'") }} AS ie_faec
    , {{ fill_null_bigint('d.nr_seq_apresent_sus', -1) }} AS nr_seq_apresent_sus
    , {{ fill_null_bigint('d.nr_seq_status_fat', -1) }} AS nr_seq_status_fat
    , {{ fill_null_bigint('d.nr_seq_status_mob', -1) }} AS nr_seq_status_mob
    , {{ fill_null_bigint('d.nr_seq_regra_fluxo', -1) }} AS nr_seq_regra_fluxo
    , {{ fill_null_bigint('d.nr_conta_orig_desdob', -1) }} AS nr_conta_orig_desdob
    , {{ standardize_timestamp('d.dh_atual_conta_conv') }} AS dh_atual_conta_conv
    , {{ normalize_decimal('d.pr_coseguro_hosp', 2, -1) }} AS pr_coseguro_hosp
    , {{ normalize_decimal('d.pr_coseguro_honor', 2, -1) }} AS pr_coseguro_honor
    , {{ normalize_decimal('d.vl_deduzido', 2, -1) }} AS vl_deduzido
    , {{ normalize_decimal('d.vl_base_conta', 2, -1) }} AS vl_base_conta
    , {{ standardize_timestamp('d.dh_definicao_conta') }} AS dh_definicao_conta
    , {{ fill_null_bigint('d.nr_seq_ordem', -1) }} AS nr_seq_ordem
    , {{ standardize_enum('d.ie_complex_aih_orig', "'i'") }} AS ie_complex_aih_orig
    , {{ normalize_decimal('d.vl_coseguro_honor', 2, -1) }} AS vl_coseguro_honor
    , {{ fill_null_bigint('d.nr_seq_tipo_cobranca', -1) }} AS nr_seq_tipo_cobranca
    , {{ normalize_decimal('d.vl_coseguro_hosp', 2, -1) }} AS vl_coseguro_hosp
    , {{ normalize_decimal('d.pr_coseg_nivel_hosp', 2, -1) }} AS pr_coseg_nivel_hosp
    , {{ normalize_decimal('d.vl_coseg_nivel_hosp', 2, -1) }} AS vl_coseg_nivel_hosp
    , {{ normalize_decimal('d.vl_maximo_coseguro', 2, -1) }} AS vl_maximo_coseguro
    , {{ standardize_enum('d.ie_claim_type', "'i'") }} AS ie_claim_type
    , {{ fill_null_bigint('d.nr_codigo_controle', -1) }} AS nr_codigo_controle
    , {{ fill_null_bigint('d.nr_seq_categoria_iva', -1) }} AS nr_seq_categoria_iva
    , {{ standardize_timestamp('d.dh_geracao_rel_conv') }} AS dh_geracao_rel_conv
    , {{ fill_null_bigint('d.nr_conta_lei_prov', -1) }} AS nr_conta_lei_prov
    , {{ standardize_enum('d.ie_integration', "'i'") }} AS ie_integration
    , {{ fill_null_bigint('d.nr_guia_prestador', -1) }} AS nr_guia_prestador
    , {{ fill_null_string('d.id_delivery', "'ind'") }} AS id_delivery
    , {{ standardize_enum('d.ie_consist_prot', "'i'") }} AS ie_consist_prot
    , {{ standardize_enum('d.ie_status', "'i'") }} AS ie_status
    , {{ fill_null_bigint('d.cd_plano', -1) }} AS cd_plano
    , {{ standardize_timestamp('d.dh_geracao_anexos_tiss') }} AS dh_geracao_anexos_tiss
    , {{ standardize_timestamp('d.dh_fim_ger_doc_tiss_relats') }} AS dh_fim_ger_doc_tiss_relats
    , {{ standardize_timestamp('d.dh_fim_ger_doc_tiss_laudos') }} AS dh_fim_ger_doc_tiss_laudos
    , {{ standardize_timestamp('d.dh_fim_ger_doc_tiss_exames') }} AS dh_fim_ger_doc_tiss_exames
    {{ silver_audit_columns() }}
  FROM latest_by_pk d
)
, final AS (
  SELECT *
  FROM shaped
  WHERE NOT _is_deleted
)
SELECT * FROM final
