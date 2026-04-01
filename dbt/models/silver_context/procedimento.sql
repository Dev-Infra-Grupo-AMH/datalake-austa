{#-
  Silver-Context: Procedimento
  Join: procedimento_paciente + proc_paciente_convenio + atendimento_paciente (contexto).
  Grão: 1 linha por procedimento realizado.
  PK: nr_sequencia.
  Tratamento de nulos: mesmos macros da camada silver.
-#}
{{
  config(
    materialized='table'
    , schema='silver_context'
    , file_format='iceberg'
    , tags=["silver_context", "tasy", "procedimento"]
  )
}}

WITH proc AS (
  SELECT * FROM {{ ref('silver_tasy_procedimento_paciente') }}
)
, conv AS (
  SELECT * FROM {{ ref('silver_tasy_proc_paciente_convenio') }}
)
, atend AS (
  SELECT * FROM {{ ref('silver_tasy_atendimento_paciente') }}
)
SELECT
    proc.nr_sequencia AS nr_sequencia
  , {{ fill_null_bigint('proc.nr_atendimento', -1) }} AS nr_atendimento
  , {{ standardize_date('proc.dt_entrada_unidade') }} AS dt_entrada_unidade
  , {{ fill_null_bigint('proc.cd_procedimento', -1) }} AS cd_procedimento
  , {{ standardize_date('proc.dt_procedimento') }} AS dt_procedimento
  , {{ normalize_decimal('proc.qt_procedimento', 2, -1) }} AS qt_procedimento
  , {{ standardize_date('proc.dt_atualizacao') }} AS dt_atualizacao
  , {{ standardize_text_initcap('proc.nm_usuario', "'indefinido'") }} AS nm_usuario
  , {{ fill_null_bigint('proc.cd_medico', -1) }} AS cd_medico
  , {{ fill_null_bigint('proc.cd_convenio', -1) }} AS cd_convenio
  , {{ fill_null_bigint('proc.cd_categoria', -1) }} AS cd_categoria
  , {{ fill_null_bigint('proc.cd_pessoa_fisica', -1) }} AS cd_pessoa_fisica
  , {{ standardize_date('proc.dt_prescricao') }} AS dt_prescricao
  , {{ standardize_text_initcap('proc.ds_observacao', "'indefinido'") }} AS ds_observacao
  , {{ normalize_decimal('proc.vl_procedimento', 2, -1) }} AS vl_procedimento
  , {{ normalize_decimal('proc.vl_medico', 2, -1) }} AS vl_medico
  , {{ normalize_decimal('proc.vl_anestesista', 2, -1) }} AS vl_anestesista
  , {{ normalize_decimal('proc.vl_materiais', 2, -1) }} AS vl_materiais
  , {{ fill_null_bigint('proc.cd_edicao_amb', -1) }} AS cd_edicao_amb
  , {{ fill_null_bigint('proc.cd_tabela_servico', -1) }} AS cd_tabela_servico
  , {{ standardize_date('proc.dt_vigencia_preco') }} AS dt_vigencia_preco
  , {{ fill_null_bigint('proc.cd_procedimento_princ', -1) }} AS cd_procedimento_princ
  , {{ standardize_date('proc.dt_procedimento_princ') }} AS dt_procedimento_princ
  , {{ standardize_date('proc.dt_acerto_conta') }} AS dt_acerto_conta
  , {{ standardize_date('proc.dt_acerto_convenio') }} AS dt_acerto_convenio
  , {{ standardize_date('proc.dt_acerto_medico') }} AS dt_acerto_medico
  , {{ normalize_decimal('proc.vl_auxiliares', 2, -1) }} AS vl_auxiliares
  , {{ normalize_decimal('proc.vl_custo_operacional', 2, -1) }} AS vl_custo_operacional
  , {{ normalize_decimal('proc.tx_medico', 2, -1) }} AS tx_medico
  , {{ normalize_decimal('proc.tx_anestesia', 2, -1) }} AS tx_anestesia
  , {{ fill_null_bigint('proc.nr_prescricao', -1) }} AS nr_prescricao
  , {{ fill_null_bigint('proc.nr_sequencia_prescricao', -1) }} AS nr_sequencia_prescricao
  , {{ fill_null_bigint('proc.cd_motivo_exc_conta', -1) }} AS cd_motivo_exc_conta
  , {{ standardize_text_initcap('proc.ds_compl_motivo_excon', "'indefinido'") }} AS ds_compl_motivo_excon
  , {{ fill_null_bigint('proc.cd_acao', -1) }} AS cd_acao
  , {{ normalize_decimal('proc.qt_devolvida', 2, -1) }} AS qt_devolvida
  , {{ fill_null_bigint('proc.cd_motivo_devolucao', -1) }} AS cd_motivo_devolucao
  , {{ fill_null_bigint('proc.nr_cirurgia', -1) }} AS nr_cirurgia
  , {{ fill_null_bigint('proc.nr_doc_convenio', -1) }} AS nr_doc_convenio
  , {{ fill_null_bigint('proc.cd_medico_executor', -1) }} AS cd_medico_executor
  , {{ standardize_enum('proc.ie_cobra_pf_pj', "'i'") }} AS ie_cobra_pf_pj
  , {{ fill_null_bigint('proc.nr_laudo', -1) }} AS nr_laudo
  , {{ standardize_date('proc.dt_conta') }} AS dt_conta
  , {{ fill_null_bigint('proc.cd_setor_atendimento', -1) }} AS cd_setor_atendimento
  , {{ fill_null_bigint('proc.cd_conta_contabil', -1) }} AS cd_conta_contabil
  , {{ fill_null_bigint('proc.cd_procedimento_aih', -1) }} AS cd_procedimento_aih
  , {{ standardize_enum('proc.ie_origem_proced', "'i'") }} AS ie_origem_proced
  , {{ fill_null_bigint('proc.nr_aih', -1) }} AS nr_aih
  , {{ standardize_enum('proc.ie_responsavel_credito', "'i'") }} AS ie_responsavel_credito
  , {{ normalize_decimal('proc.tx_procedimento', 2, -1) }} AS tx_procedimento
  , {{ fill_null_bigint('proc.cd_equipamento', -1) }} AS cd_equipamento
  , {{ standardize_enum('proc.ie_valor_informado', "'i'") }} AS ie_valor_informado
  , {{ fill_null_bigint('proc.cd_estabelecimento_custo', -1) }} AS cd_estabelecimento_custo
  , {{ fill_null_bigint('proc.cd_tabela_custo', -1) }} AS cd_tabela_custo
  , {{ fill_null_bigint('proc.cd_situacao_glosa', -1) }} AS cd_situacao_glosa
  , {{ fill_null_bigint('proc.nr_lote_contabil', -1) }} AS nr_lote_contabil
  , {{ fill_null_bigint('proc.cd_procedimento_convenio', -1) }} AS cd_procedimento_convenio
  , {{ fill_null_bigint('proc.nr_seq_autorizacao', -1) }} AS nr_seq_autorizacao
  , {{ standardize_enum('proc.ie_tipo_servico_sus', "'i'") }} AS ie_tipo_servico_sus
  , {{ standardize_enum('proc.ie_tipo_ato_sus', "'i'") }} AS ie_tipo_ato_sus
  , {{ standardize_documents('proc.cd_cgc_prestador', 14) }} AS cd_cgc_prestador
  , {{ fill_null_bigint('proc.nr_nf_prestador', -1) }} AS nr_nf_prestador
  , {{ fill_null_bigint('proc.cd_atividade_prof_bpa', -1) }} AS cd_atividade_prof_bpa
  , {{ fill_null_bigint('proc.nr_interno_conta', -1) }} AS nr_interno_conta
  , {{ fill_null_bigint('proc.nr_seq_proc_princ', -1) }} AS nr_seq_proc_princ
  , {{ standardize_enum('proc.ie_guia_informada', "'i'") }} AS ie_guia_informada
  , {{ standardize_date('proc.dt_inicio_procedimento') }} AS dt_inicio_procedimento
  , {{ standardize_enum('proc.ie_emite_conta', "'i'") }} AS ie_emite_conta
  , {{ standardize_enum('proc.ie_funcao_medico', "'i'") }} AS ie_funcao_medico
  , {{ standardize_enum('proc.ie_classif_sus', "'i'") }} AS ie_classif_sus
  , {{ fill_null_bigint('proc.cd_especialidade', -1) }} AS cd_especialidade
  , {{ standardize_text_initcap('proc.nm_usuario_original', "'indefinido'") }} AS nm_usuario_original
  , {{ fill_null_bigint('proc.nr_seq_proc_pacote', -1) }} AS nr_seq_proc_pacote
  , {{ standardize_enum('proc.ie_tipo_proc_sus', "'i'") }} AS ie_tipo_proc_sus
  , {{ fill_null_bigint('proc.cd_setor_receita', -1) }} AS cd_setor_receita
  , {{ normalize_decimal('proc.vl_adic_plant', 2, -1) }} AS vl_adic_plant
  , {{ normalize_decimal('proc.qt_porte_anestesico', 2, -1) }} AS qt_porte_anestesico
  , {{ normalize_decimal('proc.tx_hora_extra', 2, -1) }} AS tx_hora_extra
  , {{ standardize_enum('proc.ie_emite_conta_honor', "'i'") }} AS ie_emite_conta_honor
  , {{ fill_null_bigint('proc.nr_seq_atepacu', -1) }} AS nr_seq_atepacu
  , {{ standardize_enum('proc.ie_proc_princ_atend', "'i'") }} AS ie_proc_princ_atend
  , {{ fill_null_bigint('proc.cd_medico_req', -1) }} AS cd_medico_req
  , {{ standardize_enum('proc.ie_tipo_guia', "'i'") }} AS ie_tipo_guia
  , {{ standardize_enum('proc.ie_video', "'i'") }} AS ie_video
  , {{ fill_null_bigint('proc.nr_doc_interno', -1) }} AS nr_doc_interno
  , {{ standardize_enum('proc.ie_auditoria', "'i'") }} AS ie_auditoria
  , {{ fill_null_bigint('proc.nr_seq_grupo_rec', -1) }} AS nr_seq_grupo_rec
  , {{ fill_null_bigint('proc.cd_medico_convenio', -1) }} AS cd_medico_convenio
  , {{ fill_null_bigint('proc.cd_motivo_ajuste', -1) }} AS cd_motivo_ajuste
  , {{ standardize_enum('proc.ie_via_acesso', "'i'") }} AS ie_via_acesso
  , {{ fill_null_bigint('proc.nr_seq_cor_exec', -1) }} AS nr_seq_cor_exec
  , {{ fill_null_bigint('proc.nr_seq_exame', -1) }} AS nr_seq_exame
  , {{ standardize_enum('proc.ie_intercorrencia', "'i'") }} AS ie_intercorrencia
  , {{ fill_null_bigint('proc.nr_seq_origem', -1) }} AS nr_seq_origem
  , {{ standardize_enum('proc.ie_dispersao', "'i'") }} AS ie_dispersao
  , {{ fill_null_bigint('proc.cd_setor_paciente', -1) }} AS cd_setor_paciente
  , {{ fill_null_bigint('proc.nr_seq_conta_origem', -1) }} AS nr_seq_conta_origem
  , {{ fill_null_bigint('proc.nr_seq_parcial', -1) }} AS nr_seq_parcial
  , {{ fill_null_bigint('proc.nr_seq_proc_interno', -1) }} AS nr_seq_proc_interno
  , {{ fill_null_bigint('proc.cd_senha', -1) }} AS cd_senha
  , {{ fill_null_bigint('proc.nr_seq_aih', -1) }} AS nr_seq_aih
  , {{ fill_null_bigint('proc.nr_doc_honor_conv', -1) }} AS nr_doc_honor_conv
  , {{ standardize_date('proc.dt_conferencia') }} AS dt_conferencia
  , {{ standardize_enum('proc.ie_tipo_atend_bpa', "'i'") }} AS ie_tipo_atend_bpa
  , {{ standardize_enum('proc.ie_grupo_atend_bpa', "'i'") }} AS ie_grupo_atend_bpa
  , {{ standardize_documents('proc.cd_cgc_prestador_conta', 14) }} AS cd_cgc_prestador_conta
  , {{ fill_null_bigint('proc.cd_medico_exec_conta', -1) }} AS cd_medico_exec_conta
  , {{ fill_null_bigint('proc.nr_seq_pq_proc', -1) }} AS nr_seq_pq_proc
  , {{ normalize_decimal('proc.qt_filme', 2, -1) }} AS qt_filme
  , {{ fill_null_bigint('proc.nr_seq_proc_crit_repasse', -1) }} AS nr_seq_proc_crit_repasse
  , {{ fill_null_bigint('proc.cd_senha_autor', -1) }} AS cd_senha_autor
  , {{ fill_null_bigint('proc.cd_autor_convenio', -1) }} AS cd_autor_convenio
  , {{ fill_null_bigint('proc.nr_seq_regra_lanc', -1) }} AS nr_seq_regra_lanc
  , {{ standardize_enum('proc.ie_tiss_tipo_guia', "'i'") }} AS ie_tiss_tipo_guia
  , {{ fill_null_bigint('proc.nr_seq_tiss_tabela', -1) }} AS nr_seq_tiss_tabela
  , {{ fill_null_bigint('proc.nr_minuto_duracao', -1) }} AS nr_minuto_duracao
  , {{ standardize_enum('proc.ie_tiss_tipo_guia_honor', "'i'") }} AS ie_tiss_tipo_guia_honor
  , {{ fill_null_bigint('proc.nr_seq_regra_preco', -1) }} AS nr_seq_regra_preco
  , {{ standardize_enum('proc.ie_tiss_tipo_guia_desp', "'i'") }} AS ie_tiss_tipo_guia_desp
  , {{ standardize_enum('proc.ie_tecnica_utilizada', "'i'") }} AS ie_tecnica_utilizada
  , {{ standardize_enum('proc.ie_tiss_tipo_despesa', "'i'") }} AS ie_tiss_tipo_despesa
  , {{ standardize_documents('proc.cd_cgc_prestador_tiss', 14) }} AS cd_cgc_prestador_tiss
  , {{ standardize_documents('proc.cd_cgc_honorario_tiss', 14) }} AS cd_cgc_honorario_tiss
  , {{ standardize_enum('proc.ie_integracao', "'i'") }} AS ie_integracao
  , {{ fill_null_bigint('proc.nr_seq_proc_acerto', -1) }} AS nr_seq_proc_acerto
  , {{ normalize_decimal('proc.vl_original_tabela', 2, -1) }} AS vl_original_tabela
  , {{ fill_null_bigint('proc.cd_doenca_cid', -1) }} AS cd_doenca_cid
  , {{ fill_null_bigint('proc.nr_seq_apac', -1) }} AS nr_seq_apac
  , {{ fill_null_bigint('proc.nr_seq_ajuste_proc', -1) }} AS nr_seq_ajuste_proc
  , {{ fill_null_bigint('proc.cd_tab_custo_preco', -1) }} AS cd_tab_custo_preco
  , {{ fill_null_bigint('proc.nr_seq_proc_autor', -1) }} AS nr_seq_proc_autor
  , {{ standardize_enum('proc.ie_doc_executor', "'i'") }} AS ie_doc_executor
  , {{ fill_null_bigint('proc.nr_seq_regra_doc', -1) }} AS nr_seq_regra_doc
  , {{ fill_null_bigint('proc.cd_cbo', -1) }} AS cd_cbo
  , {{ fill_null_bigint('proc.cd_prestador_tiss', -1) }} AS cd_prestador_tiss
  , {{ standardize_documents('proc.cd_cgc_prest_solic_tiss', 14) }} AS cd_cgc_prest_solic_tiss
  , {{ fill_null_bigint('proc.cd_setor_resp', -1) }} AS cd_setor_resp
  , {{ standardize_date('proc.dt_final_procedimento') }} AS dt_final_procedimento
  , {{ standardize_enum('proc.ie_tiss_desp_honor', "'i'") }} AS ie_tiss_desp_honor
  , {{ fill_null_bigint('proc.nr_seq_proc_est', -1) }} AS nr_seq_proc_est
  , {{ fill_null_bigint('proc.cd_prest_resp', -1) }} AS cd_prest_resp
  , {{ fill_null_bigint('proc.cd_medico_exec_tiss', -1) }} AS cd_medico_exec_tiss
  , {{ standardize_text_initcap('proc.ds_proc_tiss', "'indefinido'") }} AS ds_proc_tiss
  , {{ fill_null_bigint('proc.nr_seq_tamanho_filme', -1) }} AS nr_seq_tamanho_filme
  , {{ standardize_text_initcap('proc.ds_prestador_tiss', "'indefinido'") }} AS ds_prestador_tiss
  , {{ fill_null_bigint('proc.nr_seq_proc_crit_hor', -1) }} AS nr_seq_proc_crit_hor
  , {{ fill_null_bigint('proc.nr_seq_reg_template', -1) }} AS nr_seq_reg_template
  , {{ fill_null_bigint('proc.cd_procedimento_tuss', -1) }} AS cd_procedimento_tuss
  , {{ standardize_enum('proc.ie_ratear_item', "'i'") }} AS ie_ratear_item
  , {{ standardize_enum('proc.ie_complexidade', "'i'") }} AS ie_complexidade
  , {{ standardize_enum('proc.ie_tipo_financiamento', "'i'") }} AS ie_tipo_financiamento
  , {{ fill_null_bigint('proc.cd_funcao', -1) }} AS cd_funcao
  , {{ fill_null_bigint('proc.cd_perfil', -1) }} AS cd_perfil
  , {{ fill_null_bigint('proc.nr_seq_regra_qtde_exec', -1) }} AS nr_seq_regra_qtde_exec
  , {{ fill_null_bigint('proc.cd_centro_custo_receita', -1) }} AS cd_centro_custo_receita
  , {{ fill_null_bigint('proc.nr_seq_regra_guia_tiss', -1) }} AS nr_seq_regra_guia_tiss
  , {{ fill_null_bigint('proc.nr_seq_regra_honor_tiss', -1) }} AS nr_seq_regra_honor_tiss
  , {{ standardize_enum('proc.ie_tipo_atend_tiss', "'i'") }} AS ie_tipo_atend_tiss
  , {{ normalize_decimal('proc.vl_desp_tiss', 2, -1) }} AS vl_desp_tiss
  , {{ standardize_enum('proc.ie_resp_cred_manual', "'i'") }} AS ie_resp_cred_manual
  , {{ fill_null_bigint('proc.nr_seq_just_valor_inf', -1) }} AS nr_seq_just_valor_inf
  , {{ fill_null_bigint('proc.cd_medico_solic_tiss', -1) }} AS cd_medico_solic_tiss
  , {{ fill_null_bigint('proc.cd_medico_prof_solic_tiss', -1) }} AS cd_medico_prof_solic_tiss
  , {{ normalize_decimal('proc.vl_repasse_calc', 2, -1) }} AS vl_repasse_calc
  , {{ normalize_decimal('proc.tx_custo_oper_qt', 2, -1) }} AS tx_custo_oper_qt
  , {{ standardize_enum('proc.ie_spect', "'i'") }} AS ie_spect
  , {{ fill_null_bigint('proc.nr_seq_agenda_rxt', -1) }} AS nr_seq_agenda_rxt
  , {{ fill_null_bigint('proc.cd_prestador_solic_tiss', -1) }} AS cd_prestador_solic_tiss
  , {{ standardize_text_initcap('proc.ds_prestador_solic_tiss', "'indefinido'") }} AS ds_prestador_solic_tiss
  , {{ normalize_decimal('proc.vl_tx_desconto', 2, -1) }} AS vl_tx_desconto
  , {{ normalize_decimal('proc.vl_tx_adm', 2, -1) }} AS vl_tx_adm
  , {{ fill_null_bigint('proc.cd_medico_honor_tiss', -1) }} AS cd_medico_honor_tiss
  , {{ fill_null_bigint('proc.nr_controle', -1) }} AS nr_controle
  , {{ fill_null_bigint('proc.nr_seq_regra_taxa_cir', -1) }} AS nr_seq_regra_taxa_cir
  , {{ fill_null_bigint('proc.nr_seq_proc_orig', -1) }} AS nr_seq_proc_orig
  , {{ fill_null_bigint('proc.nr_sequencia_gas', -1) }} AS nr_sequencia_gas
  , {{ fill_null_bigint('proc.nr_seq_etapa_checkup', -1) }} AS nr_seq_etapa_checkup
  , {{ fill_null_bigint('proc.nr_seq_checkup_etapa', -1) }} AS nr_seq_checkup_etapa
  , {{ standardize_enum('proc.ie_via_hemodinamica', "'i'") }} AS ie_via_hemodinamica
  , {{ fill_null_bigint('proc.cd_regra_repasse', -1) }} AS cd_regra_repasse
  , {{ standardize_enum('proc.ie_trat_conta_rn', "'i'") }} AS ie_trat_conta_rn
  , {{ fill_null_bigint('proc.nr_seq_reg_proced', -1) }} AS nr_seq_reg_proced
  , {{ fill_null_bigint('proc.nr_seq_pepo', -1) }} AS nr_seq_pepo
  , {{ fill_null_bigint('proc.nr_seq_motivo_incl', -1) }} AS nr_seq_motivo_incl
  , {{ standardize_date('proc.dt_conversao_manual') }} AS dt_conversao_manual
  , {{ standardize_documents('proc.cd_cgc_prest_honor_tiss', 14) }} AS cd_cgc_prest_honor_tiss
  , {{ fill_null_bigint('proc.nr_seq_transfusao', -1) }} AS nr_seq_transfusao
  , {{ fill_null_bigint('proc.nr_seq_reserva', -1) }} AS nr_seq_reserva
  , {{ standardize_enum('proc.ie_lado', "'i'") }} AS ie_lado
  , {{ fill_null_bigint('proc.nr_seq_hc_equipamento', -1) }} AS nr_seq_hc_equipamento
  , {{ standardize_date('proc.dt_vinc_proced_adic') }} AS dt_vinc_proced_adic
  , {{ standardize_text_initcap('proc.ds_indicacao', "'indefinido'") }} AS ds_indicacao
  , {{ fill_null_bigint('proc.cd_tipo_anestesia', -1) }} AS cd_tipo_anestesia
  , {{ fill_null_bigint('proc.nr_seq_lanc_acao', -1) }} AS nr_seq_lanc_acao
  , {{ fill_null_bigint('proc.nr_seq_material', -1) }} AS nr_seq_material
  , {{ standardize_enum('proc.ie_carater_cirurgia', "'i'") }} AS ie_carater_cirurgia
  , {{ standardize_text_initcap('proc.ds_just_valor_inf', "'indefinido'") }} AS ds_just_valor_inf
  , {{ fill_null_bigint('proc.nr_fone_integracao', -1) }} AS nr_fone_integracao
  , {{ standardize_date('proc.dt_ligacao_integracao') }} AS dt_ligacao_integracao
  , {{ standardize_text_initcap('proc.ds_just_alter_data', "'indefinido'") }} AS ds_just_alter_data
  , {{ fill_null_bigint('proc.cd_medico_prev_laudo', -1) }} AS cd_medico_prev_laudo
  , {{ fill_null_bigint('proc.nr_seq_orig_audit', -1) }} AS nr_seq_orig_audit
  , {{ fill_null_bigint('proc.nr_seq_servico', -1) }} AS nr_seq_servico
  , {{ fill_null_bigint('proc.nr_seq_servico_classif', -1) }} AS nr_seq_servico_classif
  , {{ fill_null_bigint('proc.nr_seq_sus_equipe', -1) }} AS nr_seq_sus_equipe
  , {{ fill_null_bigint('proc.nr_seq_proc_ditado', -1) }} AS nr_seq_proc_ditado
  , {{ fill_null_bigint('proc.nr_seq_crit_honorario', -1) }} AS nr_seq_crit_honorario
  , {{ standardize_enum('proc.ie_tx_cir_tempo', "'i'") }} AS ie_tx_cir_tempo
  , {{ fill_null_bigint('proc.nr_seq_pe_prescr', -1) }} AS nr_seq_pe_prescr
  , {{ fill_null_bigint('proc.cd_medico_autenticacao', -1) }} AS cd_medico_autenticacao
  , {{ standardize_date('proc.dt_autenticacao') }} AS dt_autenticacao
  , {{ standardize_text_initcap('proc.ds_biometria', "'indefinido'") }} AS ds_biometria
  , {{ fill_null_bigint('proc.nr_seq_prescr_mat', -1) }} AS nr_seq_prescr_mat
  , {{ fill_null_bigint('proc.nr_seq_solucao', -1) }} AS nr_seq_solucao
  , {{ fill_null_bigint('proc.nr_seq_proc_desdob', -1) }} AS nr_seq_proc_desdob
  , {{ fill_null_bigint('proc.cd_medico_residente', -1) }} AS cd_medico_residente
  , {{ standardize_enum('proc.ie_retorno', "'i'") }} AS ie_retorno
  , {{ fill_null_bigint('proc.nr_ato_ipasgo', -1) }} AS nr_ato_ipasgo
  , {{ fill_null_bigint('proc.nr_seq_conta_reversao', -1) }} AS nr_seq_conta_reversao
  , {{ fill_null_bigint('proc.nr_seq_regra_pepo', -1) }} AS nr_seq_regra_pepo
  , {{ standardize_text_initcap('proc.ds_proc_tuss', "'indefinido'") }} AS ds_proc_tuss
  , {{ fill_null_bigint('proc.nr_seq_tuss_item', -1) }} AS nr_seq_tuss_item
  , {{ fill_null_bigint('proc.cd_sequencia_parametro', -1) }} AS cd_sequencia_parametro
  , {{ normalize_decimal('proc.tx_auxiliar', 2, -1) }} AS tx_auxiliar
  , {{ fill_null_bigint('proc.cd_autorizacao_prest', -1) }} AS cd_autorizacao_prest
  , {{ fill_null_bigint('proc.nr_presc_mipres', -1) }} AS nr_presc_mipres
  , {{ fill_null_bigint('proc.cd_id_entrega', -1) }} AS cd_id_entrega
  , {{ fill_null_bigint('proc.nr_seq_proc_pacote_origem', -1) }} AS nr_seq_proc_pacote_origem
  , {{ standardize_text_initcap('conv.ds_procedimento', "'indefinido'") }} AS ds_procedimento
  , {{ fill_null_bigint('conv.cd_unidade_medida', -1) }} AS cd_unidade_medida
  , {{ normalize_decimal('conv.tx_conversao_qtde', 4, -1) }} AS tx_conversao_qtde
  , {{ fill_null_bigint('conv.cd_grupo', -1) }} AS cd_grupo
  , {{ fill_null_bigint('conv.nr_proc_interno', -1) }} AS nr_proc_interno
  , {{ standardize_enum('atend.ie_tipo_atendimento', "'i'") }} AS atend_ie_tipo_atendimento
  , {{ standardize_date('atend.dt_entrada') }} AS atend_dt_entrada
  , {{ standardize_date('atend.dt_alta') }} AS atend_dt_alta
  , {{ standardize_enum('atend.ie_status_atendimento', "'i'") }} AS atend_ie_status_atendimento
  , {{ fill_null_bigint('atend.cd_medico_resp', -1) }} AS atend_cd_medico_resp
  , {{ standardize_enum('atend.ie_clinica', "'i'") }} AS atend_ie_clinica
  , {{ fill_null_bigint('atend.cd_estabelecimento', -1) }} AS atend_cd_estabelecimento
  , {{ fill_null_bigint('atend.cd_procedencia', -1) }} AS atend_cd_procedencia
  , {{ fill_null_bigint('atend.cd_motivo_alta', -1) }} AS atend_cd_motivo_alta
  {{ silver_context_audit_columns() }}

FROM proc
LEFT JOIN conv
  ON proc.nr_sequencia = conv.nr_seq_procedimento
LEFT JOIN atend
  ON proc.nr_atendimento = atend.nr_atendimento
