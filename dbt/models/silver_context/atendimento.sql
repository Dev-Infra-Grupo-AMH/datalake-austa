{#-
  Silver-Context: Atendimento
  Join 1:1 entre atendimento_paciente e conta_paciente via nr_atendimento.
  Grão: 1 linha por atendimento.
  PK: nr_atendimento.
  Tratamento de nulos: mesmos macros da camada silver (expressões apontam para colunas da silver).
-#}
{{
  config(
    materialized='table'
    , schema='silver_context'
    , file_format='iceberg'
    , tags=["silver_context", "tasy", "atendimento"]
  )
}}

WITH atend AS (
  SELECT * FROM {{ ref('silver_tasy_atendimento_paciente') }}
)
, conta AS (
  SELECT * FROM {{ ref('silver_tasy_conta_paciente') }}
)
SELECT
    atend.nr_atendimento AS nr_atendimento
  , {{ fill_null_bigint('atend.cd_pessoa_fisica', -1) }} AS cd_pessoa_fisica
  , {{ fill_null_bigint('atend.cd_estabelecimento', -1) }} AS cd_estabelecimento
  , {{ fill_null_bigint('atend.cd_procedencia', -1) }} AS cd_procedencia
  , {{ standardize_date('atend.dt_entrada') }} AS dt_entrada
  , {{ standardize_enum('atend.ie_tipo_atendimento', "'i'") }} AS ie_tipo_atendimento
  , {{ standardize_date('atend.dt_atualizacao') }} AS dt_atualizacao
  , {{ standardize_text_initcap('atend.nm_usuario', "'indefinido'") }} AS nm_usuario
  , {{ fill_null_bigint('atend.cd_medico_resp', -1) }} AS cd_medico_resp
  , {{ fill_null_bigint('atend.cd_motivo_alta', -1) }} AS cd_motivo_alta
  , {{ standardize_text_initcap('atend.ds_sintoma_paciente', "'indefinido'") }} AS ds_sintoma_paciente
  , {{ standardize_text_initcap('atend.ds_observacao', "'indefinido'") }} AS ds_observacao
  , {{ standardize_date('atend.dt_alta') }} AS dt_alta
  , {{ standardize_enum('atend.ie_clinica', "'i'") }} AS ie_clinica
  , {{ standardize_text_initcap('atend.nm_usuario_atend', "'indefinido'") }} AS nm_usuario_atend
  , {{ standardize_enum('atend.ie_responsavel', "'i'") }} AS ie_responsavel
  , {{ standardize_date('atend.dt_fim_conta') }} AS dt_fim_conta
  , {{ standardize_enum('atend.ie_fim_conta', "'i'") }} AS ie_fim_conta
  , {{ fill_null_bigint('atend.nr_cat', -1) }} AS nr_cat
  , {{ standardize_text_initcap('atend.ds_causa_externa', "'indefinido'") }} AS ds_causa_externa
  , {{ standardize_documents('atend.cd_cgc_seguradora', 14) }} AS cd_cgc_seguradora
  , {{ fill_null_bigint('atend.nr_bilhete', -1) }} AS nr_bilhete
  , {{ fill_null_bigint('atend.nr_serie_bilhete', -1) }} AS nr_serie_bilhete
  , {{ standardize_enum('atend.ie_carater_inter_sus', "'i'") }} AS ie_carater_inter_sus
  , {{ standardize_enum('atend.ie_vinculo_sus', "'i'") }} AS ie_vinculo_sus
  , {{ standardize_enum('atend.ie_tipo_convenio', "'i'") }} AS ie_tipo_convenio
  , {{ standardize_enum('atend.ie_tipo_atend_bpa', "'i'") }} AS ie_tipo_atend_bpa
  , {{ standardize_enum('atend.ie_grupo_atend_bpa', "'i'") }} AS ie_grupo_atend_bpa
  , {{ fill_null_bigint('atend.cd_medico_atendimento', -1) }} AS cd_medico_atendimento
  , {{ standardize_date('atend.dt_alta_interno') }} AS dt_alta_interno
  , {{ fill_null_bigint('atend.nr_seq_unid_atual', -1) }} AS nr_seq_unid_atual
  , {{ fill_null_bigint('atend.nr_seq_unid_int', -1) }} AS nr_seq_unid_int
  , {{ fill_null_bigint('atend.nr_atend_original', -1) }} AS nr_atend_original
  , {{ normalize_decimal('atend.qt_dia_longa_perm', 2, -1) }} AS qt_dia_longa_perm
  , {{ standardize_date('atend.dt_inicio_atendimento') }} AS dt_inicio_atendimento
  , {{ standardize_enum('atend.ie_permite_visita', "'i'") }} AS ie_permite_visita
  , {{ standardize_enum('atend.ie_status_atendimento', "'i'") }} AS ie_status_atendimento
  , {{ standardize_date('atend.dt_previsto_alta') }} AS dt_previsto_alta
  , {{ standardize_text_initcap('atend.nm_usuario_alta', "'indefinido'") }} AS nm_usuario_alta
  , {{ fill_null_bigint('atend.cd_pessoa_responsavel', -1) }} AS cd_pessoa_responsavel
  , {{ standardize_date('atend.dt_atend_medico') }} AS dt_atend_medico
  , {{ standardize_date('atend.dt_fim_consulta') }} AS dt_fim_consulta
  , {{ standardize_date('atend.dt_medicacao') }} AS dt_medicacao
  , {{ standardize_date('atend.dt_saida_real') }} AS dt_saida_real
  , {{ standardize_enum('atend.ie_clinica_alta', "'i'") }} AS ie_clinica_alta
  , {{ standardize_date('atend.dt_lib_medico') }} AS dt_lib_medico
  , {{ fill_null_bigint('atend.nr_seq_regra_funcao', -1) }} AS nr_seq_regra_funcao
  , {{ fill_null_bigint('atend.nr_seq_local_pa', -1) }} AS nr_seq_local_pa
  , {{ fill_null_bigint('atend.nr_seq_tipo_acidente', -1) }} AS nr_seq_tipo_acidente
  , {{ standardize_date('atend.dt_ocorrencia') }} AS dt_ocorrencia
  , {{ standardize_text_initcap('atend.ds_pend_autorizacao', "'indefinido'") }} AS ds_pend_autorizacao
  , {{ fill_null_bigint('atend.nr_seq_check_list', -1) }} AS nr_seq_check_list
  , {{ standardize_date('atend.dt_fim_triagem') }} AS dt_fim_triagem
  , {{ fill_null_bigint('atend.nr_reserva_leito', -1) }} AS nr_reserva_leito
  , {{ standardize_enum('atend.ie_paciente_isolado', "'i'") }} AS ie_paciente_isolado
  , {{ standardize_enum('atend.ie_permite_visita_rel', "'i'") }} AS ie_permite_visita_rel
  , {{ standardize_text_initcap('atend.ds_senha', "'indefinido'") }} AS ds_senha
  , {{ standardize_enum('atend.ie_probabilidade_alta', "'i'") }} AS ie_probabilidade_alta
  , {{ fill_null_bigint('atend.nr_seq_forma_chegada', -1) }} AS nr_seq_forma_chegada
  , {{ fill_null_bigint('atend.nr_seq_indicacao', -1) }} AS nr_seq_indicacao
  , {{ standardize_text_initcap('atend.ds_obs_alta', "'indefinido'") }} AS ds_obs_alta
  , {{ fill_null_bigint('atend.cd_pessoa_indic', -1) }} AS cd_pessoa_indic
  , {{ standardize_text_initcap('atend.nm_medico_externo', "'indefinido'") }} AS nm_medico_externo
  , {{ fill_null_bigint('atend.nr_gestante_pre_natal', -1) }} AS nr_gestante_pre_natal
  , {{ fill_null_bigint('atend.nr_seq_forma_laudo', -1) }} AS nr_seq_forma_laudo
  , {{ standardize_date('atend.dt_alta_medico') }} AS dt_alta_medico
  , {{ fill_null_bigint('atend.cd_motivo_alta_medica', -1) }} AS cd_motivo_alta_medica
  , {{ fill_null_bigint('atend.nr_seq_pq_protocolo', -1) }} AS nr_seq_pq_protocolo
  , {{ standardize_enum('atend.ie_extra_teto', "'i'") }} AS ie_extra_teto
  , {{ fill_null_bigint('atend.cd_psicologo', -1) }} AS cd_psicologo
  , {{ fill_null_bigint('atend.nr_seq_classificacao', -1) }} AS nr_seq_classificacao
  , {{ standardize_text_initcap('atend.ds_senha_qmatic', "'indefinido'") }} AS ds_senha_qmatic
  , {{ fill_null_bigint('atend.nr_seq_queixa', -1) }} AS nr_seq_queixa
  , {{ fill_null_bigint('atend.nr_seq_triagem', -1) }} AS nr_seq_triagem
  , {{ fill_null_bigint('atend.cd_medico_referido', -1) }} AS cd_medico_referido
  , {{ standardize_enum('atend.ie_necropsia', "'i'") }} AS ie_necropsia
  , {{ fill_null_bigint('atend.nr_seq_classif_medico', -1) }} AS nr_seq_classif_medico
  , {{ standardize_enum('atend.ie_tipo_consulta', "'i'") }} AS ie_tipo_consulta
  , {{ standardize_enum('atend.ie_tipo_saida_consulta', "'i'") }} AS ie_tipo_saida_consulta
  , {{ standardize_enum('atend.ie_tipo_atend_tiss', "'i'") }} AS ie_tipo_atend_tiss
  , {{ standardize_date('atend.dt_impressao') }} AS dt_impressao
  , {{ fill_null_bigint('atend.cd_pessoa_juridica_indic', -1) }} AS cd_pessoa_juridica_indic
  , {{ fill_null_bigint('atend.nr_atendimento_mae', -1) }} AS nr_atendimento_mae
  , {{ standardize_enum('atend.ie_trat_conta_rn', "'i'") }} AS ie_trat_conta_rn
  , {{ normalize_decimal('atend.qt_dias_prev_inter', 2, -1) }} AS qt_dias_prev_inter
  , {{ fill_null_bigint('atend.nr_seq_grau_parentesco', -1) }} AS nr_seq_grau_parentesco
  , {{ standardize_enum('atend.ie_boletim_inform', "'i'") }} AS ie_boletim_inform
  , {{ standardize_date('atend.dt_chamada_paciente') }} AS dt_chamada_paciente
  , {{ standardize_enum('atend.ie_chamado', "'i'") }} AS ie_chamado
  , {{ standardize_text_initcap('atend.ds_obs_prev_alta', "'indefinido'") }} AS ds_obs_prev_alta
  , {{ standardize_enum('atend.ie_avisar_medico_referido', "'i'") }} AS ie_avisar_medico_referido
  , {{ fill_null_bigint('atend.cd_medico_chamado', -1) }} AS cd_medico_chamado
  , {{ fill_null_bigint('atend.nr_atend_alta', -1) }} AS nr_atend_alta
  , {{ standardize_date('atend.dt_saida_prev_loc_pa') }} AS dt_saida_prev_loc_pa
  , {{ standardize_enum('atend.ie_status_pa', "'i'") }} AS ie_status_pa
  , {{ standardize_date('atend.dt_liberacao_enfermagem') }} AS dt_liberacao_enfermagem
  , {{ standardize_date('atend.dt_reavaliacao_medica') }} AS dt_reavaliacao_medica
  , {{ standardize_text_initcap('atend.ds_justif_saida_real', "'indefinido'") }} AS ds_justif_saida_real
  , {{ standardize_text_initcap('atend.nm_usuario_triagem', "'indefinido'") }} AS nm_usuario_triagem
  , {{ fill_null_bigint('atend.nr_seq_ficha', -1) }} AS nr_seq_ficha
  , {{ standardize_date('atend.dt_recebimento_senha') }} AS dt_recebimento_senha
  , {{ standardize_enum('atend.ie_prm', "'i'") }} AS ie_prm
  , {{ standardize_date('atend.dt_ver_prev_alta') }} AS dt_ver_prev_alta
  , {{ fill_null_bigint('atend.nr_seq_cat', -1) }} AS nr_seq_cat
  , {{ standardize_date('atend.dt_atend_original') }} AS dt_atend_original
  , {{ standardize_date('atend.dt_chamada_enfermagem') }} AS dt_chamada_enfermagem
  , {{ fill_null_bigint('atend.nr_seq_local_destino', -1) }} AS nr_seq_local_destino
  , {{ standardize_enum('atend.ie_divulgar_obito', "'i'") }} AS ie_divulgar_obito
  , {{ fill_null_bigint('atend.nr_seq_topografia', -1) }} AS nr_seq_topografia
  , {{ fill_null_bigint('atend.nr_seq_tipo_lesao', -1) }} AS nr_seq_tipo_lesao
  , {{ standardize_enum('atend.ie_clinica_ant', "'i'") }} AS ie_clinica_ant
  , {{ fill_null_bigint('atend.cd_perfil_ativo', -1) }} AS cd_perfil_ativo
  , {{ standardize_text_initcap('atend.nm_usuario_intern', "'indefinido'") }} AS nm_usuario_intern
  , {{ standardize_text_initcap('atend.ds_obs_alta_medic', "'indefinido'") }} AS ds_obs_alta_medic
  , {{ standardize_enum('atend.ie_modo_internacao', "'i'") }} AS ie_modo_internacao
  , {{ fill_null_bigint('atend.cd_municipio_ocorrencia', -1) }} AS cd_municipio_ocorrencia
  , {{ standardize_date('atend.dt_usuario_intern') }} AS dt_usuario_intern
  , {{ standardize_date('atend.dt_chamada_reavaliacao') }} AS dt_chamada_reavaliacao
  , {{ standardize_date('atend.dt_inicio_reavaliacao') }} AS dt_inicio_reavaliacao
  , {{ fill_null_bigint('atend.nr_vinculo_censo', -1) }} AS nr_vinculo_censo
  , {{ standardize_date('atend.dt_chegada_paciente') }} AS dt_chegada_paciente
  , {{ standardize_text_initcap('atend.ds_vinculo_censo', "'indefinido'") }} AS ds_vinculo_censo
  , {{ fill_null_bigint('atend.nr_seq_triagem_prioridade', -1) }} AS nr_seq_triagem_prioridade
  , {{ standardize_text_initcap('atend.nm_usuario_saida', "'indefinido'") }} AS nm_usuario_saida
  , {{ fill_null_bigint('atend.nr_seq_triagem_old', -1) }} AS nr_seq_triagem_old
  , {{ standardize_text_initcap('atend.crm_medico_externo', "'indefinido'") }} AS crm_medico_externo
  , {{ standardize_date('atend.dt_fim_reavaliacao') }} AS dt_fim_reavaliacao
  , {{ fill_null_bigint('atend.cd_setor_obito', -1) }} AS cd_setor_obito
  , {{ standardize_text_initcap('atend.nm_usuario_alta_medica', "'indefinido'") }} AS nm_usuario_alta_medica
  , {{ fill_null_bigint('atend.nr_seq_segurado', -1) }} AS nr_seq_segurado
  , {{ standardize_date('atend.dt_alta_tesouraria') }} AS dt_alta_tesouraria
  , {{ fill_null_bigint('atend.nr_atend_origem_pa', -1) }} AS nr_atend_origem_pa
  , {{ standardize_documents('atend.cd_cgc_indicacao', 14) }} AS cd_cgc_indicacao
  , {{ standardize_date('atend.dt_inicio_observacao') }} AS dt_inicio_observacao
  , {{ standardize_date('atend.dt_fim_observacao') }} AS dt_fim_observacao
  , {{ fill_null_bigint('atend.nr_seq_pac_senha_fila', -1) }} AS nr_seq_pac_senha_fila
  , {{ standardize_date('atend.dt_cancelamento') }} AS dt_cancelamento
  , {{ standardize_text_initcap('atend.nm_usuario_cancelamento', "'indefinido'") }} AS nm_usuario_cancelamento
  , {{ standardize_text_initcap('atend.nm_usuario_prob_alta', "'indefinido'") }} AS nm_usuario_prob_alta
  , {{ standardize_enum('atend.ie_permite_acomp', "'i'") }} AS ie_permite_acomp
  , {{ fill_null_bigint('atend.nr_seq_informacao', -1) }} AS nr_seq_informacao
  , {{ standardize_text_initcap('atend.ds_informacao', "'indefinido'") }} AS ds_informacao
  , {{ fill_null_bigint('atend.nr_seq_tipo_midia', -1) }} AS nr_seq_tipo_midia
  , {{ standardize_text_initcap('atend.nm_inicio_atendimento', "'indefinido'") }} AS nm_inicio_atendimento
  , {{ standardize_text_initcap('atend.nm_fim_triagem', "'indefinido'") }} AS nm_fim_triagem
  , {{ fill_null_bigint('atend.nr_dias_prev_alta', -1) }} AS nr_dias_prev_alta
  , {{ standardize_enum('atend.ie_declaracao_obito', "'i'") }} AS ie_declaracao_obito
  , {{ standardize_date('atend.dt_chamada_medic') }} AS dt_chamada_medic
  , {{ standardize_date('atend.dt_chegada_medic') }} AS dt_chegada_medic
  , {{ fill_null_bigint('atend.nr_seq_atend_pls', -1) }} AS nr_seq_atend_pls
  , {{ fill_null_bigint('atend.nr_seq_evento_atend', -1) }} AS nr_seq_evento_atend
  , {{ standardize_date('atend.dt_descolonizar_paciente') }} AS dt_descolonizar_paciente
  , {{ standardize_text_initcap('atend.nm_descolonizar_paciente', "'indefinido'") }} AS nm_descolonizar_paciente
  , {{ standardize_date('atend.dt_checagem_adep') }} AS dt_checagem_adep
  , {{ standardize_enum('atend.ie_laudo_preenchido', "'i'") }} AS ie_laudo_preenchido
  , {{ standardize_date('atend.dt_impressao_alta') }} AS dt_impressao_alta
  , {{ fill_null_bigint('atend.nr_seq_classif_esp', -1) }} AS nr_seq_classif_esp
  , {{ fill_null_bigint('atend.cd_medico_preferencia', -1) }} AS cd_medico_preferencia
  , {{ standardize_text_initcap('atend.ds_obs_prior', "'indefinido'") }} AS ds_obs_prior
  , {{ normalize_decimal('atend.vl_consulta', 2, -1) }} AS vl_consulta
  , {{ standardize_text_initcap('atend.ds_obs_pa', "'indefinido'") }} AS ds_obs_pa
  , {{ fill_null_bigint('atend.nr_conv_interno', -1) }} AS nr_conv_interno
  , {{ standardize_date('atend.dt_inicio_prescr_pa') }} AS dt_inicio_prescr_pa
  , {{ standardize_date('atend.dt_fim_prescr_pa') }} AS dt_fim_prescr_pa
  , {{ standardize_date('atend.dt_inicio_esp_exame') }} AS dt_inicio_esp_exame
  , {{ standardize_date('atend.dt_fim_esp_exame') }} AS dt_fim_esp_exame
  , {{ fill_null_bigint('atend.nr_seq_pa_status', -1) }} AS nr_seq_pa_status
  , {{ fill_null_bigint('atend.nr_seq_registro', -1) }} AS nr_seq_registro
  , {{ standardize_enum('atend.ie_assinou_termo_biobanco', "'i'") }} AS ie_assinou_termo_biobanco
  , {{ standardize_text_initcap('atend.ds_observacao_biobanco', "'indefinido'") }} AS ds_observacao_biobanco
  , {{ fill_null_bigint('atend.nr_seq_motivo_biobanco', -1) }} AS nr_seq_motivo_biobanco
  , {{ standardize_text_initcap('atend.nm_usuario_biobanco', "'indefinido'") }} AS nm_usuario_biobanco
  , {{ standardize_date('atend.dt_atualizacao_biobanco') }} AS dt_atualizacao_biobanco
  , {{ standardize_date('atend.dt_liberacao_financeiro') }} AS dt_liberacao_financeiro
  , {{ fill_null_bigint('atend.nr_seq_ficha_lote', -1) }} AS nr_seq_ficha_lote
  , {{ standardize_enum('atend.ie_tipo_endereco_entrega', "'i'") }} AS ie_tipo_endereco_entrega
  , {{ fill_null_bigint('atend.cd_setor_usuario_atend', -1) }} AS cd_setor_usuario_atend
  , {{ standardize_date('atend.dt_ultima_menstruacao') }} AS dt_ultima_menstruacao
  , {{ normalize_decimal('atend.qt_ig_semana', 2, -1) }} AS qt_ig_semana
  , {{ normalize_decimal('atend.qt_ig_dia', 2, -1) }} AS qt_ig_dia
  , {{ fill_null_bigint('atend.cd_funcao_alta_medica', -1) }} AS cd_funcao_alta_medica
  , {{ fill_null_bigint('atend.nr_submotivo_alta', -1) }} AS nr_submotivo_alta
  , {{ standardize_enum('atend.ie_tipo_status_alta', "'i'") }} AS ie_tipo_status_alta
  , {{ standardize_enum('atend.ie_tipo_alta', "'i'") }} AS ie_tipo_alta
  , {{ standardize_enum('atend.ie_tipo_vaga', "'i'") }} AS ie_tipo_vaga
  , {{ fill_null_bigint('atend.cd_setor_desejado', -1) }} AS cd_setor_desejado
  , {{ fill_null_bigint('atend.cd_tipo_acomod_desej', -1) }} AS cd_tipo_acomod_desej
  , {{ fill_null_bigint('atend.nr_int_cross', -1) }} AS nr_int_cross
  , {{ standardize_date('atend.dt_classif_risco') }} AS dt_classif_risco
  , {{ fill_null_bigint('atend.nr_seq_ficha_lote_ant', -1) }} AS nr_seq_ficha_lote_ant
  , {{ standardize_enum('atend.ie_classif_lote_ent', "'i'") }} AS ie_classif_lote_ent
  , {{ fill_null_bigint('atend.cd_medico_infect', -1) }} AS cd_medico_infect
  , {{ fill_null_bigint('atend.nr_seq_tipo_obs_alta', -1) }} AS nr_seq_tipo_obs_alta
  , {{ fill_null_bigint('atend.nr_atend_origem_bpa', -1) }} AS nr_atend_origem_bpa
  , {{ standardize_enum('atend.ie_liberado_checkout', "'i'") }} AS ie_liberado_checkout
  , {{ fill_null_bigint('atend.nr_seq_oftalmo', -1) }} AS nr_seq_oftalmo
  , {{ standardize_text_initcap('atend.ds_senha_internet', "'indefinido'") }} AS ds_senha_internet
  , {{ standardize_date('atend.dt_atualizacao_nrec') }} AS dt_atualizacao_nrec
  , {{ standardize_text_initcap('atend.nm_usuario_nrec', "'indefinido'") }} AS nm_usuario_nrec
  , {{ standardize_enum('atend.ie_paciente_gravida', "'i'") }} AS ie_paciente_gravida
  , {{ standardize_text_initcap('atend.ds_utc', "'indefinido'") }} AS ds_utc
  , {{ standardize_enum('atend.ie_horario_verao', "'i'") }} AS ie_horario_verao
  , {{ standardize_enum('atend.ie_nivel_atencao', "'i'") }} AS ie_nivel_atencao
  , {{ standardize_text_initcap('atend.ds_utc_atualizacao', "'indefinido'") }} AS ds_utc_atualizacao
  , {{ fill_null_bigint('atend.nr_seq_episodio', -1) }} AS nr_seq_episodio
  , {{ fill_null_bigint('atend.cd_lugar_acc', -1) }} AS cd_lugar_acc
  , {{ standardize_enum('atend.ie_tipo_serv_mx', "'i'") }} AS ie_tipo_serv_mx
  , {{ fill_null_bigint('atend.cd_serv_entrada_mx', -1) }} AS cd_serv_entrada_mx
  , {{ fill_null_bigint('atend.cd_serv_sec_mx', -1) }} AS cd_serv_sec_mx
  , {{ fill_null_bigint('atend.cd_serv_ter_mx', -1) }} AS cd_serv_ter_mx
  , {{ fill_null_bigint('atend.cd_serv_alta_mx', -1) }} AS cd_serv_alta_mx
  , {{ fill_null_bigint('atend.nr_seq_tipo_admissao_fat', -1) }} AS nr_seq_tipo_admissao_fat
  , {{ standardize_enum('atend.ie_inform_incompletas', "'i'") }} AS ie_inform_incompletas
  , {{ standardize_text_initcap('atend.btn_alterar_dados_resp', "'indefinido'") }} AS btn_alterar_dados_resp
  , {{ standardize_enum('atend.ie_tipo_complemento_resp', "'i'") }} AS ie_tipo_complemento_resp
  , {{ standardize_timestamp('conta.dh_acerto_conta') }} AS dh_acerto_conta
  , {{ standardize_enum('conta.ie_status_acerto', "'i'") }} AS ie_status_acerto
  , {{ standardize_timestamp('conta.dh_periodo_inicial') }} AS dh_periodo_inicial
  , {{ standardize_timestamp('conta.dh_periodo_final') }} AS dh_periodo_final
  , {{ standardize_timestamp('conta.dh_atualizacao') }} AS conta_dh_atualizacao
  , {{ standardize_text_initcap('conta.nm_usuario', "'indefinido'") }} AS conta_nm_usuario
  , {{ fill_null_bigint('conta.cd_convenio_parametro', -1) }} AS cd_convenio_parametro
  , {{ fill_null_bigint('conta.nr_protocolo', -1) }} AS nr_protocolo
  , {{ standardize_timestamp('conta.dh_mesano_referencia') }} AS dh_mesano_referencia
  , {{ standardize_timestamp('conta.dh_mesano_contabil') }} AS dh_mesano_contabil
  , {{ fill_null_bigint('conta.cd_convenio_calculo', -1) }} AS cd_convenio_calculo
  , {{ fill_null_bigint('conta.cd_categoria_calculo', -1) }} AS cd_categoria_calculo
  , {{ fill_null_bigint('conta.nr_interno_conta', -1) }} AS nr_interno_conta
  , {{ fill_null_bigint('conta.nr_seq_protocolo', -1) }} AS nr_seq_protocolo
  , {{ fill_null_bigint('conta.cd_categoria_parametro', -1) }} AS cd_categoria_parametro
  , {{ standardize_text_initcap('conta.ds_inconsistencia', "'indefinido'") }} AS ds_inconsistencia
  , {{ standardize_timestamp('conta.dh_recalculo') }} AS dh_recalculo
  , {{ fill_null_bigint('conta.cd_estabelecimento', -1) }} AS conta_cd_estabelecimento
  , {{ standardize_enum('conta.ie_cancelamento', "'i'") }} AS ie_cancelamento
  , {{ fill_null_bigint('conta.nr_lote_contabil', -1) }} AS nr_lote_contabil
  , {{ fill_null_bigint('conta.nr_seq_apresent', -1) }} AS nr_seq_apresent
  , {{ standardize_enum('conta.ie_complexidade', "'i'") }} AS ie_complexidade
  , {{ standardize_enum('conta.ie_tipo_guia', "'i'") }} AS ie_tipo_guia
  , {{ fill_null_bigint('conta.cd_autorizacao', -1) }} AS cd_autorizacao
  , {{ normalize_decimal('conta.vl_conta', 2, -1) }} AS vl_conta
  , {{ normalize_decimal('conta.vl_desconto', 2, -1) }} AS vl_desconto
  , {{ fill_null_bigint('conta.nr_seq_conta_origem', -1) }} AS nr_seq_conta_origem
  , {{ standardize_enum('conta.ie_tipo_nascimento', "'i'") }} AS ie_tipo_nascimento
  , {{ standardize_timestamp('conta.dh_cancelamento') }} AS conta_dh_cancelamento
  , {{ fill_null_bigint('conta.cd_proc_princ', -1) }} AS cd_proc_princ
  , {{ fill_null_bigint('conta.nr_conta_convenio', -1) }} AS nr_conta_convenio
  , {{ standardize_timestamp('conta.dh_conta_definitiva') }} AS dh_conta_definitiva
  , {{ standardize_timestamp('conta.dh_conta_protocolo') }} AS dh_conta_protocolo
  , {{ fill_null_bigint('conta.nr_seq_conta_prot', -1) }} AS nr_seq_conta_prot
  , {{ fill_null_bigint('conta.nr_seq_pq_protocolo', -1) }} AS conta_nr_seq_pq_protocolo
  , {{ standardize_text_initcap('conta.ds_observacao', "'indefinido'") }} AS conta_ds_observacao
  , {{ standardize_enum('conta.ie_tipo_atend_tiss', "'i'") }} AS conta_ie_tipo_atend_tiss
  , {{ fill_null_bigint('conta.nr_seq_saida_consulta', -1) }} AS nr_seq_saida_consulta
  , {{ fill_null_bigint('conta.nr_seq_saida_int', -1) }} AS nr_seq_saida_int
  , {{ fill_null_bigint('conta.nr_seq_saida_spsadt', -1) }} AS nr_seq_saida_spsadt
  , {{ standardize_timestamp('conta.dh_geracao_tiss') }} AS dh_geracao_tiss
  , {{ fill_null_bigint('conta.nr_lote_repasse', -1) }} AS nr_lote_repasse
  , {{ standardize_enum('conta.ie_reapresentacao_sus', "'i'") }} AS ie_reapresentacao_sus
  , {{ fill_null_bigint('conta.nr_seq_ret_glosa', -1) }} AS nr_seq_ret_glosa
  , {{ standardize_timestamp('conta.dh_prev_protocolo') }} AS dh_prev_protocolo
  , {{ fill_null_bigint('conta.nr_conta_pacote_exced', -1) }} AS nr_conta_pacote_exced
  , {{ standardize_timestamp('conta.dh_alta_tiss') }} AS dh_alta_tiss
  , {{ standardize_timestamp('conta.dh_entrada_tiss') }} AS dh_entrada_tiss
  , {{ standardize_enum('conta.ie_tipo_fatur_tiss', "'i'") }} AS ie_tipo_fatur_tiss
  , {{ standardize_enum('conta.ie_tipo_consulta_tiss', "'i'") }} AS ie_tipo_consulta_tiss
  , {{ fill_null_bigint('conta.cd_especialidade_conta', -1) }} AS cd_especialidade_conta
  , {{ fill_null_bigint('conta.cd_plano_retorno_conv', -1) }} AS cd_plano_retorno_conv
  , {{ standardize_enum('conta.ie_tipo_atend_conta', "'i'") }} AS ie_tipo_atend_conta
  , {{ normalize_decimal('conta.qt_dias_conta', 2, -1) }} AS qt_dias_conta
  , {{ fill_null_bigint('conta.cd_responsavel', -1) }} AS cd_responsavel
  , {{ fill_null_bigint('conta.nr_seq_estagio_conta', -1) }} AS nr_seq_estagio_conta
  , {{ standardize_text_initcap('conta.nm_usuario_original', "'indefinido'") }} AS nm_usuario_original
  , {{ fill_null_bigint('conta.nr_seq_audit_hist_glosa', -1) }} AS nr_seq_audit_hist_glosa
  , {{ normalize_decimal('conta.vl_repasse_conta', 2, -1) }} AS vl_repasse_conta
  , {{ fill_null_bigint('conta.nr_fechamento', -1) }} AS nr_fechamento
  , {{ fill_null_bigint('conta.nr_seq_tipo_fatura', -1) }} AS nr_seq_tipo_fatura
  , {{ fill_null_bigint('conta.nr_seq_apresentacao', -1) }} AS nr_seq_apresentacao
  , {{ fill_null_bigint('conta.nr_seq_motivo_cancel', -1) }} AS nr_seq_motivo_cancel
  , {{ standardize_timestamp('conta.dh_conferencia_sus') }} AS dh_conferencia_sus
  , {{ fill_null_bigint('conta.cd_medico_conta', -1) }} AS cd_medico_conta
  , {{ normalize_decimal('conta.qt_dias_periodo', 2, -1) }} AS qt_dias_periodo
  , {{ standardize_timestamp('conta.dh_geracao_resumo') }} AS dh_geracao_resumo
  , {{ normalize_decimal('conta.vl_conta_relat', 2, -1) }} AS vl_conta_relat
  , {{ standardize_enum('conta.ie_faec', "'i'") }} AS ie_faec
  , {{ fill_null_bigint('conta.nr_seq_apresent_sus', -1) }} AS nr_seq_apresent_sus
  , {{ fill_null_bigint('conta.nr_seq_status_fat', -1) }} AS nr_seq_status_fat
  , {{ fill_null_bigint('conta.nr_seq_status_mob', -1) }} AS nr_seq_status_mob
  , {{ fill_null_bigint('conta.nr_seq_regra_fluxo', -1) }} AS nr_seq_regra_fluxo
  , {{ fill_null_bigint('conta.nr_conta_orig_desdob', -1) }} AS nr_conta_orig_desdob
  , {{ standardize_timestamp('conta.dh_atual_conta_conv') }} AS dh_atual_conta_conv
  , {{ normalize_decimal('conta.pr_coseguro_hosp', 2, -1) }} AS pr_coseguro_hosp
  , {{ normalize_decimal('conta.pr_coseguro_honor', 2, -1) }} AS pr_coseguro_honor
  , {{ normalize_decimal('conta.vl_deduzido', 2, -1) }} AS vl_deduzido
  , {{ normalize_decimal('conta.vl_base_conta', 2, -1) }} AS vl_base_conta
  , {{ standardize_timestamp('conta.dh_definicao_conta') }} AS dh_definicao_conta
  , {{ fill_null_bigint('conta.nr_seq_ordem', -1) }} AS nr_seq_ordem
  , {{ standardize_enum('conta.ie_complex_aih_orig', "'i'") }} AS ie_complex_aih_orig
  , {{ normalize_decimal('conta.vl_coseguro_honor', 2, -1) }} AS vl_coseguro_honor
  , {{ fill_null_bigint('conta.nr_seq_tipo_cobranca', -1) }} AS nr_seq_tipo_cobranca
  , {{ normalize_decimal('conta.vl_coseguro_hosp', 2, -1) }} AS vl_coseguro_hosp
  , {{ normalize_decimal('conta.pr_coseg_nivel_hosp', 2, -1) }} AS pr_coseg_nivel_hosp
  , {{ normalize_decimal('conta.vl_coseg_nivel_hosp', 2, -1) }} AS vl_coseg_nivel_hosp
  , {{ normalize_decimal('conta.vl_maximo_coseguro', 2, -1) }} AS vl_maximo_coseguro
  , {{ standardize_enum('conta.ie_claim_type', "'i'") }} AS ie_claim_type
  , {{ fill_null_bigint('conta.nr_codigo_controle', -1) }} AS nr_codigo_controle
  , {{ fill_null_bigint('conta.nr_seq_categoria_iva', -1) }} AS nr_seq_categoria_iva
  , {{ standardize_timestamp('conta.dh_geracao_rel_conv') }} AS dh_geracao_rel_conv
  , {{ fill_null_bigint('conta.nr_conta_lei_prov', -1) }} AS nr_conta_lei_prov
  , {{ standardize_enum('conta.ie_integration', "'i'") }} AS ie_integration
  , {{ fill_null_bigint('conta.nr_guia_prestador', -1) }} AS nr_guia_prestador
  , {{ fill_null_string('conta.id_delivery', "'ind'") }} AS id_delivery
  , {{ standardize_enum('conta.ie_consist_prot', "'i'") }} AS ie_consist_prot
  , {{ standardize_enum('conta.ie_status', "'i'") }} AS ie_status
  , {{ fill_null_bigint('conta.cd_plano', -1) }} AS cd_plano
  , {{ standardize_timestamp('conta.dh_geracao_anexos_tiss') }} AS dh_geracao_anexos_tiss
  , {{ standardize_timestamp('conta.dh_fim_ger_doc_tiss_relats') }} AS dh_fim_ger_doc_tiss_relats
  , {{ standardize_timestamp('conta.dh_fim_ger_doc_tiss_laudos') }} AS dh_fim_ger_doc_tiss_laudos
  , {{ standardize_timestamp('conta.dh_fim_ger_doc_tiss_exames') }} AS dh_fim_ger_doc_tiss_exames
  {{ silver_context_audit_columns() }}

FROM atend
LEFT JOIN conta
  ON atend.nr_atendimento = conta.nr_atendimento
