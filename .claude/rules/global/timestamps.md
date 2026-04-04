# Timestamps — Padrão Obrigatório

SEMPRE: FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')
NUNCA:  NOW()
NUNCA:  CURRENT_TIMESTAMP() sem conversão de timezone
NUNCA:  GETDATE() ou equivalentes de outros dialetos
NUNCA:  CONVERT_TZ()

Timezone do projeto: America/Sao_Paulo (UTC-3 / UTC-2 em horário de verão)

Aplicar em:
- _gold_loaded_at (fatos e dimensões Gold)
- _context_processed_at (Silver-Context)
- Qualquer coluna de auditoria de carga
