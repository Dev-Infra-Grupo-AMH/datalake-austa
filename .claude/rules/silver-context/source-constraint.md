# Silver-Context — Restrição de Fonte para Gold

Gold SEMPRE consome de Silver-Context. Nunca de Silver ou Bronze diretamente.

Referência válida:   {{ ref('{entidade}') }}
                     (entidade = nome do modelo silver_context, ex: 'atendimento')

Referência inválida: {{ ref('silver_tasy_{tabela}') }}
Referência inválida: {{ ref('bronze_tasy_{tabela}') }}
Referência inválida: source('bronze', '{tabela}')
Referência inválida: source('silver', '{tabela}')

## Entidades Silver-Context disponíveis (lakehouse_tasy)

| Entidade                | PK              | Status     |
|-------------------------|-----------------|------------|
| atendimento             | nr_atendimento  | disponível |
| procedimento            | nr_sequencia    | disponível |
| movimentacao_paciente   | nr_seq_interno  | disponível |
| paciente                | id_paciente     | PENDENTE   |

## Protocolo quando entidade não existe em Silver-Context

1. PARAR a geração do modelo Gold
2. Reportar ao usuário qual entidade está faltando
3. Instruir: execute `/engenheiro cria silver-context para {entidade}` primeiro
4. Nunca usar Silver diretamente como substituto — os dados não estão no grain correto
