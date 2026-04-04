# Gold — Constellation Schema

## Conceito

Múltiplas tabelas fato compartilham dimensões conformadas.
Dimensões conformadas = mesmo conteúdo, mesmo grain, mesma surrogate key em todos os fatos.

## Dimensões conformadas do lakehouse Austa

Estas dimensões devem ser compartilhadas entre TODOS os fatos quando aplicável:

| Dimensão           | SCD | PK natural          | Compartilhável |
|--------------------|-----|---------------------|----------------|
| dim_paciente       | 2   | cd_pessoa_fisica    | Sim — todos os cubos clínicos |
| dim_medico         | 2   | cd_medico           | Sim — produção, internação, procedimento |
| dim_convenio       | 2   | cd_convenio         | Sim — financeiro, procedimento |
| dim_unidade        | 1   | cd_setor_atendimento| Sim — internação, movimentação |
| dim_tempo          | 1   | data (YYYYMMDD int) | Sim — todos os fatos |
| dim_procedimento   | 1   | cd_procedimento     | Sim — produção médica, faturamento |

## Regra de localização

- Dimensões conformadas: `models/gold/dimensions/shared/`
- Fatos: `models/gold/facts/`
- Bridge (M:N): `models/gold/facts/` com prefixo `bridge_`

## Protocolo de verificação antes de criar nova dimensão

```
1. Existe em models/gold/dimensions/shared/?
   SIM → referenciar via {{ ref('dim_{entidade}') }} — NÃO criar novo arquivo
   NÃO → verificar Glue Catalog (tasy_gold)
         SIM no Glue → criar modelo dbt correspondente ao schema existente
         NÃO no Glue → criar novo com SCD correto
```

## Regra de grain entre fatos

Se o grain do novo cubo coincidir com fato existente → propor extensão do modelo existente.
Nunca criar dois fatos com o mesmo grain — viola o Constellation Schema.
