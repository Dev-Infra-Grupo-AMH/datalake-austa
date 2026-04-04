# SQL Style — Padrões Globais

- Snake case em tudo: colunas, modelos, CTEs, aliases
- Sem SELECT * em modelos finais — colunas sempre explícitas
- CTEs nomeadas semanticamente (source, latest_by_pk, final, deduplicated)
- Comentários de grain e fonte obrigatórios no topo de modelos fato
- Indentação: 4 espaços
- Vírgulas no início da linha (estilo dbt)
- Palavras-chave SQL em MAIÚSCULAS (SELECT, FROM, WHERE, JOIN, etc.)
- Aliases sempre explícitos em JOINs (ex.: `atend.nr_atendimento`)
- Sem alias de tabela de uma só letra — usar nome semântico
