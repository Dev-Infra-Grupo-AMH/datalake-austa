# Git Workflow — Padrões Obrigatórios

- NUNCA commitar em main ou master diretamente
- Branches Gold:           feat/gold-{nome-do-cubo}
- Branches Silver-Context: feat/silver-context-{entidade}
- Branches Silver:         feat/silver-{entidade}
- Branches Bronze:         feat/bronze-{entidade}
- Commit message Gold:     "feat(gold): add {nome} dimensional model — grain: {grain}"
- Nenhum agente faz push sem aprovação humana explícita
- PR obrigatório para qualquer merge em main
- Nunca usar --no-verify ou --force-push
