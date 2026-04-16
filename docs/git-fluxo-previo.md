# Git — organização prévia (antes do Gitflow)

Este repositório ainda **não** adopta Gitflow completo (`develop`/`release`/etc.). O objectivo é só **classificar** mudanças e manter histórico legível.

## O que pedimos em cada PR

Ao abrir um pull request, o GitHub mostra o **template** (`.github/pull_request_template.md`) com a pergunta:

- **Feature** — algo novo ou uma melhoria de capacidade acordada.
- **Bugfix** — correcção de erro ou regressão.

Marque **uma** das duas opções no corpo da PR.

## Convenções leves (opcional)

| Prefixo de branch (sugestão) | Uso |
|------------------------------|-----|
| `feat/` | features |
| `fix/` | bugfixes |
| `chore/` | manutenção, tooling, sem impacto directo em dados |

Não é obrigatório seguir estes prefixos até o fluxo ser formalizado.

## Integração com `main`

O deploy descrito no [README](../README.md) continua a partir de **`main`** (GitHub Actions). As PRs devem ser revistas e fundidas em `main` quando estiverem prontas.
