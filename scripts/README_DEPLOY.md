# Deploy para EC2 (Airflow + dbt)

## 1) Deploy manual local (via SSH)

No PowerShell, na raiz do projeto:

```powershell
.\scripts\deploy_to_ec2.ps1
.\scripts\validate_ec2_sync.ps1
```

## 2) Deploy automatico via GitHub Actions

O workflow `.github/workflows/deploy-airflow-dbt.yml` roda em push na branch `main`
quando houver mudancas em `airflow/dags/**` ou `dbt/**`.

Configure os seguintes secrets no repositorio GitHub:

- `EC2_HOST` -> `18.228.95.222`
- `EC2_USER` -> usuario SSH do EC2 (ex.: `ec2-user`)
- `EC2_SSH_PRIVATE_KEY` -> conteudo da chave privada PEM

## 3) Seguranca

- Nao versionar chaves PEM, logs, artefatos do dbt e arquivos temporarios.
- Credenciais do Spark/dbt devem vir de variaveis de ambiente no EC2.
