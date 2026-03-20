param(
    [string]$Ec2Host = "18.228.95.222",
    [string]$Ec2User = "ec2-user",
    [string]$PemPath = "C:\Users\Rafael\.ssh\dlk-austa-sa.pem"
)

$ErrorActionPreference = "Stop"

if (!(Test-Path $PemPath)) {
    throw "Chave PEM nao encontrada em: $PemPath"
}

$sshOptions = @(
    "-o", "StrictHostKeyChecking=accept-new",
    "-i", $PemPath
)

$remoteChecks = @'
set -e
echo "== Diretorio DAGs =="
sudo ls -la /opt/airflow/dags | sed -n '1,40p'
echo
echo "== Diretorio dbt =="
sudo ls -la /opt/airflow/dbt | sed -n '1,40p'
echo
echo "== Quantidade de arquivos =="
echo "DAGs: $(sudo find /opt/airflow/dags -type f | wc -l)"
echo "dbt : $(sudo find /opt/airflow/dbt -type f | wc -l)"
echo
echo "== Arquivos de verificacao =="
sudo test -f /opt/airflow/dags/orchestration/dbt_lakehouse_dag.py && echo "OK dags/orchestration/dbt_lakehouse_dag.py"
sudo test -f /opt/airflow/dbt/dbt_project.yml && echo "OK dbt/dbt_project.yml"
'@

ssh @sshOptions "$Ec2User@$Ec2Host" $remoteChecks

Write-Host "Validacao concluida."
