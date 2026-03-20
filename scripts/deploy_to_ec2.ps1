param(
    [string]$Ec2Host = "18.228.95.222",
    [string]$Ec2User = "ec2-user",
    [string]$PemPath = "C:\Users\Rafael\.ssh\dlk-austa-sa.pem"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$localDags = Join-Path $root "airflow\dags"
$localDbt = Join-Path $root "dbt"

if (!(Test-Path $PemPath)) {
    throw "Chave PEM nao encontrada em: $PemPath"
}

if (!(Test-Path $localDags)) {
    throw "Pasta local de DAGs nao encontrada: $localDags"
}

if (!(Test-Path $localDbt)) {
    throw "Pasta local do dbt nao encontrada: $localDbt"
}

$sshOptions = @(
    "-o", "StrictHostKeyChecking=accept-new",
    "-i", $PemPath
)

function Invoke-CheckedCommand {
    param([scriptblock]$Command)
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Comando falhou com codigo de saida $LASTEXITCODE"
    }
}

$dagsTar = Join-Path $env:TEMP "austa_dags.tgz"
$dbtTar = Join-Path $env:TEMP "austa_dbt.tgz"

if (Test-Path $dagsTar) { Remove-Item $dagsTar -Force }
if (Test-Path $dbtTar) { Remove-Item $dbtTar -Force }

Write-Host "Empacotando airflow/dags..."
Invoke-CheckedCommand { tar -czf $dagsTar -C $root airflow/dags }

Write-Host "Empacotando dbt..."
Invoke-CheckedCommand { tar -czf $dbtTar -C $root dbt }

Write-Host "Enviando pacotes para EC2..."
Invoke-CheckedCommand { scp @sshOptions $dagsTar "${Ec2User}@${Ec2Host}:/tmp/austa_dags.tgz" }
Invoke-CheckedCommand { scp @sshOptions $dbtTar "${Ec2User}@${Ec2Host}:/tmp/austa_dbt.tgz" }

Write-Host "Aplicando deploy em /opt/airflow com sudo..."
Invoke-CheckedCommand {
    ssh @sshOptions "$Ec2User@$Ec2Host" "sudo mkdir -p /opt/airflow/dags /opt/airflow/dbt && sudo tar -xzf /tmp/austa_dags.tgz -C /opt/airflow/dags --strip-components=2 airflow/dags && sudo tar -xzf /tmp/austa_dbt.tgz -C /opt/airflow/dbt --strip-components=1 dbt && sudo rm -rf /opt/airflow/dbt/target /opt/airflow/dbt/logs /opt/airflow/dbt/.user.yml /opt/airflow/dbt/dbt_packages && sudo find /opt/airflow/dags -type d -name __pycache__ -prune -exec rm -rf {} + && sudo rm -f /tmp/austa_dags.tgz /tmp/austa_dbt.tgz"
}

Write-Host "Deploy finalizado com sucesso."
