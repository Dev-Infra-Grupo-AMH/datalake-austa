# AWS — Regras de Segurança

- SEMPRE --profile ${AWS_PROFILE} em todo comando AWS CLI
- SEMPRE --region ${AWS_REGION} em todo comando AWS CLI
- Se AWS_PROFILE não estiver definido, usar profile `default` do ~/.aws/credentials
- Se nenhum profile configurado: PARAR e instruir o usuário a configurar
- Nunca hardcode de credencial, access key ou secret em qualquer arquivo
- Kyuubi: sempre read-only — nunca DDL ou DML de escrita via CLI
- glue:UpdateTable: exclusivo do processo dbt (não executar manualmente)
- S3 produção: read-only — nunca escrever diretamente no bucket raw
- Região do projeto: sa-east-1
- Bucket: austa-lakehouse-prod-data-lake-169446931765
