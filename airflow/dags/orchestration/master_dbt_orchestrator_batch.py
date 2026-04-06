"""
Orquestrador operacional (agendado): a cada hora processa o lakehouse na ordem canônica.

Fluxo padrão (sem config): bronze (todos) → silver → silver_context [→ gold quando ativado].
Opcional: passo extra de `dbt run` com --vars antes do bronze (reprocesso / janela CDC customizada).
"""
from datetime import timedelta

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from common.config import DBT_PROFILE_NAME, DBT_PROJECT_DIR, DBT_TARGET
from common.default_args import DEFAULT_ARGS


def _truthy_run_cli_first(conf: dict, params: dict) -> bool:
    """True só se run_cli_first vier explicitamente como verdadeiro (conf ou params)."""
    for src in (conf, params):
        v = src.get("run_cli_first") if isinstance(src, dict) else None
        if v is True:
            return True
        if isinstance(v, str) and v.strip().lower() in ("1", "true", "yes", "on"):
            return True
    return False


def _pick_cli_branch(**context):
    conf = context["dag_run"].conf
    if conf is None:
        conf = {}
    if not isinstance(conf, dict):
        conf = {}
    params = context.get("params") or {}
    if _truthy_run_cli_first(conf, params):
        return "dbt_run_with_vars"
    return "skip_cli_before_triggers"


_DBT_CLI_PREFIX = (
    f"cd {DBT_PROJECT_DIR} && PYTHONPATH={DBT_PROJECT_DIR}/plugins dbt run "
    f"--profile {DBT_PROFILE_NAME} --target {DBT_TARGET} "
)

_MASTER_DOC_MD = """
## Execução normal (rotina)

- **Trigger manual sem JSON** ou com `{}` → segue o mesmo fluxo do agendamento: dispara **bronze all → silver → silver_context**.
- Não é obrigatório preencher nada no formulário de parâmetros.
- A task `skip_cli_before_triggers` **só pula o passo opcional** `dbt run` na CLI; **não** pula bronze/silver/silver_context (isso exige `trigger_rule` especial após o branch — já configurado na DAG).

## Reprocessamento / janela CDC (opcional)

1. Marque **`run_cli_first`** = `True` (ou no JSON de conf abaixo).
2. Ajuste **`cdc_lookback_hours`** / **`cdc_reprocess_hours`** e, se precisar, **`dbt_select`** (default: `path:models/bronze`).

Exemplo de **JSON Configuration** ao disparar o DAG:

```json
{
  "run_cli_first": true,
  "cdc_lookback_hours": 48,
  "cdc_reprocess_hours": 24,
  "dbt_select": "path:models/bronze"
}
```

As DAGs Cosmos (bronze/silver/silver_context) continuam usando os defaults do `dbt_project.yml`, salvo o passo CLI opcional acima.
"""


@dag(
    dag_id="master_dbt_orchestrator_batch",
    description="Lakehouse: bronze all → silver → silver_context (1h ou manual); parâmetros só para reprocesso opcional",
    schedule=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    doc_md=_MASTER_DOC_MD,
    params={
        "run_cli_first": Param(
            False,
            type="boolean",
            title="Rodar dbt CLI antes do bronze",
            description=(
                "Desligado = fluxo padrão (só triggers Cosmos). "
                "Ligado = um `dbt run` com --vars (CDC) antes de bronze/silver/silver_context."
            ),
        ),
        "dbt_select": Param(
            "path:models/bronze",
            type="string",
            title="dbt --select (só com CLI)",
            description="Seleção dbt usada apenas quando 'Rodar dbt CLI antes do bronze' está ligado.",
        ),
        "cdc_lookback_hours": Param(
            2,
            type="integer",
            title="cdc_lookback_hours",
            description="Só afeta o passo CLI opcional (--vars). Rotina Cosmos usa `dbt_project.yml`.",
        ),
        "cdc_reprocess_hours": Param(
            0,
            type="integer",
            title="cdc_reprocess_hours",
            description="Só afeta o passo CLI opcional (--vars).",
        ),
    },
    tags=["orchestrator", "batch", "lakehouse", "dbt", "hourly", "scheduled"],
)
def master_dbt_orchestrator_batch_dag():
    branch = BranchPythonOperator(
        task_id="branch_cli_or_skip",
        python_callable=_pick_cli_branch,
    )
    skip_cli = EmptyOperator(task_id="skip_cli_before_triggers")
    dbt_run_with_vars = BashOperator(
        task_id="dbt_run_with_vars",
        pool="spark_dbt",
        queue="dbt",
        bash_command=(
            _DBT_CLI_PREFIX
            + "--select '{{ params.dbt_select or \"path:models/bronze\" }}' "
            + "--vars '{{ dict(cdc_lookback_hours=params.cdc_lookback_hours, "
            + "cdc_reprocess_hours=params.cdc_reprocess_hours) | tojson }}'"
        ),
    )

    trigger_bronze_all = TriggerDagRunOperator(
        task_id="trigger_bronze_dbt_task_group_all",
        trigger_dag_id="bronze_dbt_task_group_all",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False,
        # Após branch: um upstream fica skipped; all_success faria pular o trigger inteiro.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dbt_task_group_all",
        trigger_dag_id="silver_dbt_task_group_all",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False,
    )
    trigger_silver_context = TriggerDagRunOperator(
        task_id="trigger_silver_context_dbt_task_group_all",
        trigger_dag_id="silver_context_dbt_task_group_all",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False,
    )

    branch >> [skip_cli, dbt_run_with_vars]
    skip_cli >> trigger_bronze_all
    dbt_run_with_vars >> trigger_bronze_all
    trigger_bronze_all >> trigger_silver >> trigger_silver_context

    # GOLD_LAYER_TODO — descomente quando gold_dbt_task_group_all.py estiver ativo:
    # trigger_gold = TriggerDagRunOperator(
    #     task_id="trigger_gold_dbt_task_group_all",
    #     trigger_dag_id="gold_dbt_task_group_all",
    #     wait_for_completion=True,
    #     poke_interval=120,
    #     reset_dag_run=False,
    # )
    # trigger_silver_context >> trigger_gold


master_dbt_orchestrator_batch_dag()
