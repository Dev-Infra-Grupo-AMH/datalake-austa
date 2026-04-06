"""
Bronze acionada por dataset (stream): um único `dbt run --select <modelo>`.

Sem Cosmos → evita `dbt ls` no parse da DAG. Pool **bronze_stream**; fila **default**.
"""
from __future__ import annotations

from common.dbt_cli import dbt_run_command, dbt_subprocess_env

BRONZE_STREAM_POOL = "bronze_stream"


def bronze_dbt_run_env() -> dict[str, str]:
    return dbt_subprocess_env()


def bash_dbt_run_select(model: str) -> str:
    return dbt_run_command(select=model)
