"""
dbt via BashOperator no worker Celery: PATH mínimo, executável explícito, DBT_PROFILES_DIR.

O Airflow costuma invocar bash sem login; PATH pode vir vazio ou sem ~/.local/bin.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path

from common.config import DBT_PROFILE_NAME, DBT_PROFILES_DIR, DBT_PROJECT_DIR, DBT_TARGET

_AIRFLOW_LOCAL_BIN = "/home/airflow/.local/bin"


def dbt_executable_path() -> str:
    override = (os.environ.get("DBT_EXECUTABLE") or "").strip()
    if override:
        return override
    found = shutil.which("dbt")
    if found:
        return found
    return f"{_AIRFLOW_LOCAL_BIN}/dbt"


def dbt_subprocess_env() -> dict[str, str]:
    env = dict(os.environ)
    plugins = str(Path(DBT_PROJECT_DIR).resolve() / "plugins")
    prev_py = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{plugins}{os.pathsep}{prev_py}" if prev_py else plugins
    env["DBT_PROFILES_DIR"] = str(Path(DBT_PROFILES_DIR).resolve())
    path = env.get("PATH", "") or ""
    parts = path.split(os.pathsep) if path else []
    if _AIRFLOW_LOCAL_BIN not in parts:
        env["PATH"] = f"{_AIRFLOW_LOCAL_BIN}{os.pathsep}{path}" if path else _AIRFLOW_LOCAL_BIN
    return env


def dbt_run_command(*, select: str, extra_args: str = "") -> str:
    """cd projeto + dbt run (profiles-dir, profile, target)."""
    profiles = str(Path(DBT_PROFILES_DIR).resolve())
    exe = dbt_executable_path()
    tail = f" {extra_args}" if extra_args.strip() else ""
    return (
        f"cd {DBT_PROJECT_DIR} && {exe} run --select {select} "
        f"--profiles-dir {profiles} --profile {DBT_PROFILE_NAME} --target {DBT_TARGET}{tail}"
    )


def dbt_deps_then_run_command(*, select: str, extra_args: str = "") -> str:
    """deps (idempotente) antes do run — necessário se dbt_packages estiver vazio no volume."""
    profiles = str(Path(DBT_PROFILES_DIR).resolve())
    exe = dbt_executable_path()
    tail = f" {extra_args}" if extra_args.strip() else ""
    return (
        f"cd {DBT_PROJECT_DIR} && {exe} deps --profiles-dir {profiles} && {exe} run --select {select} "
        f"--profiles-dir {profiles} --profile {DBT_PROFILE_NAME} --target {DBT_TARGET}{tail}"
    )
