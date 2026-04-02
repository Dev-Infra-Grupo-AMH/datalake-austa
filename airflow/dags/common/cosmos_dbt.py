"""
Configuração compartilhada para DAGs Astronomer Cosmos + dbt-spark (lakehouse Tasy).

LoadMode.DBT_LS: descoberta de nós via `dbt ls` no parse da DAG (evoluir para DBT_MANIFEST no deploy).
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode
from cosmos.operators.local import DbtTaskGroup

from common.config import DBT_PROJECT_DIR, DBT_PROFILES_DIR


def get_project_config() -> ProjectConfig:
    return ProjectConfig(dbt_project_path=str(Path(DBT_PROJECT_DIR).resolve()))


def get_profile_config() -> ProfileConfig:
    target = os.environ.get("DBT_TARGET", "dev")
    profiles_yml = Path(DBT_PROFILES_DIR).resolve() / "profiles.yml"
    return ProfileConfig(
        profile_name="lakehouse_tasy",
        target_name=target,
        profiles_yml_filepath=str(profiles_yml),
    )


def render_config_for_select(select: list[str]) -> RenderConfig:
    return RenderConfig(
        load_method=LoadMode.DBT_LS,
        select=select,
    )


def dbt_operator_args() -> Dict[str, Any]:
    """Env com PYTHONPATH para `dbt/plugins` (sitecustomize / perfil), pool Kyuubi."""
    merged = os.environ.copy()
    plugins = str(Path(DBT_PROJECT_DIR).resolve() / "plugins")
    prev = merged.get("PYTHONPATH", "")
    merged["PYTHONPATH"] = f"{plugins}{os.pathsep}{prev}" if prev else plugins
    return {
        "install_deps": False,
        "pool": "spark_dbt",
        "env": merged,
    }


def layer_dbt_task_group(group_id: str, select: list[str]) -> DbtTaskGroup:
    """Um DbtTaskGroup com seleção dbt (modelo, path:, tag:, etc.)."""
    return DbtTaskGroup(
        group_id=group_id,
        project_config=get_project_config(),
        profile_config=get_profile_config(),
        render_config=render_config_for_select(select),
        operator_args=dbt_operator_args(),
    )
