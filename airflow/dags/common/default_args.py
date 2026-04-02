"""
Default arguments for Airflow DAGs.
Reusable across extraction, orchestration, and delivery DAGs.
"""
import os

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 60,
    "email_on_failure": False,
    "email_on_retry": False,
}

OBSERVABILITY_DEFAULT_ARGS = {
    **DEFAULT_ARGS,
    "email_on_failure": True,
    "email": [os.environ.get("AIRFLOW_ALERT_EMAIL", "rsantos@rhemadata.com")],
}
