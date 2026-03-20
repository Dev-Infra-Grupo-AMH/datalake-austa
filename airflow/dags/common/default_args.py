"""
Default arguments for Airflow DAGs.
Reusable across extraction, orchestration, and delivery DAGs.
"""
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 60,
    "email_on_failure": False,
    "email_on_retry": False,
}
