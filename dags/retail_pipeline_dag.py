"""
Retail Analytics Data Pipeline DAG.
Two parallel ingestion/validation branches converge into a single load:
  - validate_schema: CSV historical bulk load -> PySpark schema validation
  - api_ingest -> normalize_api: DummyJSON daily incremental -> schema-aligned normalization
Both branches feed load_postgres, which merges them into the Postgres star schema.
All four scripts run as subprocesses inside the Airflow worker container
(transform/ and ingestion/ are mounted from the host repo), so each PySpark
job gets its own isolated JVM gateway.
"""

from datetime import datetime, timedelta
import os
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email


def run_script(script_path: str):
    """Run a pipeline script as a subprocess so PySpark's JVM gateway
    stays isolated per task, and surface failures with full output."""
    result = subprocess.run(
        ["python", script_path],
        cwd="/opt/airflow",
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"{script_path} failed with exit code {result.returncode}")


def alert_on_failure(context):
    task_instance = context["task_instance"]
    subject = f"[Airflow] Retail pipeline failed: {task_instance.task_id}"
    body = (
        f"Task: {task_instance.task_id}\n"
        f"DAG: {task_instance.dag_id}\n"
        f"Execution date: {context['execution_date']}\n"
        f"Log URL: {task_instance.log_url}\n"
    )
    try:
        alert_to = os.environ.get("ALERT_EMAIL_TO")
        if not alert_to:
            print("ALERT_EMAIL_TO not set — skipping email alert")
            return
        send_email(to=[alert_to], subject=subject, html_content=body)
    except Exception as e:
        # SMTP not configured yet — don't let alerting failure mask the real pipeline failure
        print(f"Alert email failed to send: {e}")


default_args = {
    "owner": "arnav",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="retail_analytics_pipeline",
    default_args=default_args,
    description="CSV -> PySpark schema validation -> Postgres star schema",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["retail", "pyspark", "postgres"],
) as dag:

    validate_schema = PythonOperator(
        task_id="validate_schema",
        python_callable=run_script,
        op_kwargs={"script_path": "transform/schema_validation.py"},
    )

    api_ingest = PythonOperator(
        task_id="api_ingest",
        python_callable=run_script,
        op_kwargs={"script_path": "ingestion/api_ingest.py"},
    )

    normalize_api = PythonOperator(
        task_id="normalize_api",
        python_callable=run_script,
        op_kwargs={"script_path": "transform/normalize_api_source.py"},
    )

    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=run_script,
        op_kwargs={"script_path": "transform/load_postgres.py"},
    )

    api_ingest >> normalize_api
    [validate_schema, normalize_api] >> load_postgres
