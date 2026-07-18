import os

from airflow.models import DagBag

DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dags"
)


def test_dag_bag_has_no_import_errors():
    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    assert not dag_bag.import_errors, (
        f"DAG import errors found: {dag_bag.import_errors}"
    )


def test_retail_pipeline_dag_loads_with_expected_tasks():
    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    dag = dag_bag.dags["retail_analytics_pipeline"]

    task_ids = set(dag.task_ids)
    assert task_ids == {"validate_schema", "api_ingest", "normalize_api", "load_postgres"}


def test_load_postgres_depends_on_validate_schema_and_normalize_api():
    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    dag = dag_bag.dags["retail_analytics_pipeline"]

    load_task = dag.get_task("load_postgres")
    upstream_ids = {t.task_id for t in load_task.upstream_list}
    assert upstream_ids == {"validate_schema", "normalize_api"}
