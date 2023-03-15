"""
### generate_analytical_tables

The present DAG materializes the analytical tables given their dependencies,
and the SQL scripts to produce them.
"""

import typing as t
from pathlib import Path

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensor,
)
from airflow.utils.trigger_rule import TriggerRule
from shared import yaml
from shared.utils import days_ago

CURR_DIR = Path(__file__).parent
TEMPLATES_DIR = Path("sql")

tables: t.List[t.Dict[str, t.Any]] = yaml.safe_load(CURR_DIR / "config.yaml")

GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "{{ conn." + GCP_CONN_ID + ".extra_dejson.project }}"

default_args = {
    "gcp_conn_id": GCP_CONN_ID,
    "project_id": PROJECT_ID,
}

with DAG(
    "generate_analytical_tables",
    start_date=days_ago(1),
    schedule="@once",
    default_args=default_args,
    doc_md=__doc__,
):
    dependency_sensors = {}

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    for table in tables:
        bq_query = BigQueryInsertJobOperator(
            task_id=f"create-{table['table_name']}-table",
            configuration={
                "query": {
                    "query": str(TEMPLATES_DIR / table["script"]),
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": "analytics",
                        "tableId": table["table_name"],
                    },
                    "useLegacySql": False,
                },
            },
            deferrable=True,
        )

        if not table["depends_on"]:
            start >> bq_query >> end
            continue

        for dep in table["depends_on"]:
            if dep not in dependency_sensors:
                dataset_id, table_id = dep.split(".")
                dependency_sensors[dep] = BigQueryTableExistenceSensor(
                    task_id=f"check-{dep}-sensor",
                    dataset_id=dataset_id,
                    table_id=table_id,
                )

            start >> dependency_sensors[dep] >> bq_query >> end
