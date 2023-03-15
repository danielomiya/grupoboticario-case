"""
### load_sales_spreadsheets

The present DAG loads spreadsheets into Google BigQuery.
"""

import os
import typing as t
from pathlib import Path

from airflow.models import DAG
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from shared import yaml
from shared.utils import days_ago

GS_BUCKET = os.getenv("GS_BUCKET")
GS_TMP_BUCKET = os.getenv("GS_TMP_BUCKET")
CURR_DIR = Path(__file__).parent
GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "{{ conn." + GCP_CONN_ID + ".extra_dejson.project }}"

CLUSTER_NAME = "{{ ti.dag_id.replace('_', '-') }}-{{ ds_nodash }}"
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 128,
        },
    },
    "software_config": {
        "image_version": "2.0-debian10",  # Spark 3.1.3 // Scala 2.12
        "properties": {
            "dataproc:dataproc.allow.zero.workers": "true",
            "spark:spark.jars": ",".join(
                [
                    f"gs://{GS_BUCKET}/resources/jars/spark-excel_2.12-3.1.3_0.18.5.jar",
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.29.0.jar",
                ]
            ),
            "spark:spark.datasource.bigquery.temporaryGcsBucket": GS_TMP_BUCKET,
        },
    },
}

with open(CURR_DIR / "config.yaml") as f:
    spreadsheets = yaml.safe_load(f)


def create_job_template(
    spreadsheet: t.Dict[str, t.Any],
) -> t.Dict[str, t.Any]:
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GS_BUCKET}/resources/scripts/cleaning.py",
            "args": [
                "--app-name",
                "{{ ti.dag_id }}-{{ ti.task_id }}",
                "--input",
                spreadsheet["source"],
                "--format",
                spreadsheet["format"],
                "--output",
                spreadsheet["table_name"],
            ],
        },
    }


default_args = {
    "region": "us-central1",
    "gcp_conn_id": GCP_CONN_ID,
    "project_id": PROJECT_ID,
}

with DAG(
    dag_id="load_sales_spreadsheets",
    start_date=days_ago(1),
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    doc_md=__doc__,
):
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        cluster_config=CLUSTER_CONFIG,
        cluster_name=CLUSTER_NAME,
    )

    spark_tasks = [
        DataprocSubmitJobOperator(
            task_id=f"process_{spreadsheet['table_name']}",
            job=create_job_template(spreadsheet),
            deferrable=True,
        )
        for spreadsheet in spreadsheets
    ]

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(create_cluster, spark_tasks, delete_cluster)
