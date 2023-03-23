"""
### extract_spotify_data

The present DAG extracts Spotify searches, transforms them and loads them into
Google BigQuery.
"""

import os
import typing as t
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.trigger_rule import TriggerRule
from shared import yaml
from shared.spotify_client import SpotifyClient
from shared.utils import days_ago

if t.TYPE_CHECKING:
    from airflow.decorators.base import TaskDecorator
    from pandas import DataFrame


def create_task_id_suffixer(
    suffix: str,
) -> t.Callable[["TaskDecorator"], "TaskDecorator"]:
    """
    Create a function that returns another which adds a suffix to the task_id

    :param suffix: a string that will be added as a suffix to the task_id
    :return: a higher-order function
    """

    def func(task: "TaskDecorator") -> "TaskDecorator":
        """
        A decorator function that adds a suffix to the given task's id

        :param task: an instance of TaskDecorator to be modified
        :return: the given task with the suffix appended to its id
        """
        name = task.function.__name__ + suffix
        return task.override(task_id=name)

    return func


@task(multiple_outputs=False)
def extract_query(query: str, type: str) -> "DataFrame":
    """
    Extracts data based on a given query from the Spotify API

    :param query: the search term to query for
    :param type: the type of item to search for
    :return: a DataFrame containing the search results
    """
    import pandas as pd

    dfs: t.List[pd.DataFrame] = []
    offset = 0
    per_page = 50

    with SpotifyClient(
        os.getenv("SPOTIFY_CLIENT_ID"),
        os.getenv("SPOTIFY_CLIENT_SECRET"),
    ) as client:
        while True:
            items = client.search(
                query, type=type, limit=per_page, offset=offset
            )

            if not items:
                break

            dfs.append(pd.json_normalize(items, sep="__"))
            offset += per_page

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


@task(multiple_outputs=False)
def transform_data(df: "DataFrame") -> "DataFrame":
    """
    Applies tranformations to a DataFrame before saving it

    :param df: a DataFrame to be transformed
    :return: a treated DataFrame
    """
    complex_columns = [
        col
        for col in df.columns
        if df[col].map(lambda val: isinstance(val, list)).any()
    ]
    return df.drop(columns=complex_columns)


@task(multiple_outputs=False)
def load_into_gbq(
    df: "DataFrame",
    table_name: str,
    gcp_conn_id: str = "google_cloud_default",
) -> None:
    """
    Loads the contents of the DataFrame into a specified BigQuery dataset

    :param df: a DataFrame to be loaded
    :param table_name: the name of the BigQuery table to load the data into
    :param gcp_conn_id: the connection to use when connecting to Google Cloud,
                        defaults to "google_cloud_default"
    """
    hook = GoogleBaseHook(gcp_conn_id)
    credentials, project_id = hook.get_credentials_and_project_id()

    df.to_gbq(
        f"staging.{table_name}",
        if_exists="replace",
        project_id=project_id,
        credentials=credentials,
    )


default_args = {
    "gcp_conn_id": "google_cloud_default",
}


@dag(
    start_date=days_ago(1),
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    doc_md=__doc__,
)
def extract_spotify_data():
    """
    Extract Spotify data, transform it and load it into Google BigQuery
    """
    queries: t.List[t.Dict[str, t.Any]] = yaml.safe_load(
        Path(__file__).parent / "queries.yaml"
    )

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    for q in queries:
        suffix = f"-{q['query']}-{q['type']}".replace(" ", "_")
        with_suffix = create_task_id_suffixer(suffix)

        raw_df = with_suffix(extract_query)(q["query"], q["type"])
        treated_df = with_suffix(transform_data)(raw_df)
        result = with_suffix(load_into_gbq)(treated_df, q["table_name"])

        start >> raw_df
        result >> end


extract_spotify_data()
