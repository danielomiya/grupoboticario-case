"""
### extract_spotify_podcasts

The present DAG extracts a Spotify search, transforms it and loads it into
Google BigQuery.
"""

import os
import typing as t

from airflow.decorators import dag, task
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from shared.spotify_client import SpotifyClient
from shared.utils import days_ago

if t.TYPE_CHECKING:
    from pandas import DataFrame


@task(multiple_outputs=False)
def extract_podcasts() -> "DataFrame":
    import pandas as pd

    client = SpotifyClient(
        os.getenv("SPOTIFY_CLIENT_ID"),
        os.getenv("SPOTIFY_CLIENT_SECRET"),
    )
    items = client.search("data hackers", type="episode")
    return pd.json_normalize(items, sep="__")


@task(multiple_outputs=False)
def transform_data(df: "DataFrame") -> "DataFrame":
    return df.drop(["images", "languages"], axis=1)


@task(multiple_outputs=False)
def load_into_gbq(
    df: "DataFrame",
    gcp_conn_id: str = "google_cloud_default",
) -> None:
    hook = GoogleBaseHook(gcp_conn_id)
    credentials, project_id = hook.get_credentials_and_project_id()

    df.to_gbq(
        "staging.data_hackers_podcasts",
        if_exists="replace",
        project_id=project_id,
        credentials=credentials,
    )


default_args = {
    "owner": "airflow",
    "gcp_conn_id": "google_cloud_default",
}


@dag(
    start_date=days_ago(1),
    schedule="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
)
def extract_spotify_podcasts():
    raw_df = extract_podcasts()
    final_df = transform_data(raw_df)
    load_into_gbq(final_df)


extract_spotify_podcasts()
