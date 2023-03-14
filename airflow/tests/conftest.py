from pathlib import Path

import pytest
from airflow.models import DagBag

CURR_DIR = Path(__file__).parent


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    dags_folder = CURR_DIR / ".." / "dags"
    return DagBag(dag_folder=dags_folder.resolve(), include_examples=False)
