from pathlib import Path

import pytest
from airflow.models import DagBag

CURR_DIR = Path(__file__).parent


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    """
    Fixture that returns a global instance of the `DagBag` class

    :return: a initialized DagBag
    """
    dags_folder = CURR_DIR / ".." / "dags"
    return DagBag(dag_folder=dags_folder.resolve(), include_examples=False)
