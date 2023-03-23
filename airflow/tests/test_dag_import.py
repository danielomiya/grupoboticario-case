from airflow.models import DagBag


def test_dag_import(dagbag: DagBag):
    """
    Tests the import of Airflow DAGs

    :param dagbag: a Airflow's DagBag
    """
    import_errors_len = len(dagbag.import_errors)
    assert (
        import_errors_len == 0
    ), f"there were {import_errors_len} errors importing dags"
