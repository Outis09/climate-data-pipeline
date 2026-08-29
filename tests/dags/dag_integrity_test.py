import os
import glob
import pytest
from airflow.dag_processing.dagbag import DagBag

DAG_PATH = "/opt/airflow/dags/*.py"
DAG_FILES = glob.glob(DAG_PATH)

@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_integrity(dag_file, caplog):
    """Test integrity of DAGs"""
    DagBag(dag_folder=dag_file)
    for record in caplog.records:
        if record.levelname == "ERROR":
            raise record.exc_info[1]
        elif "assumed to contain no DAGs" in record.message:
            assert False, f"No DAGs found in {dag_file}"