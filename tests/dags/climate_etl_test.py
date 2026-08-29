import pytest
from airflow.dag_processing.dagbag import DagBag

@pytest.fixture()
def dagbag():
    return DagBag(dag_folder="/opt/airflow/dags")

def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id='climate')
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 15

def assert_dag_dict_equal(source, dag):
    assert dag.task_dict.keys() == source.keys()
    for task_id, downstream_list in source.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)


def test_dag(dagbag):
    dag = dagbag.get_dag(dag_id='climate')
    assert_dag_dict_equal(
        {
            "aggregate_hourly_air_quality": ['validate_air_quality_pre_load'],
            "consolidate_daily_climate_chunks": ['validate_climate_pre_load'],
            "consolidate_daily_land_surface": ['validate_land_surface_pre_load'],
            "end": [],
            "fetch_daily_air_quality": ['aggregate_hourly_air_quality'],
            "fetch_daily_climate": ['consolidate_daily_climate_chunks'],
            "fetch_daily_land_surface": ['consolidate_daily_land_surface'],
            "get_cities": ['fetch_daily_air_quality', 'fetch_daily_climate', 'fetch_daily_land_surface'],
            "start": ['get_cities'],
            "upsert_air_quality": ['end'],
            "upsert_climate": ['end'],
            "upsert_land_surface": ['end'],
            "validate_air_quality_pre_load": ['upsert_air_quality'],
            "validate_climate_pre_load": ['upsert_climate'],
            "validate_land_surface_pre_load": ['upsert_land_surface'],
        },
        dag,
    )