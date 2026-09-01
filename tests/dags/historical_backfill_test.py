import pytest
from airflow.dag_processing.dagbag import DagBag

@pytest.fixture()
def dagbag():
    return DagBag(dag_folder="/opt/airflow/dags")

def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id='historical_backfill')
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 18

def assert_dag_dict_equal(source, dag):
    assert dag.task_dict.keys() == source.keys()
    for task_id, downstream_list in source.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)


def test_dag(dagbag):
    dag = dagbag.get_dag(dag_id='historical_backfill')
    assert_dag_dict_equal(
        {
            "air_quality_pipeline.backfill_air_quality": ['air_quality_pipeline.consolidate_daily_air_quality'],
            "air_quality_pipeline.consolidate_daily_air_quality": ['air_quality_pipeline.validate_pre_load'],
            "air_quality_pipeline.emit_air_quality_year_processed": [],
            "air_quality_pipeline.upsert_air_quality": ['air_quality_pipeline.emit_air_quality_year_processed'],
            "air_quality_pipeline.validate_pre_load": ['air_quality_pipeline.upsert_air_quality'],
            "build_city_period_pairs": ['air_quality_pipeline.backfill_air_quality', 'climate_period_pipeline.backfill_climate'],
            "climate_period_pipeline.backfill_climate": ['climate_period_pipeline.consolidate_daily_climate_chunks'],
            "climate_period_pipeline.consolidate_daily_climate_chunks": ['climate_period_pipeline.validate_climate_pre_load'],
            "climate_period_pipeline.emit_climate_year_processed": [],
            "climate_period_pipeline.upsert_climate": ['climate_period_pipeline.emit_climate_year_processed'],
            "climate_period_pipeline.validate_climate_pre_load": ['climate_period_pipeline.upsert_climate'],
            "get_cities": ['build_city_period_pairs', 'land_surface_period_pipeline.backfill_period_land_surface'],
            "get_periods": ['build_city_period_pairs', 'land_surface_period_pipeline.backfill_period_land_surface'],
            "land_surface_period_pipeline.backfill_period_land_surface": ['land_surface_period_pipeline.consolidate_daily_land_surface'],
            "land_surface_period_pipeline.consolidate_daily_land_surface": ['land_surface_period_pipeline.validate_land_surface_pre_load'],
            "land_surface_period_pipeline.emit_land_surface_year_processed": [],
            "land_surface_period_pipeline.upsert_land_surface": ['land_surface_period_pipeline.emit_land_surface_year_processed'],
            "land_surface_period_pipeline.validate_land_surface_pre_load": ['land_surface_period_pipeline.upsert_land_surface'],
        },
        dag,
    )

