from airflow.sdk import DAG, task
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from include.extract import extract_daily_land_surface, extract_daily_air_quality, extract_daily_climate
from include.db import extract_cities, load_data
from include.transform import transform_daily_climate_chunks, transform_daily_land_surface, agg_hourly_air_quality

with DAG(
    dag_id='backfill_climate',
    start_date=pendulum.datetime(2001, 1, 1, tz='UTC'),
    end_date=pendulum.datetime(2025, 12, 31, tz='UTC'),
    schedule=CronDataIntervalTimetable('@yearly', timezone='UTC'),
    catchup=False
):

    start = EmptyOperator(task_id='start')


    @task
    def get_cities() -> list[str]:
        chunk_paths = extract_cities()
        return chunk_paths
    

    @task
    def backfill_land_surface(period,parquet_paths, **context):
        save_path = extract_daily_land_surface(period, parquet_paths, **context)

    @task
    def backfill_climate(period, parquet_paths, **context):
        save_path = extract_daily_climate(period, parquet_paths, **context)

    @task
    def backfill_air_quality(period, parquet_paths, **context):
        save_path = extract_daily_air_quality(period, parquet_paths, **context)

        @task
    def aggregate_hourly_air_quality(period, parquet_paths,**context):
        parquet_path = agg_hourly_air_quality(period, parquet_paths, **context)
        return parquet_path
    
    @task
    def consolidate_daily_climate_chunks(period, parquet_paths, **context):
        consolidated_loc = transform_daily_climate_chunks(period, parquet_paths, **context)
        return consolidated_loc


    @task
    def consolidate_daily_land_surface(period, parquet_paths, **context):
        trnasformed_loc = transform_daily_land_surface(period, parquet_paths, **context)
        return trnasformed_loc


    end = EmptyOperator(task_id='end')

    cities = get_cities()

    backfill_climate_yearly = backfill_climate.partial(period='yearly').expand(parquet_paths=cities)
    backfill_land_surface_yearly =backfill_land_surface.partial(period='yearly').expand(parquet_paths=cities)
    backfill_air_quality_yearly = backfill_air_quality.partial(period='yearly').expand(parquet_paths=cities)

    transform_climate = consolidate_daily_climate_chunks(period='daily', parquet_paths=backfill_climate_yearly)
    transform_land_surface = consolidate_daily_land_surface(period='yearly', parquet_paths=backfill_land_surface_yearly)
    transform_air_quality = backfill_air_quality_yearly



    start >> cities
    [backfill_air_quality_yearly, backfill_land_surface_yearly, backfill_climate_yearly] >> end


    

    
                

