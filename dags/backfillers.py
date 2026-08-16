from airflow.sdk import DAG, task
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from include.extract import extract_daily_land_surface, extract_daily_air_quality, extract_daily_climate
from include.db import extract_cities, load_data
from include.transform import transform_daily_climate_chunks, transform_daily_land_surface, agg_hourly_air_quality
from include.custom.operators import QuotaAwareOpenMeteoExtractionOperator

with DAG(
    dag_id='backfill_climate',
    start_date=pendulum.datetime(2001, 1, 1, tz='UTC'),
    end_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    schedule=CronDataIntervalTimetable('@yearly', timezone='UTC'),
    catchup=False,
    max_active_runs=1
):

    start = EmptyOperator(task_id='start')


    @task
    def get_cities() -> list[str]:
        chunk_paths = extract_cities()
        return chunk_paths
    

    @task
    def backfill_land_surface(period,parquet_paths, **context):
        save_path = extract_daily_land_surface(period, parquet_paths, **context)
        return save_path

    # @task
    # def backfill_climate(period, parquet_paths, **context):
    #     save_path = extract_daily_climate(period, parquet_paths, **context)
    #     return save_path



    
    @task
    def consolidate_daily_climate_chunks(period, parquet_paths, **context):
        consolidated_loc = transform_daily_climate_chunks(period, parquet_paths, **context)
        return consolidated_loc


    @task
    def consolidate_daily_land_surface(period, parquet_paths, **context):
        trnasformed_loc = transform_daily_land_surface(period, parquet_paths, **context)
        return trnasformed_loc

    @task(pool="db_upsert_pool")
    def upsert_data(parquet_paths, table_name):
        load_data(parquet_paths, table_name)
        return None

    end = EmptyOperator(task_id='end')

    cities = get_cities()

    backfill_climate_yearly = QuotaAwareOpenMeteoExtractionOperator.partial(
    task_id="backfill_climate",
    python_callable=extract_daily_climate,
    period = 'yearly',
    retries=3
    ).expand(parquet_paths=cities)

    # backfill_climate_yearly = backfill_climate.partial(period='yearly').expand(parquet_paths=cities)
    backfill_land_surface_yearly =backfill_land_surface.partial(period='yearly').expand(parquet_paths=cities)

    transform_climate = consolidate_daily_climate_chunks(period='daily', parquet_paths=backfill_climate_yearly.output)
    transform_land_surface = consolidate_daily_land_surface(period='yearly', parquet_paths=backfill_land_surface_yearly)


    upsert_climate = upsert_data.override(task_id="upsert_climate")(
        transform_climate, table_name='daily_climate'
    )


    upsert_land_surface = upsert_data.override(task_id="upsert_land_surface")(
        transform_land_surface, table_name='daily_land_surface'
    )

    start >> cities >> [backfill_climate_yearly, backfill_land_surface_yearly]
    backfill_land_surface_yearly >> transform_land_surface
    backfill_climate_yearly >> transform_climate
    transform_climate >> upsert_climate
    transform_land_surface >> upsert_land_surface


    

    
                

