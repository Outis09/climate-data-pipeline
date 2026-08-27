from airflow.sdk import DAG, task, task_group
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from utils.extract import extract_daily_land_surface, extract_daily_air_quality, extract_daily_climate
from utils.db import extract_cities, load_data
from utils.transform import transform_daily_climate_chunks, transform_daily_land_surface, agg_hourly_air_quality
from utils.validate import run_validation
from utils.custom.operators import QuotaAwareOpenMeteoExtractionOperator


          
with DAG(
    dag_id='historical_backfill',
    start_date=None,
    schedule=None,
    catchup=False
):
    @task
    def get_periods():
        import os
        from datetime import datetime, timedelta
        start_date = datetime.strptime(os.getenv('START_DATE'), '%Y-%m-%d')
        end_date = datetime.now() - timedelta(days=2)

        periods = []

        years = list(range(start_date.year, end_date.year + 1))

        for year in years:
            year_start = max(start_date, datetime(year,1,1))
            year_end = min(end_date, datetime(year, 12, 31))
            periods.append([year_start.strftime('%Y-%m-%d'), year_end.strftime('%Y-%m-%d')])
        return periods
    @task
    def get_cities() -> list[str]:
        chunk_paths = extract_cities()
        return chunk_paths

        

    @task
    def backfill_period_land_surface(period, cities_chunk_paths, **context):
        start_date = period[0]
        end_date = period[1]

        parquet_chunk_paths = []
        for cities_chunk_path in cities_chunk_paths:
            parquet_chunk_path = extract_daily_land_surface(period=[start_date, end_date], cities_chunk_paths=cities_chunk_path)
            parquet_chunk_paths.append(parquet_chunk_path)
        return parquet_chunk_paths

    @task
    def consolidate_daily_land_surface(parquet_paths):
        transformed_loc = transform_daily_land_surface(parquet_paths)
        return transformed_loc

    @task
    def consolidate_daily_climate_chunks(parquet_paths):
        consolidated_loc = transform_daily_climate_chunks(parquet_paths)
        return consolidated_loc

    @task
    def validate_data(parquet_path, api_source, **context):
        validated_paths = []
        for path in parquet_path:
            validated_path = run_validation(parquet_path=path, api_source=api_source)
            validated_paths.append(validated_path)

        return validated_paths

    @task(pool="db_upsert_pool")
    def upsert_data(parquet_paths, table_name, **context):
        load_data(parquet_paths, table_name, **context)
        return None



    @task
    def build_city_period_pairs(periods, cities):
        pairs = []
        for period in periods:
            pairs.append({"period": period, "parquet_paths":cities})
        return pairs #[{"period": period, "parquet_paths":cities} for period in periods]

    
    @task_group(group_id="climate_period_pipeline")
    def climate_period_pipeline(period, parquet_paths):
        extract = QuotaAwareOpenMeteoExtractionOperator(
            task_id="backfill_climate",
            python_callable=extract_daily_climate,
            period=period,
            parquet_paths=parquet_paths,
            retries=2,
            pool="open_meteo_extraction_pool",
            pool_slots=1,
            priority_weight=10
            )

        transform_climate = consolidate_daily_climate_chunks(parquet_paths=extract.output)

        validate_climate = validate_data.override(task_id="validate_climate_pre_load")(api_source='climate', parquet_path=transform_climate)

        upsert_climate = upsert_data.override(task_id="upsert_climate")(table_name='daily_climate', parquet_paths=validate_climate)

    @task_group(group_id="land_surface_period_pipeline")
    def land_surface_pipeline(period, cities_chunk_paths):
        backfill_land_surface_period = backfill_period_land_surface(cities_chunk_paths=cities_chunk_paths, period=period)

        transform_land_surface = consolidate_daily_land_surface(parquet_paths=backfill_land_surface_period)

        validate_land_surface = validate_data.override(task_id="validate_land_surface_pre_load")(api_source="land_surface", parquet_path=transform_land_surface)

        upsert_land_surface = upsert_data.override(task_id="upsert_land_surface")(table_name='daily_land_surface', parquet_paths=validate_land_surface)

    cities = get_cities()
    periods = get_periods()
    pair_list = build_city_period_pairs(periods=periods, cities=cities) #partial(cities=cities).expand(periods=periods)

    climate_backfill = climate_period_pipeline.expand_kwargs(pair_list)
    land_surface_backfill = land_surface_pipeline.partial(cities_chunk_paths=cities).expand(period=periods)



    

    
    

    


    


    

