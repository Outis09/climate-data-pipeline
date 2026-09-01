from airflow.sdk import DAG, task, task_group
from datetime import datetime
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.sdk.observability import stats
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
    def get_periods(**context):
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

        num_years = len(years)
        stats.gauge("pipeline.backfill.years_requested", value=num_years)
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
    def consolidate_daily_air_quality(parquet_paths):
        consolidated_loc = agg_hourly_air_quality(raw_parquet_paths=parquet_paths)
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
        processed_date = load_data(parquet_paths, table_name, **context)
        return processed_date

    @task
    def emit_year_processed_metric(processed_date, metric_name):
        try:
            processed_date = datetime.strptime(processed_date, '%Y-%m-%d')
        except:
            processed_date = processed_date
        stats.gauge(stat=metric_name, value=1, tags={"year": str(processed_date.year)})



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

        emit_climate_year_processed_metric = emit_year_processed_metric.override(task_id="emit_climate_year_processed")(processed_date=upsert_climate, metric_name="pipeline.backfill.climate.years_processed")

    @task_group(group_id="air_quality_pipeline")
    def air_quality_period_pipeline(period, parquet_paths):
        extract = QuotaAwareOpenMeteoExtractionOperator(
            task_id="backfill_air_quality",
            python_callable=extract_daily_air_quality,
            period=period,
            parquet_paths=parquet_paths,
            retries=2,
            pool="open_meteo_extraction_pool",
            pool_slots=1,
            priority_weight=10
        )

        transform_air_quality = consolidate_daily_air_quality(parquet_paths=extract.output)

        validate_air_quality = validate_data.override(task_id="validate_pre_load")(api_source='air_quality', parquet_path=transform_air_quality)

        upsert_air_quality = upsert_data.override(task_id="upsert_air_quality")(table_name="daily_air_quality", parquet_paths=validate_air_quality)

        emit_air_quality_year_processed_metric = emit_year_processed_metric.override(task_id="emit_air_quality_year_processed")(processed_date=upsert_air_quality, metric_name="pipeline.backfill_air_quality.years_processed")

    @task_group(group_id="land_surface_period_pipeline")
    def land_surface_pipeline(period, cities_chunk_paths):
        backfill_land_surface_period = backfill_period_land_surface(cities_chunk_paths=cities_chunk_paths, period=period)

        transform_land_surface = consolidate_daily_land_surface(parquet_paths=backfill_land_surface_period)

        validate_land_surface = validate_data.override(task_id="validate_land_surface_pre_load")(api_source="land_surface", parquet_path=transform_land_surface)

        upsert_land_surface = upsert_data.override(task_id="upsert_land_surface")(table_name='daily_land_surface', parquet_paths=validate_land_surface)

        emit_climate_year_processed_metric = emit_year_processed_metric.override(task_id="emit_land_surface_year_processed")(processed_date=upsert_land_surface, metric_name="pipeline.backfill.land_surface.years_processed")


    cities = get_cities()
    periods = get_periods()
    pair_list = build_city_period_pairs(periods=periods, cities=cities) #partial(cities=cities).expand(periods=periods)

    climate_backfill = climate_period_pipeline.expand_kwargs(pair_list)
    air_quality_backfill = air_quality_period_pipeline.expand_kwargs(pair_list)
    land_surface_backfill = land_surface_pipeline.partial(cities_chunk_paths=cities).expand(period=periods)



    

    
    

    


    


    

