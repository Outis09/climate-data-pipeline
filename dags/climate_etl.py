import pendulum
import os
from datetime import timedelta
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk.exceptions import AirflowException
from datetime import datetime, timedelta
# from utils.db import load_data, extract_cities
from utils.extract import extract_daily_climate, extract_daily_air_quality
# from utils.transform import agg_hourly_air_quality, transform_daily_climate_chunks, transform_daily_land_surface
# from utils.validate import run_validation
from utils.custom.operators import QuotaAwareOpenMeteoExtractionOperator


recipient_email = os.getenv('NOTIFICATION_EMAIL')

task_fail_notify = SmtpNotifier(
        smtp_conn_id="smtp_default",
        to=recipient_email,
        subject="Airflow Failure: {{ ti.task_id }} in {{ dag.dag_id }}",
        html_content="""
    <h3>Task Failure Alert</h3>
    <p><b>DAG:</b> {{ "".join(dag.dag_id) }}</p>
    <p><b>Task:</b> {{ ti.task_id }}</p>
    <p><b>Execution Time:</b> {{ dag_run.logical_date }}</p>
    <p><b>Error Message:</b></p>
    <pre style="color: #721c24;">
    {{ exception }}
    </pre>
    <p><a href="{{ ti.log_url }}">Click here to view full Airflow logs</a></p>
    
"""
    )



default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(hours=1), 
    "on_failure_callback": task_fail_notify
}



with DAG(
    dag_id = 'climate',
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1 , 1, tz='UTC'),
    schedule=CronDataIntervalTimetable("@daily", timezone='UTC'),
    catchup=False,):
    
    start = EmptyOperator(task_id='start')


    @task
    def get_cities() -> list[str]:
        """Return a list of paths to the chunks of cities data"""
        from utils.db import extract_cities
        chunk_paths = extract_cities()
        return chunk_paths
    

    @task(pool="nasa_power_extraction_pool", priority_weight=100)
    def fetch_daily_land_surface(parquet_chunk_path: str, data_interval_start: pendulum.DateTime) -> str:
        """Fetch daily land surface data and return path to saved extract"""
        from utils.extract import extract_daily_land_surface
        start_date = data_interval_start - timedelta(days=2)
        start_date = start_date.strftime('%Y-%m-%d')
        file_name = extract_daily_land_surface(period=[start_date, start_date], cities_chunk_paths=parquet_chunk_path)
        return file_name


    @task
    def aggregate_hourly_air_quality(parquet_paths: list[str]) -> list[str]:
        """Aggregate hourly raw air quality data into daily transformed data in Parquet files"""
        from utils.transform import agg_hourly_air_quality
        parquet_path = agg_hourly_air_quality(parquet_paths)
        return parquet_path

    
    @task
    def consolidate_daily_climate_chunks(parquet_paths: list[str]) -> list[str]:
        """Consolidate raw city-chunked climate data into transformed data in Parquet files"""
        from utils.transform import transform_daily_climate_chunks
        consolidated_loc = transform_daily_climate_chunks(raw_parquet_paths=parquet_paths)
        return consolidated_loc


    @task
    def consolidate_daily_land_surface(parquet_paths: list[str]) -> list[str]:
        """Consolidate raw city-chunked land surface data into transformed data in Parquet files"""
        from utils.transform import transform_daily_land_surface
        trnasformed_loc = transform_daily_land_surface(raw_parquet_paths=parquet_paths)
        return trnasformed_loc


    @task(pool="gx_validation_pool")
    def validate_data(parquet_paths: list[str], api_source: str)-> list[str]:
        """Run Great Expectations Checkpoint on transformed data"""
        from utils.validate import run_validation
        validated_paths = run_validation(parquet_paths=parquet_paths, api_source=api_source)
        return validated_paths

        
    @task(pool="db_upsert_pool")
    def upsert_data(parquet_paths: list[str], table_name: str, run_id: str) -> None:
        """Upsert data into Postgres or BigQuery"""
        from utils.db import load_data
        load_data(parquet_paths, table_name, run_id)
        return None
         

    cities = get_cities()

    # extraction tasks
    fetch_climate = QuotaAwareOpenMeteoExtractionOperator(
    task_id="fetch_daily_climate",
    python_callable=extract_daily_climate,
    period=["{{ ds }}"],
    parquet_paths=cities,
    pool="open_meteo_extraction_pool",
    pool_slots=1,
    priority_weight=100
    )

    fetch_air_quality = QuotaAwareOpenMeteoExtractionOperator(
        task_id="fetch_daily_air_quality",
        python_callable=extract_daily_air_quality,
        period=["{{ ds }}"],
        parquet_paths=cities,
        pool="open_meteo_extraction_pool",
        pool_slots=1,
        priority_weight=100
    )
    fetch_land_surface = fetch_daily_land_surface.expand(parquet_chunk_path=cities)


    # transformation tasks
    calc_daily_air_quality = aggregate_hourly_air_quality(parquet_paths=fetch_air_quality.output)
    consolidating_climate_chunks = consolidate_daily_climate_chunks(parquet_paths=fetch_climate.output)
    transform_land_surface = consolidate_daily_land_surface(parquet_paths=fetch_land_surface)


    # validation tasks
    validate_climate = validate_data.override(task_id="validate_climate_pre_load")(
        parquet_paths=consolidating_climate_chunks,
        api_source="climate"
    )

    validate_air_quality = validate_data.override(task_id="validate_air_quality_pre_load")(
        parquet_paths=calc_daily_air_quality,
        api_source="air_quality"
    )

    validate_land_surface = validate_data.override(task_id='validate_land_surface_pre_load')(
        parquet_paths=transform_land_surface,
        api_source='land_surface'
    )


    # db upsert tasks
    upsert_climate = upsert_data.override(task_id="upsert_climate")(
        validate_climate, table_name='daily_climate'
    )

    upsert_air_quality = upsert_data.override(task_id="upsert_air_quality")(
            validate_air_quality, table_name='daily_air_quality'
    )

    upsert_land_surface = upsert_data.override(task_id="upsert_land_surface")(
        validate_land_surface, table_name='daily_land_surface'
    )

    end = EmptyOperator(task_id='end')

    # explicit dependencies
    start >> cities
    [upsert_climate , upsert_air_quality, upsert_land_surface] >> end