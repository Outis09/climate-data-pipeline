from airflow.sdk import DAG, task
import pendulum
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
import os
import pandas as pd
from datetime import datetime
from collections import defaultdict
from airflow.sdk.exceptions import AirflowSkipException


recipient_email = os.getenv('NOTIFICATION_EMAIL')


# email template to send when task fails
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

# email template to send when dag succeeds
dag_success_notify = SmtpNotifier(
    smtp_conn_id="smtp_default",
    to=recipient_email,
    subject="Airflow Success | Late Arriving Radiation Data DAG Completed Successfully ",
    html_content="""
<h3 style="color: #155724;">DAG Completed Successfully</h3>

<p><b>DAG:</b> {{ dag.dag_id }}</p>
<p><b>Run ID:</b> {{ dag_run.run_id }}</p>
<p><b>Execution Time:</b> {{ dag_run.logical_date }}</p>
<p><b>Status:</b>
    <span style="color: #155724; font-weight: bold;">
        SUCCESS
    </span>
</p>

<div style="color: #155724;">
    All tasks in this DAG completed successfully.
</div>

<p>
    <a href="{{ ti.log_url }}">Click here to view the Airflow logs</a>
</p>
"""
)

# default arguments for tasks
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(hours=1), 
    "on_failure_callback": task_fail_notify
}

with DAG(
    dag_id="late_arriving_radiation_data",
    schedule="@weekly",
    catchup=False
):
    @task
    def get_next_radiation_period():
        """Get the next radiation period to be extracted from the API"""
        from utils.db import fetch_next_radiation_period
        start_date, end_date = fetch_next_radiation_period()
        if not start_date and not end_date:
            raise AirflowSkipException("Last radiation date not available")
        return [start_date, end_date]

    @task
    def get_cities():
        """Get path to cities chunks"""
        from utils.db import extract_cities
        chunk_paths = extract_cities()
        return chunk_paths

    @task(pool="nasa_power_extraction_pool")
    def get_late_arriving_data(period: list[str], cities_chunk_paths: list[str]) -> list[str]:
        """Extract land surface data for given city chunk data and period"""
        from utils.extract import extract_daily_land_surface
        start_date = period[0]
        end_date = period[1]

        parquet_chunk_paths = []
        for cities_chunk_path in cities_chunk_paths:
            parquet_chunk_path = extract_daily_land_surface(period=[start_date, end_date], cities_chunk_paths=cities_chunk_path, request='radiation')
            parquet_chunk_paths.extend(parquet_chunk_path)
        return parquet_chunk_paths


    @task
    def consolidate_daily_land_surface(parquet_paths: list[str]) -> list[str]:
        """Consolidate raw land surface data into transformed data in Parquet files"""
        from utils.transform import transform_daily_land_surface
        transformed_loc = transform_daily_land_surface(parquet_paths)
        return transformed_loc

    @task(pool="gx_validation_pool")
    def validate_data(parquet_paths: list[str], api_source: str) -> list[str]:
        """Validate transformed data using Great Expectations Suites and Checkpoints"""
        from utils.validate import run_validation
        validated_paths = run_validation(parquet_paths=parquet_paths, api_source=api_source)

        return validated_paths

    @task(pool="db_upsert_pool")
    def upsert_data(parquet_paths: tuple[list[str], defaultdict], table_name: str, run_id: str) -> str:
        """Upsert data into Postgres or BigQuery"""
        from utils.db import load_data
        processed_date = load_data(parquet_paths, table_name, run_id)
        return processed_date


    next_radiation_period = get_next_radiation_period()
    cities = get_cities()
    late_data = get_late_arriving_data(period=next_radiation_period,cities_chunk_paths=cities)
    transform = consolidate_daily_land_surface(parquet_paths=late_data)
    validate = validate_data(parquet_paths=transform, api_source='land_surface')
    upsert = upsert_data(parquet_paths=validate, table_name='daily_land_surface')
