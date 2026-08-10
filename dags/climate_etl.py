import pendulum
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.exceptions import AirflowException
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult
from datetime import datetime, timedelta
from include.db import load_data, extract_cities
from include.extract import extract_daily_climate, extract_daily_air_quality, extract_daily_land_surface
from include.transform import agg_hourly_air_quality, transform_daily_climate_chunks, transform_daily_land_surface



task_fail_notify = SmtpNotifier(
        to="sshakurace@gmail.com",
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

dag_success_notify = SmtpNotifier(
    to="sshakurace@gmail.com",
    subject="Airflow Success | {{ dag.dag_id }} | {{ dag_run.run_id }} ",
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

default_args = {
    "owner": "airflow",
    "retries": 0,
    "on_failure_callback": task_fail_notify
}



with DAG(
    dag_id = 'climate',
    on_success_callback=dag_success_notify,
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1 , 1, tz='UTC'),
    schedule=CronDataIntervalTimetable("@daily", timezone='UTC'),
    catchup=False
):
    
    start = EmptyOperator(task_id='start')


    @task
    def get_cities() -> list[str]:
        chunk_paths = extract_cities()
        return chunk_paths


    @task
    def fetch_daily_climate(parquet_chunk_path, **context):      
        file_name = extract_daily_climate(parquet_chunk_path, **context)
        return file_name


    @task
    def fetch_daily_air_quality(parquet_chunk_path, **context):
        file_name = extract_daily_air_quality(parquet_chunk_path, **context)
        return file_name
    

    @task
    def fetch_daily_land_surface(parquet_chunk_path, **context):
        file_name = extract_daily_land_surface(parquet_chunk_path, **context)
        return file_name

    @task
    def aggregate_hourly_air_quality(parquet_paths,**context):
        parquet_path = agg_hourly_air_quality(parquet_paths, **context)
        return parquet_path
    
    @task
    def consolidate_daily_climate_chunks(parquet_paths, **context):
        consolidated_loc = transform_daily_climate_chunks(parquet_paths, **context)
        return consolidated_loc


    @task
    def consolidate_daily_land_surface(parquet_paths, **context):
        trnasformed_loc = transform_daily_land_surface(parquet_paths, **context)
        return trnasformed_loc

    @task
    def validate_data(parquet_path, api_source, **context):
        import great_expectations as gx

        gx_root = '/opt/airflow/include/gx'
        gx_context = gx.get_context(project_root_dir=gx_root)

        if api_source == 'land_surface':
            run_date = datetime.strptime(context['ds'], "%Y-%m-%d").date() - timedelta(days=1)
        else:
            run_date = datetime.strptime(context['ds'], "%Y-%m-%d")

        year = str(run_date.year)
        month = str(run_date.strftime("%m"))
        day = str(run_date.strftime("%d"))

        checkpoint = gx_context.checkpoints.get(f"daily_{api_source}_checkpoint") 
        daily_batch_parameters = {"year":year,
                                  "month":month,
                                  "day":day}
        result = checkpoint.run(batch_parameters=daily_batch_parameters)
        validation_result_id = list(result.run_results.keys())[0]
        validation_result = result.run_results[validation_result_id]

        if validation_result.get_max_severity_failure() == "CRITICAL": 
        # if not result.success:
            raise AirflowException(
                f"{api_source} data failed GX validation for {context['ds']}"
            )

        return parquet_path

        
    @task
    def upsert_data(parquet_paths, table_name):
        load_data(parquet_paths, table_name)
        return None
         


    cities = get_cities()
    fetch_climate = fetch_daily_climate.expand(parquet_chunk_path=cities)
    fetch_air_quality = fetch_daily_air_quality.expand(parquet_chunk_path=cities)
    fetch_land_surface = fetch_daily_land_surface.expand(parquet_chunk_path=cities)

    calc_daily_air_quality = aggregate_hourly_air_quality(fetch_air_quality)
    consolidating_climate_chunks = consolidate_daily_climate_chunks(fetch_climate)
    transform_land_surface = consolidate_daily_land_surface(fetch_land_surface)


    validate_climate = validate_data.override(task_id="validate_climate_pre_load")(
        parquet_path=consolidating_climate_chunks,
        api_source="climate"
    )

    validate_air_quality = validate_data.override(task_id="validate_air_quality_pre_load")(
        parquet_path=calc_daily_air_quality,
        api_source="air_quality"
    )

    validate_land_surface = validate_data.override(task_id='validate_land_surface_pre_load')(
        parquet_path=transform_land_surface,
        api_source='land_surface'
    )

    upsert_climate = upsert_data.override(task_id="upsert_climate")(
        validate_climate, table_name='daily_climate'
    )

    upsert_air_quality = upsert_data.override(task_id="upsert_air_quality")(
            validate_air_quality, table_name='daily_air_quality'
    )

    end = EmptyOperator(task_id='end')

    start >> cities
    [upsert_climate , upsert_air_quality] >> end