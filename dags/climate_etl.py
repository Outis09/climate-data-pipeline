import pendulum
import pandas as pd
import numpy as np
from pathlib import Path
import openmeteo_requests
import requests_cache
from retry_requests import retry
from airflow.sdk import DAG, task
from airflow.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.db import load_data, extract_cities
from include.extract import extract_daily_climate, extract_daily_air_quality
from include.transform import agg_hourly_air_quality


with DAG(
    dag_id = 'climate',
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
    def aggregate_hourly_air_quality(parquet_paths,**context):
        parquet_path = agg_hourly_air_quality(parquet_paths, **context)
        return [str(parquet_path)]
    
    end = EmptyOperator(task_id='end')

 

    @task
    def upsert_data(parquet_paths, table_name):
        load_data(parquet_paths, table_name)
        return None


    cities = get_cities()
    fetch_climate = fetch_daily_climate.expand(parquet_chunk_path=cities)
    fetch_air_quality = fetch_daily_air_quality.expand(parquet_chunk_path=cities)
    calc_daily_air_quality = aggregate_hourly_air_quality(fetch_air_quality)

    upsert_climate = upsert_data.override(task_id="upsert_climate")(
        fetch_climate, table_name='daily_climate'
    )

    upsert_air_quality = upsert_data.override(task_id="upsert_air_quality")(
            calc_daily_air_quality, table_name='daily_air_quality'
    )

    start >> cities
    cities >> fetch_climate
    cities >> fetch_air_quality
    fetch_air_quality >> calc_daily_air_quality
    calc_daily_air_quality >> upsert_air_quality
    fetch_climate >> upsert_climate
    [upsert_climate , upsert_air_quality] >> end