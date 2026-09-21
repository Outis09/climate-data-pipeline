from pathlib import Path
import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery
from google.cloud import storage
import os
from datetime import date
import calendar
from collections import defaultdict
from utils.helpers import get_data_path
import re
from decimal import Decimal, ROUND_HALF_UP
from cloudpathlib import GSPath
from utils.metrics import emit_gauge



def extract_cities() -> list[str]:
    """Get a country's cities' data based on storage type"""
    storage_type = os.getenv('STORAGE_BACKEND')
    country = os.getenv('CLIMATE_COUNTRY')
    if not country:
        country = 'Ghana'
    country = country.lower()
    if storage_type == 'local':
        paths = extract_cities_postgres(country)
    elif storage_type == 'gcs':
        paths = extract_cities_bigquery(country)
    return paths


def extract_cities_postgres(country: str) -> list[str]:
    """Get a country's cities' data """
    chunk_storage_path = Path('/opt/airflow/include/data/cities')
    if next(chunk_storage_path.glob("*.parquet"), None):
        return [str(chunk_path) for chunk_path in (chunk_storage_path.glob("*.parquet"))]

    hook = PostgresHook(postgres_conn_id='weather_db')
    sql_query = f"""SELECT city_id, lat, lng 
                    FROM climate.cities 
                    WHERE LOWER(country) = '{country}'
                    ORDER BY population DESC
                    LIMIT 100;
                """
    df = hook.get_pandas_df(sql_query)
    df = df.head(100)

    # set number of rows for each chunk
    chunk_size = 50
    chunks = [df.iloc[i : i + chunk_size] for i in range(0,len(df), chunk_size)]
    
    chunk_storage_path.mkdir(parents=True, exist_ok=True)
    chunk_paths = []
    chunk_no = 0
    total_rows = 0
    for chunk in chunks:
        file_storage = chunk_storage_path.joinpath(f'cities{chunk_no}.parquet')
        chunk.to_parquet(path=file_storage, engine="pyarrow", compression="snappy", index=False)
        chunk_paths.append(str(file_storage)) 
        chunk_no += 1
        total_rows += len(chunk)

    emit_gauge(metric_name="pipeline/global/cities_requested", value=total_rows)

    
    return chunk_paths

def extract_cities_bigquery(country: str) -> list[str]:
    """Get a country's cities' data"""
    gcs_client = storage.Client()
    dataset = os.getenv('BQ_DATASET_NAME')
    # project = os.getenv('PROJECT_ID')
    bucket = os.getenv('BUCKET_NAME')
    bucket = gcs_client.bucket(bucket)

    blobs = list(gcs_client.list_blobs(bucket, prefix='data/cities_chunks/'))
    chunk_paths = [get_data_path(blob.name) for blob in blobs if blob.name.endswith('.parquet')]
    if chunk_paths:
        return chunk_paths

    bq_client = bigquery.Client()

    query = f"""
    SELECT city_id, lat, lng
    FROM `{dataset}.cities`
    WHERE LOWER(country) = @country
    ORDER BY population desc
    LIMIT 100;
"""

    job_config = bigquery.QueryJobConfig(
        query_parameters = [bigquery.ScalarQueryParameter("country", "STRING", country)]
    )
    df = bq_client.query(query,job_config=job_config).to_dataframe()

    chunk_size = 50
    chunks = [df.iloc[i : i + chunk_size] for i in range(0,len(df), chunk_size)]
    chunk_paths = []
    chunk_no = 0
    total_rows = 0
    for chunk in chunks:
        file_storage = get_data_path(f'data/cities_chunks/cities{chunk_no}.parquet')
        chunk.to_parquet(file_storage)
        chunk_paths.append(str(file_storage))
        chunk_no += 1
        total_rows += len(chunk)
    emit_gauge(metric_name="pipeline/global/cities_requested", value=total_rows)
    return chunk_paths



    



main_table_config = {
    "daily_climate": {
        "keys": ["date", "city_id"],
        "columns": [
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "cloud_cover_mean",
            "relative_humidity_2m_max",
            "relative_humidity_2m_min",
            "relative_humidity_2m_mean",
            "soil_moisture_0_to_10cm_mean",
            "precipitation_sum",
            "rain_sum",
            "snowfall_sum",
            "wind_speed_10m_mean",
            "wind_speed_10m_max",
            "pressure_msl_mean",
            "shortwave_radiation_sum",
            "river_discharge",
        ],
    },

    "daily_air_quality": {
        "keys": ["date", "city_id"],
        "columns": [
            "pm2_5_mean",
            "pm10_mean",
            "carbon_dioxide_mean",
            "ozone_8h_max",
            "carbon_monoxide_8h_max",
            "nitrogen_dioxide_1h_max",
            "sulphur_dioxide_1h_max",
        ],
    },

    "daily_land_surface": {
        "keys": ["date", "city_id"],
        "columns": [
            "surface_pressure",
            "total_precipitable_water",
            "sea_level_pressure",
            "land_surface_temp",
            "root_zone_soil_wetness",
            "surface_longwave_downward_irradiance",
            "surface_shortwave_upward_irradiance",
            "surface_longwave_upward_irradiance",
            "total_solar_irradiance",
            "all_sky_surface_albedo",
        ],
    },
}

def to_decimal(value):
    """Convert value to decimal"""
    return None if pd.isna(value) else Decimal(str(float(value))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def get_table_config(table_name: str) -> dict:
    """Get config for main or dead-letter table."""

    if table_name.startswith("dead_letter_"):
        source = table_name.removeprefix("dead_letter_")
        main_table_name = f"daily_{source}"

        config = main_table_config[main_table_name]

        return {
            "keys": config["keys"],
            "columns": config["columns"] + [
                "failure_reasons"
            ],
        }

    return main_table_config[table_name]

def bq_upsert_tables(df: pd.DataFrame, table_name: str, run_id: str) -> None:
    """Upsert data into BigQuery using a staging table"""
    run_id = re.sub(r'[^a-zA-Z0-9_]', '_', str(run_id))
    dataset = os.getenv("BQ_DATASET_NAME")

    config = get_table_config(table_name)

    keys = config["keys"]
    columns = config["columns"]

    target_table = f"{dataset}.{table_name}"
    staging_table = f"{dataset}.{table_name}_{run_id}_staging"


    bq_client = bigquery.Client()

    target_schema = bq_client.get_table(target_table).schema
    staging_schema = [
        field
        for field in target_schema
        if field.name in set(keys + columns)
    ]


    for field in staging_schema:
        if field.name not in df.columns:
            continue
        if field.field_type in ("NUMERIC", 'BIGNUMERIC'):
            df[field.name] = df[field.name].map(to_decimal)

    job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=staging_schema
    )
        
    load_job = bq_client.load_table_from_dataframe(
        df,
        staging_table,
        job_config=job_config,
    )
    load_job.result()


    merge_condition = "\nAND ".join(
        f"target.{key} = source.{key}"
        for key in keys
    )

    update_clause = ",\n    ".join(
        f"{column} = source.{column}"
        for column in columns
    )

    insert_columns = keys + columns + ["created_at"]

    insert_column_clause = ",\n    ".join(insert_columns)

    insert_values = (
        [f"source.{column}" for column in keys + columns]
        + ["CURRENT_TIMESTAMP()"]
    )

    insert_value_clause = ",\n    ".join(insert_values)

    merge_query = f"""
        MERGE `{target_table}` AS target
        USING `{staging_table}` AS source

        ON {merge_condition}

        WHEN MATCHED THEN
          UPDATE SET
            {update_clause}

        WHEN NOT MATCHED THEN
          INSERT (
            {insert_column_clause}
          )
          VALUES (
            {insert_value_clause}
          )
    """

    try:
        merge_job = bq_client.query(merge_query)
        merge_job.result()

    finally:
        bq_client.delete_table(
            staging_table,
            not_found_ok=True
        )
    return None

def upsert_postgres(df: pd.DataFrame, table_name: str) -> None:
    """Upsert data into Postgres"""

    hook = PostgresHook(postgres_conn_id='weather_db')

    # table_name = "daily_climate"
    rows = list(df.itertuples(index=False, name=None))

    hook.upsert_rows(
        table=f"climate.{table_name}",
        rows=rows,
        target_fields=df.columns.to_list(),
        conflict_fields=['city_id', 'date']
    )
    return None

def load_data(load_vars: tuple[list[str], defaultdict], table_name: str, run_id: str):
    """Upsert data based on storage type"""
    storage_type = os.getenv('STORAGE_BACKEND')

    parquet_paths = load_vars[0]
    failed_rows = load_vars[1]

    df_list = [pd.read_parquet(path) for path in parquet_paths]
    full_df = pd.concat(df_list, ignore_index=True)
    full_df['date'] = pd.to_datetime(full_df['date']).dt.date
    full_df['city_id'] = full_df["city_id"].astype(int)
    first_date = full_df["date"].iloc[0]

    if not failed_rows:
        if storage_type == 'local':
            upsert_postgres(df=full_df, table_name=table_name)
        elif storage_type == 'gcs':
            bq_upsert_tables(df=full_df, table_name=table_name, run_id=run_id)
    else:
        failures_df = pd.DataFrame([{"city_id": city_id, "date": date, "failure_reasons": reasons} for (city_id, date), reasons in failed_rows.items()])
        failures_df['date'] = pd.to_datetime(failures_df['date']).dt.date
        failures_df['city_id'] = failures_df['city_id'].astype(int)

        merged_df = pd.merge(full_df, failures_df, how='left', on=['city_id', "date"])

        dead_letter_df = merged_df[merged_df['failure_reasons'].notna()].copy()
        good_df = merged_df[merged_df['failure_reasons'].isna()].drop(columns='failure_reasons')

        dead_letter_table = table_name.replace('daily', 'dead_letter')
        if storage_type == 'local':
            upsert_postgres(df=good_df, table_name=table_name)
            upsert_postgres(df=dead_letter_df, table_name=dead_letter_table)
        if storage_type == 'gcs':
            bq_upsert_tables(df=good_df, table_name=table_name, run_id=run_id)
            bq_upsert_tables(df=dead_letter_df, table_name=dead_letter_table, run_id=run_id)
    return first_date


def fetch_last_radiation_data_bigquery() -> str | None:
    """Get last date with full radiation data from bigquery"""
    bq_client = bigquery.Client()
    dataset = os.getenv('BQ_DATASET_NAME')

    query = f"""
    SELECT max(date) as max_date
    FROM `{dataset}.daily_land_surface`
    WHERE surface_longwave_downward_irradiance IS NOT NULL AND
            surface_shortwave_upward_irradiance IS NOT NULL AND
            surface_longwave_upward_irradiance IS NOT NULL AND
            total_solar_irradiance IS NOT NULL AND
            all_sky_surface_albedo IS NOT NULL;
    """

    result = bq_client.query(query).result()
    row = next(result)

    if row.max_date is None:
        return None

    return row.max_date.strftime("%Y-%m-%d")

def fetch_last_radiation_data_postgres() -> str | None:
    """Get last date with full radiation data from postgres db"""
    hook = PostgresHook(postgres_conn_id='weather_db')
    sql_query = """
    SELECT  max(date) as max_date
    FROM climate.daily_land_surface
    WHERE surface_longwave_downward_irradiance IS NOT NULL AND surface_longwave_downward_irradiance != 'NaN' AND
          surface_shortwave_upward_irradiance IS NOT NULL AND surface_shortwave_upward_irradiance != 'NaN' AND
          surface_longwave_upward_irradiance IS NOT NULL AND surface_longwave_upward_irradiance != 'NaN' AND
          total_solar_irradiance IS NOT NULL AND total_solar_irradiance != 'NaN' AND
          all_sky_surface_albedo IS NOT NULL AND all_sky_surface_albedo != 'NaN'
    LIMIT 1;
    """

    row = hook.get_first(sql_query)

    if row is None or row[0] is None:
        return None

    return row[0].isoformat()

def fetch_next_radiation_period() -> tuple[str, str]:
    """Get last date with full radiation data"""
    storage_type = os.getenv('STORAGE_BACKEND')
    if storage_type == 'local':
        max_date = fetch_last_radiation_data_postgres()
    elif storage_type == 'gcs':
        max_date = fetch_last_radiation_data_bigquery()

    
    max_date = date.fromisoformat(max_date)

    # Last day of max_date's month
    last_day = calendar.monthrange(
        max_date.year,
        max_date.month
    )[1]

    current_month_end = max_date.replace(day=last_day)

    if max_date == current_month_end:
        # Current month is complete, move to next month
        if max_date.month == 12:
            start_date = date(max_date.year + 1, 1, 1)
        else:
            start_date = date(max_date.year, max_date.month + 1, 1)

    else:
        # Current month is incomplete, reprocess the whole month
        start_date = max_date.replace(day=1)

    end_day = calendar.monthrange(
        start_date.year,
        start_date.month
    )[1]

    end_date = start_date.replace(day=end_day)

    return start_date.isoformat(), end_date.isoformat()