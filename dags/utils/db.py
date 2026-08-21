from pathlib import Path
import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery
from google.cloud import storage
import os
from utils.helpers import get_data_path


def extract_cities() -> list[str]:
    storage_type = os.getenv('STORAGE_BACKEND')
    country = os.getenv('CLIMATE_COUNTRY')
    if storage_type == 'local':
        paths = extract_cities_postgres()
    elif storage_type == 'gcs':
        paths = extract_cities_bigquery(country)
    return paths


def extract_cities_postgres():
    chunk_storage_path = Path('/opt/airflow/include/data/cities')
    if next(chunk_storage_path.glob("*.parquet"), None):
        return [str(chunk_path) for chunk_path in (chunk_storage_path.glob("*.parquet"))]

    hook = PostgresHook(postgres_conn_id='weather_db')
    sql_query = """SELECT city_id, lat, lng FROM cities"""
    df = hook.get_pandas_df(sql_query)
    df = df.head(100)

    # set number of rows for each chunk
    chunk_size = 50
    chunks = [df.iloc[i : i + chunk_size] for i in range(0,len(df), chunk_size)]
    
    chunk_storage_path.mkdir(parents=True, exist_ok=True)
    chunk_paths = []
    chunk_no = 0
    for chunk in chunks:
        file_storage = chunk_storage_path.joinpath(f'cities{chunk_no}.parquet')
        chunk.to_parquet(path=file_storage, engine="pyarrow", compression="snappy", index=False)
        chunk_paths.append(str(file_storage)) 
        chunk_no += 1
    return chunk_paths

def extract_cities_bigquery(country):
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
    WHERE country = @country
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
    for chunk in chunks:
        file_storage = get_data_path(f'data/cities_chunks/cities{chunk_no}.parquet')
        chunk.to_parquet(file_storage)
        chunk_paths.append(str(file_storage))
        chunk_no += 1
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


def bq_upsert_tables(parquet_path, table_name, context):
    project_id = os.getenv("PROJECT_ID")
    dataset = os.getenv("BQ_DATASET_NAME")

    config = main_table_config[table_name]

    keys = config["keys"]
    columns = config["columns"]

    target_table = f"{project_id}.{dataset}.{table_name}"
    staging_table = f"{project_id}.{dataset}.{table_name}_{context['run_id']}_staging"

    df = pd.read_parquet(parquet_path)

    bq_client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
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

def upsert_postgres(parquet_path, table_name):
    upsert_df = pd.read_parquet(parquet_path, engine='pyarrow')
    upsert_df.replace({np.nan: None}, inplace=True)

    hook = PostgresHook(postgres_conn_id='weather_db')

    # table_name = "daily_climate"
    rows = list(upsert_df.itertuples(index=False, name=None))

    hook.upsert_rows(
        table=table_name,
        rows=rows,
        target_fields=upsert_df.columns.to_list(),
        conflict_fields=['city_id', 'date']
    )

def load_data(parquet_path, table_name, **context):
    storage_type = os.getenv('STORAGE_BACKEND')

    if storage_type == 'local':
        upsert_postgres(parquet_path, table_name)
    if storage_type == 'gcs':
        bq_upsert_tables(parquet_path, table_name, context)
