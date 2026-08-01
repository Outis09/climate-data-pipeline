from pathlib import Path
import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract_cities() -> list[str]:
    chunk_storage_path = Path('//opt/airflow/include/staging/chunks/cities')
    if next(chunk_storage_path.glob("*.parquet"), None):
        return [str(chunk_path) for chunk_path in (chunk_storage_path.glob("*.parquet"))]

    hook = PostgresHook(postgres_conn_id='weather_db')
    sql_query = """SELECT city_id, lat, lng FROM cities"""
    df = hook.get_pandas_df(sql_query)
    df = df.head(50)

    # set number of rows for each chunk
    chunk_size = 50
    chunks = [df.iloc[i : i + chunk_size] for i in range(0,len(df), chunk_size)]
    
    chunk_storage_path.mkdir(parents=True, exist_ok=True)
    chunk_paths = []
    chunk_no = 0
    for chunk in chunks:
        file_storage = chunk_storage_path.joinpath(f'cities{chunk_no}.parquet')
        chunk.to_parquet(path=file_storage, engine="pyarrow", compression="snappy", index=False)
        chunk_paths.append(str(file_storage)) # converted to string because Path values are not serializable, so there may be xcom issues
        chunk_no += 1
    return chunk_paths

def load_data(parquet_paths, table_name):
    dfs = [pd.read_parquet(parquet_path, engine='pyarrow') for parquet_path in parquet_paths]
    upsert_df = pd.concat(dfs, ignore_index=True)
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