from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from pathlib import Path
from sqlalchemy import text

def get_raw_csv(csv_path: Path):
    df = pd.read_csv(csv_path)
    return df, csv_path

def clean_csv(df: pd.DataFrame, csv_path: Path):
    clean_df = df.drop_duplicates(subset=['lat', 'lng'], keep='first')
    clean_df['population'] = clean_df['population'].astype("Int64")
    clean_df.rename(columns={'id':'city_id'}, inplace=True)

    clean_csv_save_path = csv_path.parents[0] / "world_cities_clean.csv"
    clean_df.to_csv(clean_csv_save_path, index=False)
    return clean_df

def upsert_cleaned_cities(df: pd.DataFrame):
    hook = PostgresHook(postgres_conn_id='weather_db')
    engine = hook.get_sqlalchemy_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE TEMP TABLE cities_temp (LIKE cities INCLUDING DEFAULTS)"))

        df.to_sql('cities_temp', con=conn, if_exists='append', index=False)

        conn.execute(text("""
        INSERT INTO cities
        SELECT *
        FROM cities_temp
        ON CONFLICT (city_id)
        DO UPDATE SET
            lat = EXCLUDED.lat,
            lng = EXCLUDED.lng
        """))


if __name__ == "__main__":
    df, csv_path = get_raw_csv(Path('/opt/airflow/include/data/worldcities.csv'))

    clean_df = clean_csv(df, csv_path)

    upsert_cleaned_cities(clean_df)

    