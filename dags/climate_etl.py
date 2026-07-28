import pendulum
import logging
import pandas as pd
import numpy as np
import time
import datetime as dt
import os
from pathlib import Path
import openmeteo_requests
import requests_cache
from retry_requests import retry
from airflow.sdk import DAG, task
from airflow.operators.empty import EmptyOperator
from airflow.timetables.trigger import CronTriggerTimetable, DeltaTriggerTimetable
from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable
from airflow.timetables.events import EventsTimetable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.sensors.filesystem import FileSensor


with DAG(
    dag_id = 'climate',
    start_date=pendulum.datetime(2026, 1 , 1, tz='UTC'),
    schedule=CronDataIntervalTimetable("@daily", timezone='UTC'),
    catchup=False
):
    
    start = EmptyOperator(task_id='start')

    @task
    def get_cities() -> list[str]:
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

    @task
    def fetch_daily_climate(parquet_chunk_path, **context):      
        start_date = context['ds']
        end_date = context['ds']
        parquet_chunk_path = Path(parquet_chunk_path)

        cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        climate_url = "https://climate-api.open-meteo.com/v1/climate"

        daily_vars = ['temperature_2m_max','temperature_2m_min', 'temperature_2m_mean','cloud_cover_mean',
                      'relative_humidity_2m_max', 'relative_humidity_2m_min', 'relative_humidity_2m_mean',
                      'soil_moisture_0_to_10cm_mean','precipitation_sum', 'rain_sum','snowfall_sum',
                      'wind_speed_10m_mean', 'wind_speed_10m_max', 'pressure_msl_mean', 'shortwave_radiation_sum']
        # climate_models = ["CMCC_CM2_VHR4", "FGOALS_f3_H", "HiRAM_SIT_HR", "MRI_AGCM3_2_S", "EC_Earth3P_HR", "MPI_ESM1_2_XR", "NICAM16_8S"]
	
        dfs = []
        df = pd.read_parquet(parquet_chunk_path, engine='pyarrow')
        city_ids = df['city_id'].to_list()
        # city_ids = ",".join(city_ids)
        latitudes = df['lat'].to_list()
        latitudes = ",".join(str(lat) for lat in latitudes)
        longitudes = df['lng'].to_list()
        longitudes = ",".join(str(long) for long in longitudes)

        climate_params = {
                'latitude': latitudes,
                'longitude': longitudes,
                'start_date': start_date,
                'end_date': end_date,
                "models": "EC_Earth3P_HR",
                'daily':daily_vars
            }

        climate_responses = openmeteo.weather_api(climate_url, climate_params)

        flood_url = "https://flood-api.open-meteo.com/v1/flood"
        flood_params = {
            'latitude': latitudes,
            'longitude': longitudes,
            'start_date': start_date,
            'end_date': end_date,
            'daily':'river_discharge'
        }   
        flood_responses = openmeteo.weather_api(flood_url, params = flood_params)

        for city_id, climate_response, flood_response in zip(city_ids,climate_responses, flood_responses):
            daily = climate_response.Daily()
            temp_max = daily.Variables(0).ValuesAsNumpy()
            temp_min = daily.Variables(1).ValuesAsNumpy()
            temp_mean = daily.Variables(2).ValuesAsNumpy()
            cloud_cover_mean = daily.Variables(3).ValuesAsNumpy()
            rel_humidity_max = daily.Variables(4).ValuesAsNumpy()
            rel_humidity_min = daily.Variables(5).ValuesAsNumpy()
            rel_humidity_mean = daily.Variables(6).ValuesAsNumpy()
            soil_moisture_mean = daily.Variables(7).ValuesAsNumpy()
            precipitation_sum = daily.Variables(8).ValuesAsNumpy()
            rain_sum = daily.Variables(9).ValuesAsNumpy()
            snowfall_sum = daily.Variables(10).ValuesAsNumpy()
            wind_speed_mean = daily.Variables(11).ValuesAsNumpy()
            wind_speed_max = daily.Variables(12).ValuesAsNumpy()
            pressure_msl_mean = daily.Variables(13).ValuesAsNumpy()
            shortwave_radiation_sum = daily.Variables(14).ValuesAsNumpy()

            flood_daily = flood_response.Daily()
            river_discharge = flood_daily.Variables(0).ValuesAsNumpy()

            daily_data = {"date": pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end = pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left"
            )}

            daily_data['city_id'] = city_id
            daily_data['temperature_2m_max'] = temp_max
            daily_data['temperature_2m_min'] = temp_min
            daily_data['temperature_2m_mean'] = temp_mean
            daily_data['cloud_cover_mean'] = cloud_cover_mean
            daily_data['relative_humidity_2m_max'] = rel_humidity_max
            daily_data['relative_humidity_2m_min'] = rel_humidity_min
            daily_data['relative_humidity_2m_mean'] = rel_humidity_mean
            daily_data['soil_moisture_0_to_10cm_mean'] = soil_moisture_mean
            daily_data['precipitation_sum'] = precipitation_sum
            daily_data['rain_sum'] = rain_sum
            daily_data['snowfall_sum'] = snowfall_sum
            daily_data['wind_speed_10m_mean'] = wind_speed_mean
            daily_data['wind_speed_10m_max'] = wind_speed_max
            daily_data['pressure_msl_mean'] = pressure_msl_mean
            daily_data['shortwave_radiation_sum'] = shortwave_radiation_sum
            daily_data['river_discharge'] = river_discharge

            daily_df = pd.DataFrame(daily_data)
            dfs.append(daily_df)

        comb = pd.concat(dfs, ignore_index=True)

        chunk_id = parquet_chunk_path.stem
        file_name = Path(f'//opt/airflow/include/data/daily_climate/{start_date}/{chunk_id}.parquet')
        file_name.parent.mkdir(parents=True, exist_ok=True)
        comb.to_parquet(file_name, engine='pyarrow', compression='snappy', index=False)
        return comb.shape


    @task
    def fetch_daily_air_quality(parquet_chunk_path, **context):
        start_date = context['ds']
        end_date = context['ds']
        parquet_chunk_path = Path(parquet_chunk_path)

        cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        hourly_vars = ['pm10','pm2_5', 'carbon_monoxide', 'nitrogen_dioxide', 
                       'sulphur_dioxide', 'ozone', 'carbon_dioxide']

        url = "https://air-quality-api.open-meteo.com/v1/air-quality"
        
        df = pd.read_parquet(parquet_chunk_path, engine='pyarrow')
        dfs = []
        city_ids = df['city_id'].to_list()
        latitudes = df['lat'].to_list()
        latitudes = ",".join(str(lat) for lat in latitudes)
        longitudes = df['lng'].to_list()
        longitudes = ",".join(str(long) for long in longitudes)

        params = {
                    "latitude": latitudes,
                    "longitude": longitudes,
                    "hourly": hourly_vars,
                    "start_date": start_date,
                    "end_date": end_date,
                }
        responses = openmeteo.weather_api(url, params = params)

        for city_id,response in zip(city_ids, responses):    
            hourly = response.Hourly()
            hourly_pm10 = hourly.Variables(0).ValuesAsNumpy()
            hourly_pm2_5 = hourly.Variables(1).ValuesAsNumpy()
            hourly_carbon_monoxide = hourly.Variables(2).ValuesAsNumpy()
            hourly_nitrogen_dioxide = hourly.Variables(3).ValuesAsNumpy()
            hourly_sulphur_dioxide = hourly.Variables(4).ValuesAsNumpy()
            hourly_ozone = hourly.Variables(5).ValuesAsNumpy()
            hourly_carbon_dioxide = hourly.Variables(6).ValuesAsNumpy()

            hourly_data = {
                "date": pd.date_range(
                    start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
                    end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
                    freq = pd.Timedelta(seconds = hourly.Interval()),
                    inclusive = "left"
                )
            }
            hourly_data['city_id'] = city_id
            hourly_data["pm10"] = hourly_pm10
            hourly_data["pm2_5"] = hourly_pm2_5
            hourly_data['carbon_monoxide'] = hourly_carbon_monoxide
            hourly_data['nitrogen_dioxide'] = hourly_nitrogen_dioxide
            hourly_data['sulphur_dioxide'] = hourly_sulphur_dioxide
            hourly_data['ozone'] = hourly_ozone
            hourly_data['carbon_dioxide'] = hourly_carbon_dioxide

            hourly_dataframe = pd.DataFrame(data = hourly_data)
            dfs.append(hourly_dataframe)
        
        comb = pd.concat(dfs, ignore_index=True)
        chunk_id = parquet_chunk_path.stem
        file_path = Path(f'//opt/airflow/include/data/daily_air_quality_raw/{start_date}/{chunk_id}.parquet')
        file_path.parent.mkdir(parents=True, exist_ok=True)
        comb.to_parquet(file_path, index=False)
        return str(file_path)




    @task
    def aggregate_hourly_air_quality(parquet_paths,**context):
        dfs = [pd.read_parquet(parquet_path, engine='pyarrow') for parquet_path in parquet_paths]
        df = pd.concat(dfs, ignore_index=True)

        df['date'] = pd.to_datetime(df['date'])
        df.sort_values(['city_id', 'date'],inplace=True)

        df['day'] = df['date'].dt.date

        # 24 hour averages for PM2.5, PM10
        pm = df.groupby(['city_id', 'day'], as_index=False).agg(pm2_5_mean=('pm2_5', 'mean'),
                                                pm10_mean=('pm10', 'mean'))

        # max of 8-hour rolling averages
        df['ozone_8h_rolling_avg'] = df.groupby(['city_id'])['ozone'].transform(lambda x: x.rolling(window=8, min_periods=8).mean())
        df['carbon_monoxide_8h_rolling_avg'] = df.groupby(['city_id'])['ozone'].transform(lambda x: x.rolling(window=8, min_periods=8).mean())

        ozone_rolling = df.groupby(['city_id', 'day']).agg(ozone_8h_max=('ozone_8h_rolling_avg', 'max'))
        carbon_monoxide_rolling = df.groupby(['city_id', 'day']).agg(carbon_monoxide_8h_max=('carbon_monoxide_8h_rolling_avg', 'max'))

        # max of 1-hour concentration
        hourly_concenc = df.groupby(['city_id', 'day']).agg(
            nitrogen_dioxide_1h_max=('nitrogen_dioxide', 'max'),
            sulphur_dioxide_1h_max=('sulphur_dioxide', 'max')
        )

        daily_air_quality = (pm.merge(ozone_rolling, on=['city_id', 'day'], how='outer')\
            .merge(carbon_monoxide_rolling, on=['city_id', 'day'], how='outer')\
            .merge (hourly_concenc, on=['city_id', 'day'], how='outer')
        )

        host_dir = Path('/opt/airflow/include/daily_air_quality_clean')
        host_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = host_dir.joinpath(f"{context['ds']}.parquet")
        daily_air_quality.to_parquet(parquet_path, engine='pyarrow', compression='snappy')

        return str(parquet_path)
    
    end = EmptyOperator(task_id='end')

 

    @task
    def upsert_data(parquet_paths, table_name):
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
    #    return f'Printing this table: {table_name} and paths: {parquet_paths}'


    cities = get_cities()
    fetch_climate = fetch_daily_climate.expand(parquet_chunk_path=cities)
    fetch_air_quality = fetch_daily_air_quality.expand(parquet_chunk_path=cities)
    # fetch_flood = fetch_daily_flood_data.expand(parquet_chunk_path=cities)
    calc_daily_air_quality = aggregate_hourly_air_quality(fetch_air_quality)

    upsert_climate = upsert_data.override(task_id="upsert_climate")(
        fetch_climate, table_name='climate'
    )

    upsert_air_quality = upsert_data.override(task_id="upsert_air_quality")(
            calc_daily_air_quality, table_name='air_quality'
    )

    start >> cities
    cities >> fetch_climate
    cities >> fetch_air_quality
    fetch_air_quality >> calc_daily_air_quality
    calc_daily_air_quality >> upsert_air_quality
    fetch_climate >> upsert_climate
    [upsert_climate , upsert_air_quality] >> end