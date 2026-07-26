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
        df = df.head(10)

        # set number of rows for each chunk
        chunk_size = 250
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

        url = "https://climate-api.open-meteo.com/v1/climate"

        daily_vars = ['temperature_2m_max','temperature_2m_min', 'temperature_2m_mean','cloud_cover_mean',
                      'relative_humidity_2m_max', 'relative_humidity_2m_min', 'relative_humidity_2m_mean',
                      'soil_moisture_0_to_10cm_mean','precipitation_sum', 'rain_sum','snowfall_sum',
                      'wind_speed_10m_mean', 'wind_speed_10m_max', 'pressure_msl_mean', 'shortwave_radiation_sum']
        climate_models = ["CMCC_CM2_VHR4", "FGOALS_f3_H", "HiRAM_SIT_HR", "MRI_AGCM3_2_S", "EC_Earth3P_HR", "MPI_ESM1_2_XR", "NICAM16_8S"]
	
        dfs = []
        df = pd.read_parquet(parquet_chunk_path, engine='pyarrow')
        city_ids = df['id'].to_list()
        city_ids = ",".join(city_ids)
        latitudes = df['lat'].to_list()
        latitudes = ",".join(str(lat) for lat in latitudes)
        longitudes = df['lng'].to_list()
        longitudes = ",".join(str(long) for long in longitudes)

        params = {
                'latitude': latitudes,
                'longitude': longitudes,
                'start_date': start_date,
                'end_date': end_date,
                "models": climate_models,
                'daily':daily_vars
            }

        responses = openmeteo.weather_api(url, params)
        for city_id, response in zip(city_ids,responses):
            daily = response.Daily()
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

        hourly_vars = ['pm10','pm2_5', 'carbon_monoxide', 'nitrogen_dioxide', 'sulphur_dioxide', 'ozone',
                        'carbon_dioxide', 'ammonia', 'aerosol_optical_depth', 'methane', 'dust', 'uv_index',
                        'uv_index_clear_sky']

        url = "https://air-quality-api.open-meteo.com/v1/air-quality"
        
        df = pd.read_parquet(parquet_chunk_path, engine='pyarrow')
        dfs = []
        city_ids = df['id'].to_list()
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
            hourly_ammonia = hourly.Variables(7).ValuesAsNumpy()
            hourly_aerosol_optical_depth = hourly.Variables(8).ValuesAsNumpy()
            hourly_methane = hourly.Variables(9).ValuesAsNumpy()
            hourly_dust = hourly.Variables(10).ValuesAsNumpy()
            hourly_uv_index = hourly.Variables(11).ValuesAsNumpy()
            hourly_uv_index_clear_sky = hourly.Variables(12).ValuesAsNumpy()

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
            hourly_data['ammonia'] = hourly_ammonia
            hourly_data['aerosol_optical_depth'] = hourly_aerosol_optical_depth
            hourly_data['methane'] = hourly_methane
            hourly_data['dust'] = hourly_dust
            hourly_data['uv_index'] = hourly_uv_index
            hourly_data['uv_index_clear_sky'] = hourly_uv_index_clear_sky

            hourly_dataframe = pd.DataFrame(data = hourly_data)
            dfs.append(hourly_dataframe)
        
        comb = pd.concat(dfs, ignore_index=True)
        chunk_id = parquet_chunk_path.stem
        file_path = Path(f'//opt/airflow/include/data/daily_air_quality_raw/{start_date}/{chunk_id}.parquet')
        file_path.parent.mkdir(parents=True, exist_ok=True)
        comb.to_parquet(file_path, index=False)
        return comb.shape


    @task
    def fetch_daily_flood_data(parquet_chunk_path, **context):
        start_date = context['ds']
        end_date = context['ds']
        parquet_chunk_path = Path(parquet_chunk_path)

        cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        url = "https://flood-api.open-meteo.com/v1/flood"
        dfs = []
        df = pd.read_parquet(parquet_chunk_path, engine='pyarrow')
        city_ids = df['id'].to_list()
        latitudes = df['lat'].to_list()
        latitudes = ",".join(str(lat) for lat in latitudes)
        longitudes = df['lng'].to_list()
        longitudes = ",".join(str(long) for long in longitudes)

        params = {
        'latitude': latitudes,
        'longitude': longitudes,
        'start_date': start_date,
        'end_date': end_date,
        'daily':'river_discharge'
            } 

        responses = openmeteo.weather_api(url, params = params) 

        for city_id, response in zip(city_ids, responses):
            daily = response.Daily()
            river_discharge = daily.Variables(0).ValuesAsNumpy()

            daily_data = {"date": pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end = pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left"
            )}

            daily_data['city_id'] = city_id
            daily_data['river_discharge'] = river_discharge


            daily_df = pd.DataFrame(daily_data)
            dfs.append(daily_df)

        comb = pd.concat(dfs, ignore_index=True)
        chunk_id = parquet_chunk_path.stem
        file_name = Path(f'//opt/airflow/include/data/daily_flood_raw/{start_date}/{chunk_id}.parquet')
        file_name.parent.mkdir(parents=True, exist_ok=True)
        comb.to_parquet(file_name, index=False)
        return comb.shape
    
    
    end = EmptyOperator(task_id='end')

    cities = get_cities()
    fetch_climate = fetch_daily_climate.expand(parquet_chunk_path=cities)
    fetch_air_quality = fetch_daily_air_quality.expand(parquet_chunk_path=cities)
    fetch_flood = fetch_daily_flood_data.expand(parquet_chunk_path=cities)

    start >> cities
    cities >> fetch_climate
    cities >> fetch_air_quality
    cities >> fetch_flood
    [fetch_climate >> fetch_air_quality >> fetch_flood] >> end