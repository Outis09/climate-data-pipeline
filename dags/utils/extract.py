from datetime import timedelta, datetime
import time
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from pathlib import Path
from airflow.sdk.exceptions import AirflowException
from utils.helpers import get_data_path
import os
from cloudpathlib import GSPath

def extract_daily_climate(period: list, cities_chunk_path):     
    storage_type = os.getenv('STORAGE_BACKEND')
    if storage_type == 'local':
        parquet_chunk_path = Path(cities_chunk_path)
    else:
        parquet_chunk_path = GSPath(cities_chunk_path)
    chunk_id = parquet_chunk_path.stem

    if len(period) == 1:
         start_date = period[0]
         end_date = period[0]
    else:
         start_date = period[0]
         end_date = period[1]
     
    # if period == 'daily':         
    #     start_date = context['data_interval_start'].date()
    #     start_date_no_dash = start_date.strftime('%Y%m%d')
    #     end_date = start_date 
    #     end_date_no_dash = start_date_no_dash

    #     file_name = get_data_path(f'raw/daily_climate/{start_date}/{chunk_id}.parquet')
        
    # elif period == 'yearly':
    #     start_date = context['data_interval_start'].date()
    #     start_date_no_dash = start_date.strftime('%Y%m%d')
    #     end_date = context['data_interval_end'].date()
    #     end_date_no_dash = end_date.strftime('%Y%m%d')

    #     file_name = get_data_path(f"raw/annual-backfills/climate/{start_date.year}/{chunk_id}.parquet")
    # else:
    #      raise ValueError(f'Invalid period: {period}. Expected "daily" or "yearly"')

#    file_name.parent.mkdir(parents=True, exist_ok=True)

    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
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
    file_names = []
    for climate_date, data in comb.groupby('date'):
        # climate_date = datetime.strptime(climate_date.strftime() '%Y-%m-%d')
        file_name = get_data_path(f"raw/daily_climate/{climate_date.year}/{climate_date.month:0.2d}/{climate_date.day:0.2d}/{chunk_id}.parquet")    
        data.to_parquet(file_name, engine='pyarrow', compression='snappy', index=False)
        file_names.append(str(file_name))
    return file_names


def extract_daily_air_quality(period, parquet_chunk_path, **context):
    storage_type = os.getenv('STORAGE_BACKEND')
    if storage_type == 'local':
        parquet_chunk_path = Path(parquet_chunk_path)
    else:
            parquet_chunk_path = GSPath(parquet_chunk_path)
    chunk_id = parquet_chunk_path.stem
     
    if period == 'daily':         
        start_date = context['data_interval_start'].date()
        start_date_no_dash = start_date.strftime('%Y%m%d')
        end_date = start_date 
        end_date_no_dash = start_date_no_dash

        file_path = get_data_path(f'raw/daily_air_quality/{start_date}/{chunk_id}.parquet')
        
    elif period == 'yearly':
        start_date = context['data_interval_start'].date()
        start_date_no_dash = start_date.strftime('%Y%m%d')
        end_date = context['data_interval_end'].date()
        end_date_no_dash = end_date.strftime('%Y%m%d')

        file_path = get_data_path(f'raw/annual-backfills/air_quality/{start_date.year}/{chunk_id}.parquet')
    else:
         raise ValueError(f'Invalid period: {period}. Expected "daily" or "yearly"')


    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
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
    comb.to_parquet(file_path, index=False)
    return str(file_path)


def extract_daily_land_surface(period, cities_chunk_paths):
    storage_type = os.getenv('STORAGE_BACKEND')
    if storage_type == 'local':
        parquet_chunk_path = Path(cities_chunk_paths)
    else:
            parquet_chunk_path = GSPath(cities_chunk_paths)
    chunk_id = parquet_chunk_path.stem

    if len(period) == 1:
         start_date = datetime.strptime(period[0], '%Y-%m-%d')
         end_date = datetime.strptime(period[0], '%Y-%m-%d')
    else:
         start_date = datetime.strptime(period[0], '%Y-%m-%d')
         end_date = datetime.strptime(period[1], '%Y-%m-%d')

    start_date_no_dash = start_date.strftime('%Y%m%d')
    end_date_no_dash = end_date.strftime('%Y%m%d')


    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)

    url = "https://power.larc.nasa.gov/api/temporal/daily/point"
    variables = ["PS", "TS", "TQV", 'SLP', 'GWETROOT', 'ALLSKY_SFC_LW_DWN', 'ALLSKY_SFC_SW_UP', 'ALLSKY_SFC_LW_UP', 'TOA_SW_DWN', 'ALLSKY_SRF_ALB']
    parameters = ','.join(variables)
    df = pd.read_parquet(parquet_chunk_path, engine='pyarrow')
    records = []

    for _,row in df.iterrows():
        latitude = row['lat']
        longitude = row['lng']
        city_id = row['city_id']

        params = {
        "parameters": parameters,
        "community": "RE",
        "longitude": longitude, 
        "latitude": latitude,
        "start": start_date_no_dash,
        "end": end_date_no_dash,
        "format": "JSON",
        "time-standard":"UTC"
        }

        try:
            response = retry_session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except:
             raise AirflowException(f"Failed to retrieve NASA Power Data for city id: {city_id} lat: {latitude} lon: {longitude}")

        data_df = pd.DataFrame(data['properties']['parameter'])
        data_df.index.name = 'date'
        data_df = data_df.reset_index()
        data_df['date'] = pd.to_datetime(data_df['date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
        data_df['city_id'] = city_id

        records.append(data_df)
        time.sleep(1)

    

    land_surface = pd.concat(records) #pd.DataFrame(records)
    file_names = []
    for land_surface_date, data in land_surface.groupby('date'):
        land_surface_date = datetime.strptime(land_surface_date, '%Y-%m-%d')
        file_name = get_data_path(f"raw/daily_land_surface/{land_surface_date.year}/{land_surface_date.month:02d}/{land_surface_date.day:02d}/{chunk_id}.parquet")
        data.to_parquet(file_name, engine='pyarrow', compression='snappy', index=False)
        file_names.append(str(file_name))
    return file_names
        