import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from utils.helpers import get_data_path

def agg_hourly_air_quality(period, parquet_paths,**context):
    dfs = [pd.read_parquet(parquet_path, engine='pyarrow') for parquet_path in parquet_paths]
    df = pd.concat(dfs, ignore_index=True)

    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(['city_id', 'date'],inplace=True)

    df['day'] = df['date'].dt.date

    # 24 hour averages for PM2.5, PM10
    pm = df.groupby(['city_id', 'day'], as_index=False).agg(pm2_5_mean=('pm2_5', 'mean'),
                                            pm10_mean=('pm10', 'mean'),
                                            carbon_dioxide_mean=('carbon_dioxide', 'mean'))

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

    daily_air_quality.rename(columns={'day':'date'}, inplace=True)

    logical_date = context['data_interval_start'].date()
    parquet_path = get_data_path(f'transformed/daily_air_quality/{logical_date.year}/{logical_date.month:02d}/{logical_date.day:02d}.parquet')
    daily_air_quality.to_parquet(parquet_path, engine='pyarrow', compression='snappy', index=False)

    return str(parquet_path)

def transform_daily_climate_chunks(parquet_paths):
    consolidated_df_list = [pd.read_parquet(parquet_path, engine='pyarrow') for parquet_path in parquet_paths]
    consolidated_df = pd.concat(consolidated_df_list, ignore_index=True)

    file_paths = []
    for logical_date, data in consolidated_df.groupby('date'):
        # logical_date = consolidated_df['date'].iloc[0]
        parquet_path = get_data_path(f"transformed/daily_climate/{logical_date.year}/{logical_date.month:02d}/{logical_date.day:02d}.parquet")
        data.to_parquet(parquet_path, engine='pyarrow', compression='snappy', index=False)
        file_paths.append(str(parquet_path))
    return file_paths

def transform_daily_land_surface(parquet_paths):
    consolidated_df_list = [pd.read_parquet(parquet_path, engine='pyarrow') for parquet_path in parquet_paths]
    consolidated_df = pd.concat(consolidated_df_list, ignore_index=True)

    consolidated_df.rename(columns={
    'PS': 'surface_pressure',

    'TQV': 'total_precipitable_water',
    'SLP': 'sea_level_pressure',
    'TS': 'land_surface_temp',
    'GWETROOT': 'root_zone_soil_wetness',
    'ALLSKY_SFC_LW_DWN': 'surface_longwave_downward_irradiance',
    'ALLSKY_SFC_SW_UP': 'surface_shortwave_upward_irradiance',
    'ALLSKY_SFC_LW_UP': 'surface_longwave_upward_irradiance',
    'TOA_SW_DWN': 'total_solar_irradiance',
    'ALLSKY_SRF_ALB': 'all_sky_surface_albedo'
    }, inplace=True)

    excl_cols = ['date', 'city_id']
    for col in consolidated_df.columns:
            if col not in excl_cols:
                    consolidated_df[col] = consolidated_df[col].replace(-999, None)

    file_paths = []
    for logical_date, data in consolidated_df.groupby('date'):
        parquet_path = get_data_path(f"transformed/ daily_land_surface/{logical_date.year}/{logical_date.month:02d}/{logical_date.day:02d}.parquet")
        data.to_parquet(parquet_path, engine='pyarrow', compression='snappy', index=False)
        file_paths.append(str(parquet_path))
    return file_paths