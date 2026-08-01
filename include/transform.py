import pandas as pd
from pathlib import Path

def agg_hourly_air_quality(parquet_paths,**context):
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

        daily_air_quality.rename(columns={'day':'date'}, inplace=True)

        parquet_path = Path(f'/opt/airflow/include/daily_air_quality_clean/{context['ds']}.parquet')
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        daily_air_quality.to_parquet(parquet_path, engine='pyarrow', compression='snappy')

        return str(parquet_path)