import great_expectations as gx
from pathlib import Path


def create_daily_climate_pre_load_suite(context):
    suite_name = "pre_load_climate"
    pre_load_climate_suite = gx.ExpectationSuite(name=suite_name)
    pre_load_climate_suite = context.suites.add_or_update(pre_load_climate_suite)

    climate_cols = ['city_id', 'date', 'temperature_2m_max', 'temperature_2m_mean', 'temperature_2m_min', 'relative_humidity_2m_max', 'relative_humidity_2m_mean',
                    'relative_humidity_2m_min', 'cloud_cover_mean', 'soil_moisture_0_to_10cm_mean', 'wind_speed_10m_mean', 'wind_speed_10m_max', 'shortwave_radiation_sum', 'soil_moisture_0_to_10cm_mean', 'precipitation_sum',
                         'rain_sum', 'snowfall_sum', 'river_discharge']

    pre_load_climate_suite.add_expectation(gx.expectations.ExpectTableColumnsToMatchSet(
        column_set=climate_cols
    ))

    pre_load_climate_suite.add_expectation(gx.expectations.ExpectCompoundColumnsToBeUnique(
        column_list=climate_cols[:2]
    ))

    pre_load_climate_suite.add_expectation(gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A='temperature_2m_max',
        column_B='temperature_2m_mean',
        or_equal=True,
        mostly=1.0
    ))

    pre_load_climate_suite.add_expectation(gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A='temperature_2m_mean',
        column_B='temperature_2m_min',
        or_equal=True,
        mostly=1.0
    ))

    pre_load_climate_suite.add_expectation(gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A='relative_humidity_2m_max',
        column_B='relative_humidity_2m_mean',
        or_equal=True,
        mostly=1.0
    ))

    pre_load_climate_suite.add_expectation(gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A='relative_humidity_2m_mean',
            column_B='relative_humidity_2m_min',
            or_equal=True,
            mostly=1.0
        ))    

    percentage_cols = ['relative_humidity_2m_max', 'relative_humidity_2m_min', 'relative_humidity_2m_mean', 'cloud_cover_mean']
    for percentage_col in percentage_cols:
        pre_load_climate_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
            column=percentage_col,
            min_value=0,
            max_value=100,
            mostly=1.0
        ))

    pre_load_climate_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column='soil_moisture_0_to_10cm_mean',
        min_value=0,
        max_value=1,
        mostly=1.0
    ))

    non_negative_cols = ['wind_speed_10m_mean', 'wind_speed_10m_max', 'shortwave_radiation_sum', 'soil_moisture_0_to_10cm_mean', 'precipitation_sum',
                         'rain_sum', 'snowfall_sum']
    for non_negative_col in non_negative_cols:
        pre_load_climate_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
            column=non_negative_col,
            min_value=0,
            max_value=None,
            mostly=1.0
        ))


def create_daily_air_quality_pre_load_suite(context):
    suite_name = "pre_load_air_quality"
    pre_load_air_quality_suite = gx.ExpectationSuite(name=suite_name)
    pre_load_air_quality_suite =  context.suites.add_or_update(pre_load_air_quality_suite)

    air_quality_cols = ['city_id','date','pm2_5_mean', 'pm10_mean', 'nitrogen_dioxide_1h_max', 'sulphur_dioxide_1h_max', 'ozone_8h_max', 'carbon_dioxide_mean', 'carbon_monoxide_8h_max']

    pre_load_air_quality_suite.add_expectation(gx.expectations.ExpectTableColumnsToMatchSet(
        column_set=air_quality_cols
    ))

    pre_load_air_quality_suite.add_expectation(gx.expectations.ExpectCompoundColumnsToBeUnique(
        column_list=air_quality_cols[:2]
    ))

    for air_quality_col in air_quality_cols[2:]:
        pre_load_air_quality_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
            column=air_quality_col,
            min_value=0,
            max_value=None,
            mostly=1.0
        ))

def create_daily_land_surface_pre_load_suite(context):
    suite_name = "pre_load_land_surface"
    pre_load_land_surface_suite = gx.ExpectationSuite(name=suite_name)
    pre_load_land_surface_suite = context.suties.add_or_update(pre_load_land_surface_suite)

    land_surface_cols = ['city_id', 'date', 'surface_pressure', 'total_precipitable_water', 'sea_level_pressure', 'land_surface_temp'
                         'root_zone_soil_wetness', 'surface_longwave_downward_irradiance', 'surface_shortwave_upward_irradiance',
                         'surface_longwave_upward_irradiance', 'total_solar_irradiance', 'all_sky_surface_albedo']

    pre_load_land_surface_suite.add_expectation(gx.expectations.ExpectTableColumnsToMatchSet(
        column_set=land_surface_cols
    ))

    pre_load_land_surface_suite.add_expectation(gx.expectations.ExpectCompoundColumnsToBeUnique(
            column_list=land_surface_cols[:2]
        ))

    pre_load_land_surface_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column=land_surface_cols[5],
        min_value=-125,
        max_value=80,
        mostly=1.0
    ))

    pre_load_land_surface_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
            column=land_surface_cols[3],
            min_value=0,
            max_value=100,
            mostly=1.0
        ))

    for zero_one_col in [land_surface_cols[6], land_surface_cols[11]]:
        pre_load_land_surface_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
                column=zero_one_col,
                min_value=0,
                max_value=1,
                mostly=1.0
            ))

    for five_hun_thou_one_col in [land_surface_cols[2], land_surface_cols[4]]:
        pre_load_land_surface_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
                        column=five_hun_thou_one_col,
                        min_value=500,
                        max_value=1100,
                        mostly=1.0
            ))

    for zero_thou_five_col in land_surface_cols[7:11]:
        pre_load_land_surface_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
                column=zero_thou_five_col,
                min_value=0,
                max_value=1500,
                mostly=1.0
            ))

    


def configure_checkpoint(context, api_source):

    expectation_suite = context.suites.get(name=f"pre_load_{api_source}")

    data_source = context.data_sources.add_or_update_pandas_filesystem(
        name=f"{api_source} parquet file",
        base_directory=Path(f'/opt/airflow/include/data/transformed/daily_{api_source}')
    )

    parquet_asset = data_source.add_parquet_asset(
        name=f"{api_source}_raw_parquet"
    )

    batch_definition = parquet_asset.add_batch_definition_daily(
        name=f"daily_{api_source}_batch",
        regex=(
            r"(?P<year>\d{4})-"
            r"(?P<month>\d{2})-"
            r"(?P<day>\d{2})"
            r"\.parquet"
        )
    )

    validation_definition = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(
            name=f"daily_{api_source}_validation",
            data=batch_definition,
            suite=expectation_suite
        )
    )

    checkpoint = gx.Checkpoint(
            name=f"daily_{api_source}_checkpoint",
            validation_definitions=[validation_definition],
            actions=[]
        )
    
    return context.checkpoints.add_or_update(checkpoint)

if __name__ == '__main__':
    include_root = Path(__file__).resolve().parents[1]

    GX_ROOT = include_root / "gx"

    context = gx.get_context(project_root_dir=GX_ROOT)

    create_daily_climate_pre_load_suite(context)

    create_daily_air_quality_pre_load_suite(context)

    create_daily_land_surface_pre_load_suite(context)

    sources = ['climate', 'air_quality', 'land_surface']
    for source in sources:
        configure_checkpoint(context, source)
