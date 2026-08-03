import great_expectations as gx
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

GX_ROOT = PROJECT_ROOT / "gx"

context = gx.get_context(project_root_dir=GX_ROOT)

def create_daily_climate_pre_load_suite(context):
    suite_name = "pre_load_climate"
    pre_load_climate_suite = gx.ExpectationSuite(name=suite_name)
    pre_load_climate_suite = context.suites.add(pre_load_climate_suite)

    pre_load_climate_suite.add_expectation(gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A='temperature_2m_max',
        column_B='temperature_2m_min',
        or_equal=True,
        mostly=1.0
    ))

    pre_load_climate_suite.add_expectation(gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A='temperature_2m_mean',
        column_B='temperature_2m_min',
        or_equal=True,
        mostly=1.0
    ))

    percentage_cols = ['relative_humidity_2m_max', 'relative_humidity_2m_min', 'relative_humidity_2m_mean', 'cloud_cover_mean', 'soil_moisture_0_to_10cm_mean']
    for percentage_col in percentage_cols:
        pre_load_climate_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
            column=percentage_col,
            min_value=0,
            max_value=100,
            mostly=1.0
        ))

    non_negative_cols = ['wind_speed_10m_mean', 'wind_speed_10m_max', 'shortwave_radiation_sum', 'soil_moisture_0_to_10cm_mean', 'precipitation_sum'
                         'rain_sum', 'snowfall_sum']
    for non_negative_col in non_negative_cols:
        pre_load_climate_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
            column=non_negative_col,
            min_value=0,
            max_value=None,
            mostly=1.0
        ))
