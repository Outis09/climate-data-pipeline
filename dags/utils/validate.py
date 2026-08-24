import great_expectations as gx
import pandas as pd
from datetime import datetime
from airflow.sdk.exceptions import AirflowException
import os

def run_validation(api_source, parquet_path):
    storage_type = os.getenv('STORAGE_BACKEND')
    if storage_type == 'local':
        gx_root = '/opt/airflow/include/gx'
    else:
        bucket = os.getenv('BUCKET_NAME')
        gx_root = f'/home/airflow/gcs/data/gx'
    gx_context = gx.get_context(project_root_dir=gx_root)

    df = pd.read_parquet(parquet_path, engine='pyarrow')
    run_date = df['date'].iloc[0]
    try:
        run_date = datetime.strptime(run_date, '%Y-%m-%d')
    except TypeError:
        pass
    year = str(run_date.year)
    month = str(run_date.strftime("%m"))
    day = str(run_date.strftime("%d"))

    checkpoint = gx_context.checkpoints.get(f"daily_{api_source}_checkpoint") 
    daily_batch_parameters = {"year":year,
                                "month":month,
                                "day":day}
    result = checkpoint.run(batch_parameters=daily_batch_parameters)
    validation_result_id = list(result.run_results.keys())[0]
    validation_result = result.run_results[validation_result_id]

    if validation_result.get_max_severity_failure() == "CRITICAL": 
    # if not result.success:
        raise AirflowException(
            f"{api_source} data failed GX validation for {run_date}"
        )
    return parquet_path