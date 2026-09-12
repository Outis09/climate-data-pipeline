import great_expectations as gx
import pandas as pd
from datetime import datetime
from airflow.sdk.exceptions import AirflowException
import os
from cloudpathlib import GSPath
from pathlib import PurePath

def get_date_from_path(parquet_path):
    path = PurePath(parquet_path)
    day = int(path.stem)
    month = int(path.parent.name)
    year = int(path.parent.parent.name)

    return year, month, day

def run_validation(api_source, parquet_paths):
    storage_type = os.getenv('STORAGE_BACKEND')
    if storage_type == 'local':
        gx_root = '/opt/airflow/include/gx'

    else:
        # bucket = os.getenv('BUCKET_NAME')
        gx_root = f'/home/airflow/gcs/data/gx'
    gx_context = gx.get_context(project_root_dir=gx_root)
    checkpoint = gx_context.checkpoints.get(f"daily_{api_source}_checkpoint") 

    validated_paths = []

    for parquet_path in parquet_paths:
        year, month, day = get_date_from_path(parquet_path)    
        daily_batch_parameters = {"year":f"{year:04d}",
                                    "month":f"{month:02d}",
                                    "day":f"{day:02d}"}
        result = checkpoint.run(batch_parameters=daily_batch_parameters)
        validation_result_id = list(result.run_results.keys())[0]
        validation_result = result.run_results[validation_result_id]

        if validation_result.get_max_severity_failure() == "CRITICAL": 
        # if not result.success:
            raise AirflowException(
                f"{api_source} data failed GX validation for {year}/{month}/{day:02d}"
            )
        validated_paths.append(parquet_path)
    return validated_paths