import os
from pathlib import Path

def get_data_path(relative_path):
    storage_type = os.environ.get('STORAGE_BACKEND')
    bucket = os.environ.get('BUCKET_NAME')
    local_dir = '/opt/airflow/include/data'

    if storage_type == 'local':
        storage_loc = Path(f'{local_dir}/{relative_path}')
        storage_loc.parent.mkdir(parents=True, exist_ok=True)
        return storage_loc

    if storage_type == 'gcs':
        return f"gs://{bucket}/{relative_path}"
