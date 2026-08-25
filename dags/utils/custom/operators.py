from datetime import timedelta, datetime

from airflow.sdk import BaseOperator, Context
from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger
from airflow.sdk.exceptions import AirflowException

class QuotaAwareOpenMeteoExtractionOperator(BaseOperator):
    """A custom operator, built using the BaseOperator, that defers an Open Meteo extraction task for 24 hours when the daily API limit is exceeded."""
    def __init__(self, *, python_callable,period, parquet_paths, **kwargs):
        super().__init__(**kwargs)
        self.python_callable = python_callable
        self.period = period
        self.parquet_paths = parquet_paths

    def execute(self, context: Context, current_path_index = 0, completed_paths=None, event=None):
        completed_paths = completed_paths or []

        for i in range(current_path_index, len(self.parquet_paths)):
            parquet_path = self.parquet_paths[i]
        
            try:
                result = self.python_callable(period=self.period, cities_chunk_path=parquet_path)
                completed_paths.append(result)
            except Exception as error:
                if isinstance(error, dict):
                    reason = error.get("reason", "").lower()
                else:
                    reason = str(error).lower()

                if "minutely api request limit exceeded" in reason:
                    self.log.warning("Minutely API quota exhausted. Deferring task for 1 day.")
                    delay = timedelta(minutes=1)
                elif "hourly api request limit exceeded" in reason:
                    self.log.warning(f"Hourly API quota exhausted. Deferring task for 1 day.")
                    delay = timedelta(hours=1)
                    
                elif 'daily api request limit exceeded' in reason:
                    now = datetime.now()
                    next_run = (now + timedelta(days=1)).replace(hour=0, minute=30)
                    delay = next_run - now
                    self.log.warning(f"Daily API quota exhausted. Deferring task for {delay} hours.")
                    # delay = timedelta(days=1)

                self.defer(
                trigger=TimeDeltaTrigger(delay),
                method_name='execute',
                kwargs={
                    "current_path_index": current_path_index,
                    "completed_paths": completed_paths
                }
                )
                raise AirflowException(error)
        self.log.info("Returned value: %s", completed_paths)
        return  completed_paths



    # def execute_complete(self, context: Context, event=None):
    #     self.log.info("Quota window reset. Resuming extraction.")
    #     return self.execute(context)

    # def _run_extraction(self,context: Context, current_path_index, completed_paths: list):
    #     for i in range(current_path_index, len(self.parquet_paths)):
    #         parquet_path = self.parquet_paths[i]
    #         result = self.python_callable(period=self.period, cities_chunk_path=parquet_path)

    #         completed_paths.append(result)
    #     self.log.info("Returned value: %s", completed_paths)
    #     return  completed_paths