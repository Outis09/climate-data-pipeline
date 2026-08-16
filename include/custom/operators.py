from typing import Any
from datetime import timedelta

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

    def execute(self, context: Context) -> None:
        try:
            return self._run_extraction(context)
        except Exception as e:
            error = str(e)
            if "hourly" in error.lower():
                self.log.warning(f"Hourly API quota exhausted. Deferring task for 1 day.")
                self.defer(
                    trigger=TimeDeltaTrigger(timedelta(hours=1)),
                    method_name='execute_complete'
                )
            elif 'daily' in error.lower():
                self.log.warning(f"Daily API quota exhausted. Deferring task for 1 day.")
                self.defer(
                    trigger=TimeDeltaTrigger(timedelta(days=1)),
                    method_name='execute_complete'
                )
            else:
                raise AirflowException(error)

    def execute_complete(self, context: Context, event=None):
        self.log.info("Quota window reset. Resuming extraction.")
        return self.execute(context)

    def _run_extraction(self,context: Context):
        return self.python_callable(
            period=self.period,
            parquet_chunk_path=self.parquet_paths,
            **context)