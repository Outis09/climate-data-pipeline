from airflow.sdk import task, DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(dag_id='setup_great_expectations', schedule=None, catchup=False) as dag:
    run = BashOperator(task_id='execute_setup',
                       bash_command='python /home/airflow/gcs/data/setup_gx.py', retries=0)