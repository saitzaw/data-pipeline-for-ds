from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from includes.data_prepare.DBconnection import pg_conn

START_DATE = datetime(2022,2,1)
SCHEDULE_INTERVAL = '30 3 5 * *'
DEFAULT_ARGS = {
    'owner': 'alex',
    'depends_on_past': False,
    'start_date': START_DATE
}

with DAG(
    'data_prepare',
    default_args = DEFAULT_ARGS,
    schedule_interval = SCHEDULE_INTERVAL
) as dag:
    DB_conn = PythonOperator(
        'task_id' = 'db_conn',
        python_callable = pg_conn
    )