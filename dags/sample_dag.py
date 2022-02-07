from unittest.mock import DEFAULT
from airflow import DAG
from datetime import datetime 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from src.data_prepare.sample import display

DEFAULT_ARGS = {
    'start_date': datetime(2022, 1, 1)
}
SCHEDULE_INTERVAL = '@daily'
with DAG(
    'sample',
    schedule_interval=SCHEDULE_INTERVAL,
    default_args=DEFAULT_ARGS,
    catchup=False
    ) as dag:

    sample_display = PythonOperator(
        task_id = "sample_display",
        python_callable=display
    )

    dummy_display = DummyOperator(
        task_id = "dummy_display"
    )

sample_display >> dummy_display



