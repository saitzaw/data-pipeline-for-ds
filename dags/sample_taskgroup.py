from datetime import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from src.data_prepare.sample_data_fetching import data_fetching
from src.models.sample_randforest import randomforest
from src.models.sample_knn import knn

START_DATE = dt(2022, 1, 1)
SCHEDULE_INTERVAL = '0 9 1 * *'

def model_selection():
    print('best model')

with DAG(
    'sample_taskgroup',
    schedule_interval = SCHEDULE_INTERVAL,
    start_date = START_DATE,
    catchup = False,
) as dag:
    fetching_data = PythonOperator(
        task_id = "data_fetching",
        python_callable = data_fetching
    )

    with TaskGroup('models') as models_task:
        random_forest_model = PythonOperator(
            task_id = 'random_forest',
            python_callable = randomforest
        )

        knn_model = PythonOperator(
            task_id = 'knn',
            python_callable = knn
        )

        best_model_selection = PythonOperator(
            task_id = 'model_selecti0n',
            python_callable = model_selection
        )

        [random_forest_model, knn_model] >> best_model_selection

fetching_data >> models_task

    



