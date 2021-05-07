from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from random import uniform
from datetime import datetime

default_args = {'start_date': datetime(2021, 5, 1)}

def _train_model(ti):
    accuracy = uniform(0.0, 1.0)
    print("Model accuracy: %0.3f"%accuracy)
    ti.xcom_push(key='model_accuracy', value=accuracy)

def _choose_best_model(ti):
    accs = ti.xcom_pull(key='model_accuracy', task_ids=['process_tasks.train_model_a',
        'process_tasks.train_model_b', 'process_tasks.train_model_c'])
    print(accs)
    ti.xcom_push(key='accuracies', value=accs)

with DAG('xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    # download task
    download_data = BashOperator(task_id='download_data', bash_command='sleep 3',
        do_xcom_push=False)

    # model training tasks - 3 models
    with TaskGroup('process_tasks') as process_tasks:
        train_model_a = PythonOperator(task_id='train_model_a', python_callable=_train_model)

        train_model_b = PythonOperator(task_id='train_model_b', python_callable=_train_model)

        train_model_c = PythonOperator(task_id='train_model_c', python_callable=_train_model)

    # choose a model
    choose_model = PythonOperator(task_id='choose_model', python_callable=_choose_best_model)

    # tak dependencies
    download_data >> process_tasks >> choose_model
