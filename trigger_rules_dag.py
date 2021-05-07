from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime

default_args = {'start_date': datetime(2021, 4, 1)}

with DAG('trigger_rules_dag', schedule_interval='@daily', default_args=default_args,
  catchup=False) as dag:
  # 1 task - 0 is succeed, 1, is fail
  task1 = BashOperator(task_id='task1', do_xcom_push=False, bash_command='exit 1')

  # 2 task
  task2 = BashOperator(task_id='task2', do_xcom_push=False, bash_command='exit 1')

  # 3 task
  task3 = BashOperator(task_id='task3', do_xcom_push=False, bash_command='exit 0',
  trigger_rule='all_done')

  [task1, task2] >> task3
