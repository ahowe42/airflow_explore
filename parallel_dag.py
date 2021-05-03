import datetime as dt
from airflow.models import DAG
from airflow.operators.bash import BashOperator

# args common to all tasks / operators in pipeline
args = {'start_date':dt.datetime(2021,4,20)}

# define dag(unique_id, ...) NB catchup only works as expected if the dag has not been run
with DAG('parallel_dag', schedule_interval='@daily', default_args=args,
    catchup=False) as dag:
    ''' define tasks/operators in the pipeline: one operator per task '''
    # 1st task
    task1 = BashOperator(task_id='store_user', bash_command='sleep 3')
    # 2nd task
    task2 = BashOperator(task_id='store_user', bash_command='sleep 3')
    # 3rd task
    task3 = BashOperator(task_id='store_user', bash_command='sleep 3')
    # 4th task
    task4 = BashOperator(task_id='store_user', bash_command='sleep 3')

    # define dependencies
    task1 >> task2 >> task4
    task1 >> task3 >> task4
