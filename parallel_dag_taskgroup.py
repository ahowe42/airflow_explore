import datetime as dt
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# args common to all tasks / operators in pipeline
args = {'start_date':dt.datetime(2021,4,20)}

# define dag(unique_id, ...) NB catchup only works as expected if the dag has not been run
with DAG('parallel_dag_taskgroup', schedule_interval='@daily', default_args=args,
    catchup=True) as dag:
    ''' define tasks/operators in the pipeline: one operator per task '''
    # 1st task
    task1 = BashOperator(task_id='t1', bash_command='sleep 3')

    # group for tasks 2 & 3
    with TaskGroup('t23') as task23:
        # 2nd task
        task2 = BashOperator(task_id='t2', bash_command='sleep 3')
        # 3rd task
        task3 = BashOperator(task_id='t3', bash_command='sleep 3')

    # 4th task
    task4 = BashOperator(task_id='t4', bash_command='sleep 3')

    # define dependencies
    task1 >> task23 >> task4
