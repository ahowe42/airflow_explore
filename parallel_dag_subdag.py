import datetime as dt
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator

''' Apparently, subdags are *not* recommended...'''

# args common to all tasks / operators in pipeline
args = {'start_date':dt.datetime(2021,4,20)}
dagID = 'parallel_dag_subdag'

# in the lecture, this was in a separate file, but seems more logical here
def subdag_parallel_subdag(parentDagID, childDagID, defArgs):
    with DAG(dag_id='%s.%s'%(parentDagID, childDagID), default_args=defArgs) as dag:
        # 2nd task
        task2 = BashOperator(task_id='t2', bash_command='sleep 3')
        # 3rd task
        task3 = BashOperator(task_id='t3', bash_command='sleep 3')

    return dag

# define dag(unique_id, ...) NB catchup only works as expected if the dag has not been run
with DAG(dagID, schedule_interval='@daily', default_args=args,
    catchup=True) as dag:
    ''' define tasks/operators in the pipeline: one operator per task '''
    # 1st task
    task1 = BashOperator(task_id='t1', bash_command='sleep 3')

    # subdag for tasks 2 & 3
    task23 = SubDagOperator(task_id='t23', subdag=subdag_parallel_subdag(dagID, 't23', args))

    # 4th task
    task4 = BashOperator(task_id='t4', bash_command='sleep 3')

    # define dependencies
    task1 >> task23 >> task4
