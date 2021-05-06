import datetime as dt
import json
import pandas as pd
from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

#ti = task instance object
def _processUser(ti):
    # get the data from the user fetching (previous) task & ensure there's data
    users = ti.xcom_pull(task_ids=['fetch_user'])
    if (len(users)==0) or ('results' not in users[0]):
        raise ValueError('user is empty')

    # flatten the result array
    user = users[0]['results'][0]
    # parse just the json fields desired into a pd dataframe
    procUser = pd.json_normalize({'email':user['email'],
        'firstname':user['name']['first'],
        'lastname':user['name']['last'],
        'country':user['location']['country'],
        'username':user['login']['username'],
        'password':user['login']['password']})
    # output as a csv file
    procUser.to_csv('/tmp/processed_user.csv', index=None, header=False)

# bash command to store data
insrt = 'echo -e ".separator ","\n.import /tmp/processed_user.csv users" | sqlite3 /home/ahowe42/airflow/airflow.db'

# args common to all tasks / operators in pipeline
args = {'start_date':dt.datetime(2021,4,20)}

# define dag(unique_id, ...) NB catchup only works as expected if the dag has not been run
with DAG('user_processing', schedule_interval='@daily', default_args=args,
    catchup=True, concurrency=1, max_active_runs=1) as dag:
    ''' define tasks/operators in the pipeline: one operator per task '''
    # create table task - this seems to not work if run first with catchup in local executor mode
    create_table = SqliteOperator(task_id='create_table', sqlite_conn_id='db_sqlite',
        sql='''create table if not exists users(
        email TEXT PRIMARY KEY,
        firstname TEXT NOT NULL,
        lastname TEXT NOT NULL,
        country TEXT NOT NULL,
        username TEXT NOT NULL,
        password TEXT NOT NULL);''')

    # check api task - https://randomuser.me/
    check_api = HttpSensor(task_id='check_api', http_conn_id='user_api', endpoint='api/')

    # fetch user task - https://randomuser.me/
    fetch_user = SimpleHttpOperator(task_id='fetch_user', http_conn_id='user_api',
        endpoint='api/', method='GET', response_filter=lambda resp: json.loads(resp.text),
        log_response=True)

    # process user task
    process_user = PythonOperator(task_id='process_user', python_callable=_processUser)

    # store user task
    store_user = BashOperator(task_id='store_user', bash_command=insrt)

    # define dependencies
    create_table >> check_api >> fetch_user >> process_user >> store_user
