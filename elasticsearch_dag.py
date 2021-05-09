from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch_plugin.hooks.elastic_hook import ElasticHook
from elasticsearch_plugin.operators.postgres_to_elastic import PostgrestoElasticOperator

from datetime import datetime

def _PrintESInfo(ti):
    hook = ElasticHook()
    print(hook.info())
    ti.xcom_push(key='esinfo', value=hook.info())

sqlQuery = 'select * from connection;'

default_args = {'start_date': datetime(2021, 4, 1)}

with DAG('elasticsearch_dag', schedule_interval='@daily', default_args=default_args,
  catchup=False) as dag:

  # show elasticsearch information
  esinfo = PythonOperator(task_id='esinfo', python_callable=_PrintESInfo)

  # push the data from postgres to elasticsearch
  pgtoes = PostgrestoElasticOperator(task_id='pgtoes', sql=sqlQuery, index='connections')

  # dependencies
  esinfo >> pgtoes
