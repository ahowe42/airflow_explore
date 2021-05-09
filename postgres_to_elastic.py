from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from elasticsearch_plugin.hooks.elastic_hook import ElasticHook
from contextlib import closing
import json

class PostgrestoElasticOperator(BaseOperator):
    def __init__(self, sql, index, pg_conn_id='postgres_default',
        es_conn_id='elasticsearch_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.index = index
        self.pg_conn_id = pg_conn_id
        self.es_conn_id = es_conn_id

    # this is what gets triggered by the executor
    def execute(self, context):
        # initialize the elasticsearch & postgres connections
        es = ElasticHook(conn_id=self.es_conn_id)
        pg = PostgresHook(conn_id=self.pg_conn_id)

        with closing(pg.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.iter_size = 1000
                # query the data from the db
                cur.execute(self.sql)
                for row in cur:
                    # make a doc from the row & add to elasticsearch
                    es.add_doc(self.index, doc_type='not important', doc=json.dumps(row, indent=2))
                    # print to the log
                    print(row)
