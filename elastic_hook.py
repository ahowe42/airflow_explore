from airflow.hooks.base import BaseHook
from elasticsearch import Elasticsearch

class ElasticHook(BaseHook):
    def __init__(self, conn_id='elasticsearch_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = self.get_connection(conn_id)
        # get connection attributes
        conn_config = {}
        hosts = []
        if conn.host:
            hosts = conn.host.split(',')

        if conn.port:
            conn_config['port'] = int(conn.port)

        if conn.login:
            conn.config['http_auth'] = (conn.login, conn.password)

        # create elasticsearch object
        self.es = Elasticsearch(hosts, **conn_config)
        self.index = conn.schema

    def info(self):
        return self.es.info()

    def set_index(self, index):
        self.index = index

    def add_doc(self, index, doc_type, doc):
        self.set_index(index)
        res = self.es.index(index=self.index, doc_type=doc_type, body=doc)
        return res
