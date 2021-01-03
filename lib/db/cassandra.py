from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory, BatchStatement, ConsistencyLevel
import time


class Cassandra:
    def __init__(self, host, user_name=None, password=None):
        self.cluster = None  # type:Cluster
        self.session = None  # type:Session

        self._create_cluster(host=host, user_name=user_name, password=password)

    @staticmethod
    def create(cassandra_info):
        host = ''
        user_name = None
        password = None
        if 'host' in cassandra_info:
            host = cassandra_info['host']

        if 'user_name' in cassandra_info:
            user_name = cassandra_info['user_name']
            password = cassandra_info['password']

        return Cassandra(host=host, user_name=user_name, password=password)

    def connect_session(self, key_space):
        self.session = self.cluster.connect(key_space)
        self.session.row_factory = dict_factory

    def set_session_key_space(self, key_space):
        self.session.set_keyspace(key_space)

    def execute(self, sql):
        return self.session.execute(sql, timeout=10)

    def get_all(self, sql):
        return self.execute(sql)

    def insert_sql(self, sql):
        return self.execute(sql=sql)

    def insert_many(self, table_name, fields, data):
        """
        fields ['a', 'b', 'c']
        data list[tuple('a', 'b', 'c'), tuple()]


        :param table_name:
        :param fields:
        :param data:
        :return:
        """
        if not data:
            return

        fields_string = ', '.join(fields)
        field_num = len(fields)

        sql = f"INSERT INTO {table_name} ({fields_string}) VALUES (" + '?,' * (field_num - 1) + "?)"
        statement = self.session.prepare(sql)
        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)

        for item in data:
            batch.add(statement, item)

        self.session.execute(batch, timeout=30)

    def truncate(self, table_name):

        sql = f'TRUNCATE {table_name}'

        return self.execute(sql=sql)

    def close(self):
        self.cluster.shutdown()

    def _create_cluster(self, host, user_name=None, password=None):
        auth_provider = None
        if user_name:
            auth_provider = PlainTextAuthProvider(username=user_name, password=password)

        self.cluster = Cluster(contact_points=[host], auth_provider=auth_provider, connect_timeout=10)
