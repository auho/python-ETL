import pymysql
from lib.common.conf import MysqlConf


class Mysql:
    def __init__(self, mysql_conf: MysqlConf):
        """

        :param mysql_conf:dict
        """
        self.db = None  # type:pymysql.connections.Connection
        self.cursor = None  # type:pymysql.cursors.Cursor
        self.__mysqlConf = mysql_conf

        self.databaseName = mysql_conf.db

    def connect(self):
        dns = self.__mysqlConf.dns()
        self.db = pymysql.connect(**dns)
        self.cursor = self.db.cursor(cursor=pymysql.cursors.DictCursor)

    def close(self):
        self.cursor.close()
        self.db.close()

    def create_database(self, name):
        sql = f"CREATE DATABASE IF NOT EXISTS {name} DEFAULT CHARACTER SET `utf8mb4` COLLATE `utf8mb4_general_ci`;"

        return self.execute(sql=sql)

    def get_table_columns(self, table_name, database_name=None):
        if database_name is None:
            database_name = self.databaseName

        sql = "SELECT `COLUMN_NAME` " + \
              "FROM `information_schema`.`COLUMNS` " + \
              f"WHERE `TABLE_NAME` = '{table_name}' AND `TABLE_SCHEMA` = '{database_name}'"

        columns = []
        columns_res = self.get_all(sql=sql)
        if columns_res:
            for item in columns_res:
                columns.append(item['COLUMN_NAME'])

        return columns

    def get_all(self, sql):
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except Exception as e:
            self._error(sql=sql)
            raise e

    def get_column_with_list(self, sql, field):
        res = self.get_all(sql=sql)

        items = []
        for data in res:
            items.append(data[field])

        return items

    def update_by_data(self, table_name, id_name, item, database_name=None):
        res = self._update_prepare(database_name=database_name, table_name=table_name, item=item, id_name=id_name)
        return self.execute(*res)

    def update_many(self, table_name, id_name, items, database_name=None):
        res = []
        try:
            for item in items:
                res = self._update_prepare(database_name=database_name, table_name=table_name, item=item, id_name=id_name)
                self.cursor.execute(*res)

            self.db.commit()
        except Exception as e:
            self.db.rollback()
            self._error(*res)
            raise e

    def insert(self, table_name, data: dict, database_name=None, op='INSERT'):
        sql = self._generate_insert_sql(table_name=table_name, fields=data.keys(), database_name=database_name, op=op)
        return self.execute(sql=sql, args=tuple(data.values()))

    def insert_many(self, table_name, fields, data, database_name=None, op='INSERT'):
        """
        fields ['a', 'b', 'c']
        data list[tuple('a', 'b', 'c'), tuple()]


        :param table_name:
        :param fields:
        :param data:
        :param database_name:
        :param op:
        :return:
        """
        if not data:
            return False

        sql = self._generate_insert_sql(table_name=table_name, fields=fields, database_name=database_name, op=op)
        try:
            self.cursor.executemany(sql, data)
            self.db.commit()

            return True
        except Exception as e:
            self.db.rollback()
            self._error(sql=sql)
            raise e

    def replace(self, table_name, data: dict, database_name=None):
        return self.insert(table_name=table_name, data=data, database_name=database_name, op='REPLACE')

    def replace_many(self, table_name, fields, data, database_name=None):
        return self.insert_many(table_name=table_name, fields=fields, data=data, database_name=database_name, op='REPLACE')

    def execute(self, sql, args=None):
        try:
            self.cursor.execute(query=sql, args=args)
            self.db.commit()

            return True
        except Exception as e:
            self.db.rollback()
            self._error(sql=sql, data=args)
            raise e

    def drop(self, table_name, database_name=None):
        table_name = self._generate_table_name(database_name=database_name, table_name=table_name)

        sql = f'DROP TABLE IF EXISTS `{table_name}`'

        return self.execute(sql=sql)

    def truncate(self, table_name, database_name=None):
        table_name = self._generate_table_name(database_name=database_name, table_name=table_name)

        sql = f'TRUNCATE TABLE `{table_name}`'

        return self.execute(sql=sql)

    def copy(self, source_table_name, target_table_name, source_database_name=None, target_database_name=None):
        self.drop(table_name=target_table_name, database_name=target_database_name)

        source_table_name = self._generate_table_name(database_name=source_database_name, table_name=source_table_name)
        target_table_name = self._generate_table_name(database_name=target_database_name, table_name=target_table_name)

        sql = f"CREATE TABLE {target_table_name} LIKE {source_table_name}"

        return self.execute(sql=sql)

    def _generate_insert_sql(self, table_name, fields, database_name=None, op='INSERT'):
        table_name = self._generate_table_name(database_name=database_name, table_name=table_name)
        fields_string = '`, `'.join(fields)
        field_num = len(fields)

        return f'{op} INTO `{table_name}` (`{fields_string}`) VALUES (' + ('%s,' * field_num)[:-1] + ')'

    def _update_prepare(self, database_name, table_name, item, id_name):
        table_name = self._generate_table_name(database_name=database_name, table_name=table_name)

        sql = f'UPDATE `{table_name}` SET '
        fields = []
        fields_data = tuple()
        for key, value in item.items():
            if key == id_name:
                continue

            fields.append(f"`{key}` = %s")
            fields_data = fields_data + (value,)

        fields_data = fields_data + (item[id_name],)
        sql = sql + ', '.join(fields) + f" WHERE `{id_name}` = %s"

        return sql, fields_data

    @staticmethod
    def _generate_table_name(database_name, table_name):
        if database_name:
            table_name = database_name + '`.`' + table_name

        return table_name

    @staticmethod
    def _error(sql, data=None):
        print(f"ERROR SQL:: {sql}", data)
