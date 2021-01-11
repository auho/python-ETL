import pymysql


class Mysql:
    def __init__(self, mysql_info):
        """

        :param mysql_info:dict
        """
        self.db = None  # type:pymysql.connections.Connection
        self.cursor = None  # type:pymysql.cursors.Cursor
        self.__mysqlInfo = mysql_info

        self.databaseName = mysql_info['db']

    def connect(self):
        self.db = pymysql.connect(**self.__mysqlInfo, charset='utf8')
        self.cursor = self.db.cursor(cursor=pymysql.cursors.DictCursor)

    def close(self):
        self.cursor.close()
        self.db.close()

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
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def update_by_data(self, table_name, id_name, item, database_name=None):
        """

        :param table_name:
        :param id_name:
        :param item:
        :param database_name:
        :return:
        """

        sql = self._generate_update_sql(database_name=database_name, table_name=table_name, item=item, id_name=id_name)

        return self.execute(sql=sql)

    def update_many(self, table_name, id_name, items, database_name=None):
        try:
            for item in items:
                sql = self._generate_update_sql(database_name=database_name, table_name=table_name, item=item, id_name=id_name)

                self.cursor.execute(sql)  # 执行sql语句

            self.db.commit()  # 提交到数据库执行
        except Exception as e:
            self.db.rollback()  # 发生错误后回滚
            raise e

    def insert_many(self, table_name, fields, data, database_name=None):
        """
        fields ['a', 'b', 'c']
        data list[tuple('a', 'b', 'c'), tuple()]


        :param table_name:
        :param fields:
        :param data:
        :param database_name:
        :return:
        """
        if not data:
            return False

        fields_string = '`, `'.join(fields)
        field_num = len(fields)

        table_name = self._generate_table_name(database_name=database_name, table_name=table_name)

        sql = f'INSERT INTO `{table_name}` (`{fields_string}`) VALUES (' + ('%s,' * field_num)[:-1] + ')'
        try:
            self.cursor.executemany(sql, data)
            self.db.commit()

            return True
        except Exception as e:
            self.db.rollback()
            raise e

    def execute(self, sql):
        try:
            self.cursor.execute(query=sql)
            self.db.commit()

            return True
        except Exception as e:
            self.db.rollback()
            raise e

    def drop(self, table_name, database_name=None):
        table_name = self._generate_table_name(database_name=database_name, table_name=table_name)

        sql = f'DROP TABLE  IF EXISTS `{table_name}`'

        return self.execute(sql)

    def truncate(self, table_name, database_name=None):
        table_name = self._generate_table_name(database_name=database_name, table_name=table_name)

        sql = f'TRUNCATE TABLE `{table_name}`'

        return self.execute(sql)

    def _generate_update_sql(self, database_name, table_name, item, id_name):
        table_name = self._generate_table_name(database_name=database_name, table_name=table_name)

        # 拼接 update sql
        sql = f'UPDATE `{table_name}` SET '
        where = ''
        fields = []
        for key, value in item.items():

            if isinstance(value, str):
                value_string = f"'{value}'"
            elif isinstance(value, int):
                value_string = f"{str(value)}"
            else:
                value_string = f"{value}"

            if key == id_name:
                where = f" WHERE `{id_name}` = {value_string}"
            else:
                fields.append(f"`{key}` = {value_string}")

        return sql + ', '.join(fields) + where

    @staticmethod
    def _generate_table_name(database_name, table_name):
        if database_name:
            table_name = database_name + '`.`' + table_name

        return table_name
