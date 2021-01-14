import pandas, time, pathlib, os


class BaseQuery:
    def __init__(self):
        self._excel_name = ''
        self._excel = None  # type: pandas.ExcelWriter
        self._basePath = ''
        self._savePath = ''
        self._isHasArgs = True
        self.DEBUG = False
        self.ENV_DEBUG = 'dev'

    def to_excel(self, name, df):
        if self.DEBUG:
            return

        start_time = time.time()

        df.to_excel(self._excel, sheet_name=name, index=False)

        end_time = time.time()
        print('add sheet to excel:: ' + str(end_time - start_time))

    def save(self):
        if self.DEBUG:
            return

        start_time = time.time()

        self._excel.save()

        end_time = time.time()
        print('save excel duration:: ' + str(end_time - start_time))

        self._excel = None

        print("excel saved\n\n")

    def _create_excel(self, excel_name):
        excel_name = excel_name.replace(' ', '_').replace('/', '_')
        self._excel_name = excel_name
        self._show_excel_info()

        if self.DEBUG:
            return

        save_path = f'{self._basePath}/xlsx'
        if self._savePath:
            save_path = f'xlsx/{self._savePath}'

        path = pathlib.Path(save_path)
        if not path.exists():
            os.makedirs(save_path, 0o755)

        if not path.exists():
            print(f'path {save_path} is error')
            exit(1)

        self._excel = pandas.ExcelWriter(f'{save_path}/{excel_name}.xlsx', options={'strings_to_urls': False}, engine='xlsxwriter')

    def _show_excel_info(self):
        print(self._excel_name)

    def _show_query_info(self, sheet_name, sql, db_conf):
        print("=================================" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print(f"excel:: {self._excel_name}")
        print(f"sheet:: {sheet_name}")
        print("db:: " + db_conf['db'])
        print(f"{sql};\n")

    def _show_execute_info(self, sql, db_conf):
        print("=================================" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print(f"excel:: {self._excel_name}")
        print("db:: " + db_conf['db'])
        print(f"{sql};\n")

    def _get_df_from_sql(self, name, sql, db, db_conf, is_force=False):
        self._show_query_info(sheet_name=name, sql=sql, db_conf=db_conf)

        if not is_force and self.DEBUG:
            return False

        duration_info = '    duration ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '[ '

        start_time = time.time()

        db.cursor.execute(sql=sql)

        end_time = time.time()
        duration_info += 'sql: ' + str(end_time - start_time) + ', '

        res = db.cursor.fetchall()  # 获取数据

        end_time = time.time()
        duration_info += 'fetch data: ' + str(end_time - end_time) + ', '

        col_result = db.cursor.description  # 获取查询结果的字段描述

        all_field = []
        for i in range(len(col_result)):
            all_field.append(col_result[i][0])  # 获取字段名，咦列表形式保存

        df = pandas.DataFrame(list(res), columns=all_field)

        end_time = time.time()
        duration_info += 'create df: ' + str(end_time - end_time) + " ]\n"

        print(duration_info)

        return df

    @staticmethod
    def _get_all(sql, db):
        return db.get_all(sql)

    @staticmethod
    def _execute_sql(sql, db):
        return db.execute(sql=sql)

    @staticmethod
    def _execute_insert_many(table_name, fields, data, db, database_name=None):
        return db.insert_many(table_name=table_name, fields=fields, data=data, database_name=database_name)

    @staticmethod
    def _execute_update_many(table_name, keyid, items, db, database_name=None):
        return db.update_many(table_name=table_name, id_name=keyid, items=items, database_name=database_name)

    @staticmethod
    def _truncate(table_name, db, database_name=None):
        return db.truncate(table_name, database_name=database_name)


class CommonQuery(BaseQuery):
    def __init__(self, app):
        super().__init__()

        self._basePath = app.basePath
        self.mysqlDbConf = app.mysqlDbConf
        self.mysqlDb = app.mysqlDb
        self.DEBUG = app.DEBUG
        self.ENV_DEBUG = app.ENV_DEBUG

        self._log_init()

    def _log_init(self):
        print("base path:: " + self._basePath)
        print("db:: " + self.mysqlDbConf['db'])
        print("debug:: " + str(int(self.DEBUG)))
        print("env_debug:: " + str(int(self.ENV_DEBUG)))
        print("\n")

    def init(self, excel_name):
        self._create_excel(excel_name)

    def truncate(self, table_name, database_name=None):
        return self._truncate(table_name=table_name, db=self.mysqlDb, database_name=database_name)

    def insert_by_sql(self, sql):
        return self._execute_sql(sql=sql, db=self.mysqlDb)

    def insert_many(self, table_name, fields, data, database_name=None):
        return self._execute_insert_many(table_name=table_name, fields=fields, data=data, db=self.mysqlDb, database_name=database_name)

    def update_many(self, table_name, keyid, items, database_name=None):
        return self._execute_update_many(table_name=table_name, keyid=keyid, items=items, db=self.mysqlDb, database_name=database_name)

    def get(self, name, sql, to_numeric_fields=None, is_to_excel=True):

        if to_numeric_fields is None:
            to_numeric_fields = []

        target_df = self._get_df_from_sql(name=name, sql=sql, db=self.mysqlDb, db_conf=self.mysqlDbConf)

        if target_df is False:
            return None

        all_field = target_df.columns

        # 转换为数字类型
        if to_numeric_fields:
            need_to_numeric_fields = []

            for field in to_numeric_fields:
                if field in all_field:
                    need_to_numeric_fields.append(field)

            if need_to_numeric_fields:
                # errors='ignore'
                target_df[need_to_numeric_fields] = target_df[need_to_numeric_fields].apply(pandas.to_numeric)

        if is_to_excel:
            self.to_excel(name, target_df)

        return target_df

    def get_all_by_sql(self, sql):
        return self._get_all(sql, self.mysqlDb)

    def get_df_by_sql(self, sql):
        """

        :param sql: sql for data
        :return:
        """

        target_df = self._get_df_from_sql(name='select by sql', sql=sql, db=self.mysqlDb, db_conf=self.mysqlDbConf, is_force=True)

        if target_df is False:
            return pandas.DataFrame([])

        return target_df
