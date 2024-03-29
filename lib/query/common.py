import pandas, time, pathlib, os, math
from lib.common.conf import MysqlConf
from lib.db.mysql import Mysql

EXCEL_MAX_LINE = 2 ** 20


class BaseQuery:
    QUERY_AMOUNT = 0
    SHEET_AMOUNT = 0
    SHEETS_INFO = dict()
    EXCEL_INFO = dict()
    START_TIME = time.time()

    def __init__(self):
        self._excelName = ''
        self._excelFullName = ''
        self._excel = None  # type: pandas.ExcelWriter
        self._basePath = ''
        self._savePath = ''
        self._isHasArgs = True
        self.DEBUG = False
        self.ENV_DEBUG = 'dev'

        self._excelPath = ''
        self._excelAbsolutePath = ''
        self._excelAbsoluteFile = ''
        self._sheetAmount = 0
        # excel 的 sheet name 列表
        self._sheetsName = []
        # excel 的 sheet 查询， 调用 get 方法,
        self._sheets = []
        self._startTime = 0
        self._queryAmount = 0
        self._preQueryAmount = 0

    def debug_off(self):
        self.DEBUG = False

    def to_excel(self, name, df):
        if self.DEBUG:
            return

        self._sheetAmount += 1

        print('start add sheet to excel::')
        start_time = time.time()

        self._sheetsName.append(name)
        self._sheets.append(f'{name}:: ~{df.index.size} query:: {self._queryAmount - self._preQueryAmount}')
        self._preQueryAmount = self._queryAmount

        if df.index.size >= EXCEL_MAX_LINE:
            start = 0
            end = EXCEL_MAX_LINE
            i = 1
            while start < df.index.size:
                sheet_name = name + '-' + str(i).zfill(2)

                self._sheetsName.append(f" sheet_name")
                self._sheets.append(f' {sheet_name}:: ~{end - start}')

                df.iloc[start:end].to_excel(self._excel, sheet_name=sheet_name, index=False)
                i += 1

                if end >= df.index.size:
                    break
                else:
                    start = end
                    end += EXCEL_MAX_LINE
                    if end >= df.index.size:
                        end = df.index.size

        else:
            df.to_excel(self._excel, sheet_name=name, index=False)

        end_time = time.time()
        print('add sheet to excel:: ' + str(end_time - start_time))

    def save(self):
        if self.DEBUG:
            return

        start_time = time.time()
        if len(self._excel.sheets) > 0:
            self._excel.save()
        else:
            os.remove(self._excelAbsoluteFile)

        self._excel = None

        end_time = time.time()
        print('save excel duration:: ' + str(end_time - start_time))
        print(f"Total time: {self._format_duration(end_time - self._startTime)} ")
        print("\n")

        file_size = round(os.path.getsize(self._excelAbsoluteFile) / 1000 / 1000, 3)
        duration = self._format_duration(end_time - self._startTime)

        BaseQuery.QUERY_AMOUNT += self._queryAmount
        BaseQuery.SHEET_AMOUNT += self._sheetAmount
        BaseQuery.SHEETS_INFO[self._excelFullName] = self._sheets
        BaseQuery.EXCEL_INFO[self._excelFullName] = f'[Sheets: {self._sheetAmount}, Query:: {self._queryAmount}, Duration:: {duration} ' \
                                                    f'Size:: {file_size} MB]'

        self._sheets = []
        self._sheetAmount = 0
        self._sheetsName = []
        self._preQueryAmount = 0
        self._queryAmount = 0

        duration = end_time - BaseQuery.START_TIME
        print(f"QUERY:: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(BaseQuery.START_TIME))} - "
              f"{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(end_time))}")
        print(f'Excels:: {len(BaseQuery.EXCEL_INFO)}, Sheets:: {BaseQuery.SHEET_AMOUNT}, Query:: {BaseQuery.QUERY_AMOUNT}')

        for excel_name, excel_info in BaseQuery.EXCEL_INFO.items():
            print(excel_name, excel_info)

            sheet_info = BaseQuery.SHEETS_INFO[excel_name]
            for sheet in sheet_info:
                if sheet[0] == ' ':
                    print(f"    - {sheet}")
                else:
                    print(f"  - {sheet}")

        print(f"Query Total Time: {self._format_duration(duration=duration)} ")
        print("\n\n")

    def _to_csv(self, name, df):
        csv_file = self._excelAbsoluteFile.replace('.xlsx', f'-{name}.csv')
        self._sheets.append(f'{csv_file}:: {df.index.size}')

        df.to_csv(csv_file, index=False)

    def _create_excel(self, excel_name):
        self._startTime = time.time()

        excel_name = excel_name.replace(' ', '_').replace('/', '_')
        self._excelName = excel_name
        self._excelFullName = excel_name + '.xlsx'
        self._show_excel_info()

        if self.DEBUG:
            return

        self._excelPath = f'{self._basePath}/xlsx'
        if self._savePath:
            self._excelPath = f'xlsx/{self._savePath}'

        path = pathlib.Path(self._excelPath)
        if not path.exists():
            os.makedirs(self._excelPath, 0o755)

        if not path.exists():
            print(f'path {self._excelPath} is error')
            exit(1)

        self._excelAbsolutePath = path.absolute().as_posix()
        self._excelAbsoluteFile = self._excelAbsolutePath + '/' + self._excelFullName

        self._excel = pandas.ExcelWriter(f'{self._excelPath}/{self._excelFullName}', options={'strings_to_urls': False}, engine='xlsxwriter')

    def _show_excel_info(self):
        print(self._excelName)

    def _show_query_info(self, sheet_name, sql, db_conf):
        print("================================= " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print(f"excel:: {self._excelName}")
        print(f"sheet:: {sheet_name}")
        print("db:: " + db_conf.db)
        print(f"{sql};\n")

    def _show_execute_info(self, sql, db_conf):
        print("================================= " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print(f"excel:: {self._excelName}")
        print("db:: " + db_conf.db)
        print(f"{sql};\n")

    def _get_df_from_sql(self, name, sql, db, db_conf, is_force=False):
        self._show_query_info(sheet_name=name, sql=sql, db_conf=db_conf)
        self._queryAmount += 1

        if not is_force and self.DEBUG:
            return False

        duration_info = '   ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ' [ '

        start_time = time.time()

        db.cursor.execute(sql)

        end_time = time.time()
        duration_info += 'sql: ' + str(end_time - start_time) + ', '

        res = db.cursor.fetchall()  # 获取数据

        end1_time = time.time()
        duration_info += 'fetch data: ' + str(end1_time - end_time) + ', '

        col_result = db.cursor.description  # 获取查询结果的字段描述

        all_field = []
        for i in range(len(col_result)):
            all_field.append(col_result[i][0])  # 获取字段名，咦列表形式保存

        df = pandas.DataFrame(list(res), columns=all_field)

        end2_time = time.time()
        duration_info += 'create df: ' + str(end2_time - end1_time) + " ]\n"

        print(f" total: {len(res)}" + duration_info)

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

    @staticmethod
    def _format_duration(duration):
        return f"{math.floor(duration / 60)} 分 {math.ceil(duration % 60)} 秒"


class CommonQuery(BaseQuery):
    def __init__(self, app):
        super().__init__()

        self._basePath = app.modulePath
        self.mysqlDbConf = app.mysqlDbConf  # type:MysqlConf
        self.mysqlDb = app.mysqlDb  # type:Mysql
        self.DEBUG = app.DEBUG
        self.ENV_DEBUG = app.ENV_DEBUG

    def init(self, excel_name):
        self._create_excel(excel_name)

    def execute(self, sqls):
        sql_list = []
        if type(sqls) == str:
            sql_list.append(sqls)
        elif type(sqls) == list or type(sqls) == tuple:
            sql_list = sqls
        else:
            raise Exception('sql list is error!')

        for sql in sql_list:
            print(f"SQL:: {sql}")

            start_time = time.time()
            rows = self._execute_sql(sql=sql, db=self.mysqlDb)
            end_time = time.time()
            duration = end_time - start_time

            print(f"SQL AFFECTED ROWS:: {rows} DURATION:: {self._format_duration(duration=duration)}\n")

    def truncate(self, table_name, database_name=None):
        return self._truncate(table_name=table_name, db=self.mysqlDb, database_name=database_name)

    def insert_many(self, table_name, fields, data, database_name=None):
        return self._execute_insert_many(table_name=table_name, fields=fields, data=data, db=self.mysqlDb, database_name=database_name)

    def update_many(self, table_name, keyid, items, database_name=None):
        return self._execute_update_many(table_name=table_name, keyid=keyid, items=items, db=self.mysqlDb, database_name=database_name)

    def get(self, name, sql, to_numeric_fields=None, is_to_excel=True, df_apply_func=None):
        if to_numeric_fields is None:
            to_numeric_fields = []

        target_df = self._get_df_from_sql(name=name, sql=sql, db=self.mysqlDb, db_conf=self.mysqlDbConf)

        if target_df is False:
            return None

        target_df = self._handle_df(df=target_df, to_numeric_fields=to_numeric_fields, apply_func=df_apply_func)

        if is_to_excel:
            self.to_excel(name, target_df)

        return target_df

    def get_all_by_sql(self, sql):
        return self._get_all(sql, self.mysqlDb)

    def get_df_by_sql(self, sql, to_numeric_fields=None):
        """
        :param sql: sql for data
        :param to_numeric_fields:
        :return:
        """
        target_df = self._get_df_from_sql(name='select by sql', sql=sql, db=self.mysqlDb, db_conf=self.mysqlDbConf, is_force=True)

        if target_df is False:
            return pandas.DataFrame([])
        else:
            target_df = self._handle_df(df=target_df, to_numeric_fields=to_numeric_fields)

        return target_df

    @staticmethod
    def _handle_df(df, to_numeric_fields, apply_func=None):
        all_field = df.columns

        # 转换为数字类型
        if to_numeric_fields:
            need_to_numeric_fields = []

            for field in to_numeric_fields:
                if field in all_field:
                    need_to_numeric_fields.append(field)

            if need_to_numeric_fields:
                # errors='ignore'
                df[need_to_numeric_fields] = df[need_to_numeric_fields].apply(pandas.to_numeric)

        if apply_func:
            df = apply_func(df)

        return df
