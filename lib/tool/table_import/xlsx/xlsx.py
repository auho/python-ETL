import pandas
from lib.db.mysql import Mysql


class XlsxImport:
    def __init__(self, xlsx_file):
        self._df = None  # type:pandas.DataFrame
        self._xlsx = pandas.io.excel.ExcelFile(xlsx_file)
        print(f"xlsx file:: {xlsx_file}")

    def read_sheet(self, sheet_name, fixed=None, header=0, dtype=None, nrows=None):
        self._df = pandas.read_excel(self._xlsx, sheet_name=self._get_sheet_name(sheet_name=sheet_name), header=header, dtype=dtype, nrows=nrows)
        self._set_df(fixed=fixed)

    def read_sheet_with_columns(self, sheet_name, start_row, columns, dtype=None, fixed=None, nrows=None):
        self._df = pandas.read_excel(self._xlsx, sheet_name=self._get_sheet_name(sheet_name=sheet_name), header=None, dtype=dtype, nrows=nrows)
        self._df = self._df.iloc[start_row:, :len(columns)]
        self._df.columns = columns
        self._set_df(fixed=fixed)

    def save(self, db: Mysql, table_name, is_truncate=True, is_replace=False):
        self._compare_diff_columns(db=db, table_name=table_name, df_columns=self._df.columns)

        data = []
        for item in self._df.values:
            data.append(tuple(item))

        if is_replace:
            db.replace_many(table_name=table_name, fields=list(self._df.columns), data=data)
        else:
            if is_truncate:
                db.truncate(table_name=table_name)
            db.insert_many(table_name=table_name, fields=list(self._df.columns), data=data)

        self._state()
        self._clean()
        print(f"xlsx import done: {table_name} {len(data)}")

    @staticmethod
    def _compare_diff_columns(db, table_name, df_columns):
        table_columns = db.get_table_columns(table_name=table_name)

        diff = set(df_columns) - set(table_columns)
        if diff:
            raise Exception(f"{db.databaseName}.{table_name} columns is miss:: ", diff)

    def _state(self):
        print('columns:: ', self._df.columns)
        print(f"line amount:: {self._df.index.size}")

    def _clean(self):
        self._df = None

    def _get_sheet_name(self, sheet_name):
        real_sheet_name = None
        if type(sheet_name) == int:
            real_sheet_name = sheet_name
        else:
            for name in self._xlsx.sheet_names:
                if name.find(sheet_name) > -1:
                    real_sheet_name = name
                    break

        if real_sheet_name is None:
            raise Exception('sheet is not exists!')

        print(f"sheet name:: {real_sheet_name}")

        return real_sheet_name

    def _set_df(self, fixed):
        self._df.fillna(value="", inplace=True)
        if fixed:
            for k, v in fixed.items():
                self._df[k] = v
