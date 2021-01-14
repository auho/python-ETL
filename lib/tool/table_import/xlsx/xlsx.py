import pandas
from lib.db.mysql import Mysql


class XlsxImport:
    def __init__(self, xlsx_file):
        self._df = None  # type:pandas.DataFrame
        self._xlsx = pandas.io.excel.ExcelFile(xlsx_file)

    def read_sheet_name(self, sheet_name, head=None):
        real_sheet_name = ''
        for name in self._xlsx.sheet_names:
            if name.find(sheet_name) > -1:
                real_sheet_name = name
                break

        if not real_sheet_name:
            raise Exception('sheet is not exists!')

        self._df = pandas.read_excel(self._xlsx, sheet_name=real_sheet_name, header=head)

    def _set_df(self, start, columns, fixed):
        self._df = self._df.iloc[start:, :len(columns)]
        self._df.columns = columns
        self._df.fillna(value="", inplace=True)
        if fixed:
            for k, v in fixed.items():
                self._df[k] = v

    def save(self, db: Mysql, table_name, is_replace=False):
        data = []
        for item in self._df.values:
            data.append(tuple(item))

        if is_replace:
            db.replace_many(table_name=table_name, fields=list(self._df.columns), data=data)
        else:
            db.truncate(table_name=table_name)
            db.insert_many(table_name=table_name, fields=list(self._df.columns), data=data)

        self._df = None

        print(f"xlsx import done: {table_name} {len(data)}")
