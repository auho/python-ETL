import pandas
from lib.db.mysql import Mysql


class XlsxImport:
    def __init__(self, xlsx_file_name):
        self._xlsx_file_name = xlsx_file_name
        self._df = None  # type:pandas.DataFrame

    def read_sheet_name(self, sheet_name, head=None):
        self._df = pandas.read_excel(self._xlsx_file_name, sheet_name=sheet_name, header=head)

    def do_import(self, keyword_name, start, columns):
        self._df = self._df.iloc[start:, :len(columns)]
        self._df.columns = columns
        self._df.drop_duplicates(subset=keyword_name, keep='first', inplace=True)
        self._df.fillna(value="", inplace=True)

        self._df['keyword_len'] = self._df.apply(lambda x: len(x[keyword_name]), axis=1)

    def save(self, db: Mysql, table_name):
        data = []
        for item in self._df.values:
            data.append(tuple(item))

        db.replace_many(table_name=table_name, fields=list(self._df.columns), data=data)
