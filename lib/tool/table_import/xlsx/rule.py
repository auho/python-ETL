import pandas
from . import xlsx


class XlsxImport(xlsx.XlsxImport):
    def do_import(self, keyword_name, start, columns, fixed=None):
        self._set_df(start=start, columns=columns, fixed=fixed)

        self._df.drop_duplicates(subset=keyword_name, keep='first', inplace=True)
        self._df['keyword_len'] = self._df.apply(lambda x: len(x[keyword_name]), axis=1)
