from . import xlsx


class XlsxImport(xlsx.XlsxImport):
    def handle(self, keyword_name):
        self._df.drop_duplicates(subset=keyword_name, keep='first', inplace=True)
        self._df['keyword_len'] = self._df.apply(lambda x: len(x[keyword_name]), axis=1)
