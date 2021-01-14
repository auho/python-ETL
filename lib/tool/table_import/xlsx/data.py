from . import xlsx


class XlsxImport(xlsx.XlsxImport):
    def do_import(self, start, columns, fixed=None):
        self._set_df(start=start, columns=columns, fixed=fixed)
