from . import xlsx


class XlsxImport(xlsx.XlsxImport):
    def handle(self):
        return self
