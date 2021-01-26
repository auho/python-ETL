from .interface import TableDDl
from lib.db.ddl import mysql


class Table(TableDDl):
    def __init__(self, table_name):
        self._tableName = table_name
        self.DDLTable = None  # type:mysql.DDLAlter

        self._init()

    def get_table_name(self):
        return self._tableName

    def build(self, db):
        self.DDLTable.build(db=db)

    def _init(self):
        self.DDLTable = mysql.DDLAlter(table_name=self._tableName)
