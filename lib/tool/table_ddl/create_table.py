from .interface import TableDDl
from lib.db.ddl import mysql


class Table(TableDDl):
    def __init__(self, table_name):
        self._tableName = table_name
        self.DDLTable = None  # type: mysql.DDLBuild

        self._init()

    def get_table_name(self):
        return self._tableName

    def build(self, db):
        self.DDLTable.build(db=db)

    def _init(self):
        self.DDLTable = mysql.DDLCreate(table_name=self._tableName)

    def add_id_index(self, name, length=20):
        self.DDLTable.add_id(name=name, length=length, is_index=True)

    def add_id_unique(self, name, length=20):
        self.DDLTable.add_id(name=name, length=length)
        self.DDLTable.add_unique_index(name=name)

    def add_string_index(self, name, length=20):
        self.DDLTable.add_string(name=name, length=length, is_index=True)

    def add_string_unique(self, name, length=20):
        self.DDLTable.add_string(name=name, length=length)
        self.DDLTable.add_unique_index(name=name)
