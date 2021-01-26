from .interface import TableDDl
from lib.db.ddl import mysql


class Table(TableDDl):
    def __init__(self, table_name, content_name, keyid, suffix='words'):
        self._tableName = table_name + "_" + content_name + '_' + suffix
        self._keyid = keyid
        self.DDLTable = None  # type: mysql.DDLBuild

        self._init()

    def get_table_name(self):
        return self._tableName

    def build(self, db):
        self.DDLTable.build(db=db)

    def _init(self):
        self.DDLTable = mysql.DDLCreate(table_name=self._tableName)

        self.DDLTable.add_id(name=self._keyid)
        self.DDLTable.add_index(name=self._keyid)
        self.DDLTable.add_string(name='word', length=30)
        self.DDLTable.add_string(name='flag', length=5)
        self.DDLTable.add_int(name='num', length=11)
