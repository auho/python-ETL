from lib.db.ddl import mysql


class Table:
    def __init__(self, table_name):
        self._sourceTableName = table_name
        self.DDLTable = None  # type: mysql.DDLBuild

        self._init()

    def build(self, db):
        self.DDLTable.build(db=db)

    def _init(self):
        self.DDLTable = mysql.DDLCreate(table_name=self._sourceTableName)

    def add_id_index(self, name, length=20):
        self.DDLTable.add_id(name=name, length=length)
        self.DDLTable.add_index(name=name)

    def add_id_unique(self, name, length=20):
        self.DDLTable.add_id(name=name, length=length)
        self.DDLTable.add_unique_index(name=name)