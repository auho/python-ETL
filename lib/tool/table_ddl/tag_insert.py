from .interface import TableDDl
from . import rule
from lib.db.ddl import mysql


class Table(rule.Table):
    def __init__(self, table_name, keyid, tag_name, tags=None):
        self._sourceTableName = table_name
        self._keyid = keyid

        self.DDLRule = None  # type: mysql.DDLBuild
        self.DDLTagTable = None  # type:mysql.DDLBuild

        super(Table, self).__init__(tag_name=tag_name, tags=tags)

    def build(self, db):
        self.DDLTagTable.build(db=db)
        super(Table, self).build(db=db)

    def _init(self):
        self.DDLTagTable = mysql.DDLCreate(table_name='tag_' + self._sourceTableName + '_' + self._tagName)

        self.DDLTagTable.add_id(name=self._keyid)
        self._add_fields(DDLRule=self.DDLTagTable)
        self.DDLTagTable.add_int(name='keyword_num')
