from . import rule
from lib.db.ddl import mysql


class Table(rule.Table):
    def __init__(self, table_name, keyid, tag_name, tags=None):
        self._insertTableName = 'tag_' + table_name + '_' + tag_name
        self._keyid = keyid

        self.DDLTagTable = None  # type:mysql.DDLBuild

        super().__init__(tag_name=tag_name, tags=tags)

        self._init()

    def get_table_name(self):
        return self._insertTableName

    def build(self, db):
        self.DDLTagTable.build(db=db)
        super().build(db=db)

    def _init(self):
        self.DDLTagTable = mysql.DDLCreate(table_name=self._insertTableName)

        self.DDLTagTable.add_id(name=self._keyid)
        self.DDLTagTable.add_string(name=self._tagName)
        self._add_fields(ddl_rule=self.DDLTagTable)
        self.DDLTagTable.add_int(name='keyword_num')
