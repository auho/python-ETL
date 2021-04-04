from . import rule
from lib.db.ddl import mysql


class Table(rule.Table):
    def __init__(self, table_name, tag_name, tags=None):
        self._sourceTableName = table_name

        self.DDLSource = None  # type: mysql.DDLBuild

        super().__init__(tag_name=tag_name, tags=tags)

        self._init()

    def get_table_name(self):
        return self._sourceTableName

    def build(self, db):
        self.DDLSource.build(db=db)
        super().build(db=db)

    def _init(self):
        self.DDLSource = mysql.DDLAlter(table_name=self._sourceTableName)
        self.DDLSource.add_string(name=self._tagName)
        self._add_fields(ddl_rule=self.DDLSource)
