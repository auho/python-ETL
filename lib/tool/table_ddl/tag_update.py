from .interface import TableDDl
from lib.db.ddl import mysql


class Table(TableDDl):
    def __init__(self, table_name, tag_name, tags=None):
        self._tableName = table_name
        self._tagName = tag_name
        self._tags = tags

        self.DDLRule = None  # type: mysql.DDLBuild
        self.DDLSource = None  # type: mysql.DDLBuild

        self._init()

    def get_table_name(self):
        return self._tableName

    def build(self, db):
        self.DDLRule.build(db=db)
        self.DDLSource.build(db=db)

    def _init(self):
        fields = []
        fields.append([mysql.T_STRING, self._tagName + '_keyword'])
        fields.append([mysql.T_STRING, self._tagName + '_tag'])

        if self._tags:
            for key_name in self._tags:
                fields.append([mysql.T_STRING, self._tagName + '_' + key_name])

        self.DDLRule = mysql.DDLCreate(table_name='rule_' + self._tagName)
        self.DDLSource = mysql.DDLAlter(table_name=self._tableName)

        for field_item in fields:
            self.DDLRule.add_field(field_item[0], field_item[1])
            self.DDLSource.add_field(field_item[0], field_item[1])

        self.DDLRule.add_unique_index(name=self._tagName + '_keyword')
