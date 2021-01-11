from .interface import TableDDl
from lib.db.ddl import mysql


class Table(TableDDl):
    def __init__(self, tag_name, tags=None):
        self._tagName = tag_name
        self._tags = tags

        self.DDLRule = None  # type: mysql.DDLBuild

        self._init()

    def build(self, db):
        self.DDLRule.build(db=db)

    def _init(self):
        self.DDLRule = mysql.DDLCreate(table_name='rule_' + self._tagName)

        self._add_fields(DDLRule=self.DDLRule)
        self.DDLRule.add_unique_index(name=self._tagName + '_keyword')

    def _generate_keys(self):
        fields = [
            [mysql.T_STRING, self._tagName + '_keyword'],
            [mysql.T_STRING, self._tagName + '_tag']
        ]

        if self._tags:
            for key_name in self._tags:
                fields.append([mysql.T_STRING, self._tagName + '_' + key_name])

        return fields

    def _add_fields(self, DDLRule):
        fields = self._generate_keys()
        for field_item in fields:
            DDLRule.add_field(field_item[0], field_item[1])
