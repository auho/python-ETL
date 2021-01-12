from .interface import TableDDl
from lib.db.ddl import mysql


class Table(TableDDl):
    KeyWord = 'keyword'

    def __init__(self, tag_name, tags=None):
        self._tagName = tag_name
        self._tags = tags  # type:list

        self.DDLRule = None  # type: mysql.DDLBuild

        if self._tags:
            self._tags.append(self.KeyWord)
        else:
            self._tags = [self.KeyWord]

        self._init()

    def build(self, db):
        self.DDLRule.build(db=db)

    def _init(self):
        self.DDLRule = mysql.DDLCreate(table_name='rule_' + self._tagName)

        self._add_fields(ddl_rule=self.DDLRule)
        self.DDLRule.add_int(name='keyword_len')
        self.DDLRule.add_unique_index(name=self._tagName + '_' + self.KeyWord)

    def _generate_keys(self):
        fields = []
        for key_name in self._tags:
            if key_name:
                key = self._tagName + '_' + key_name
            else:
                key = self._tagName

            fields.append([mysql.T_STRING, key])

        return fields

    def _add_fields(self, ddl_rule):
        fields = self._generate_keys()
        for field_item in fields:
            ddl_rule.add_field(field_item[0], field_item[1])
