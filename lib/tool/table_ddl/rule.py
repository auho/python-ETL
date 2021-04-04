from .interface import TableDDl
from lib.db.ddl import mysql


class Table(TableDDl):
    TableRule = 'rule_'
    KeyWord = 'keyword'

    def __init__(self, tag_name, tags=None, table_name=None):
        self._tableName = None
        self._tagName = tag_name
        self._tags = tags  # type:list

        self.DDLRule = None  # type: mysql.DDLBuild

        if table_name:
            self._tableName = self.TableRule + table_name
        else:
            self._tableName = self.TableRule + tag_name

        if self._tags:
            self._tags.append(self.KeyWord)
        else:
            self._tags = [self.KeyWord]

        self._generate_ddl_rule()

    def get_table_name(self):
        return self._tableName

    def get_rule_table_name(self):
        return self._tableName

    def build(self, db):
        self.DDLRule.build(db=db)

    def _generate_ddl_rule(self):
        self.DDLRule = mysql.DDLCreate(table_name=self._tableName)

        self.DDLRule.add_string(name=self._tagName)
        self._add_fields(ddl_rule=self.DDLRule)
        self.DDLRule.add_int(name=self.KeyWord + '_len')
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
