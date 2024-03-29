from .interface import TableDDl
from lib.db.ddl import mysql


class Table(TableDDl):
    TableRule = 'rule'
    KeyWord = 'keyword'

    def __init__(self, tag_name, tags=None, complete_tags=None, table_name_prefix=None, table_name_suffix=None):
        self._tableName = None
        self._tagName = tag_name
        self._tags = tags  # type:list
        self._completeTags = complete_tags  # type:list

        self.DDLRule = None  # type: mysql.DDLBuild

        self._tableName = self._generate_table_name(tag_name, table_name_prefix, table_name_suffix)

        self._generate_ddl_rule()

    def get_table_name(self):
        return self._tableName

    def get_rule_table_name(self):
        return self._tableName

    def build(self, db, is_truncate_table=False):
        if is_truncate_table:
            db.drop(table_name=self._tableName)

        self.DDLRule.build(db=db)

        return self

    def _generate_ddl_rule(self):
        self.DDLRule = mysql.DDLCreate(table_name=self._tableName)

        self._add_fields(ddl_rule=self.DDLRule)
        self.DDLRule.add_int(name=self.KeyWord + '_len')
        self.DDLRule.add_unique_index(name=self._tagName + '_' + self.KeyWord)

    def _generate_keys(self):
        fields = [
            [mysql.T_STRING, self._tagName]
        ]

        if self._tags:
            for tag in self._tags:
                key = self._tagName + '_' + tag

                fields.append([mysql.T_STRING, key])

        if self._completeTags:
            for tag in self._completeTags:
                fields.append([mysql.T_STRING, tag])

        fields.append([mysql.T_STRING, self._tagName + '_' + self.KeyWord])

        return fields

    def _add_fields(self, ddl_rule):
        fields = self._generate_keys()
        for field_item in fields:
            ddl_rule.add_field(field_item[0], field_item[1])

    def _generate_table_name(self, tag_name, prefix, suffix):
        if prefix:
            prefix = f'_{prefix}'
        else:
            prefix = ''

        if suffix:
            suffix = f'_{suffix}'
        else:
            suffix = ''

        tag_name = f'_{tag_name}'

        return self.TableRule + prefix + tag_name + suffix
