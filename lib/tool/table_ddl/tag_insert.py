from . import rule
from lib.db.ddl import mysql


class Table(rule.Table):
    def __init__(self, table_name, keyid, tag_name, tags=None, complete_tags=None, rule_table_name_with_prefix=True):
        self._insertTableName = 'tag_' + table_name + '_' + tag_name
        self._keyid = keyid

        self.DDLTagTable = None  # type:mysql.DDLBuild

        if rule_table_name_with_prefix:
            super().__init__(tag_name=tag_name, tags=tags, complete_tags=complete_tags, table_name_prefix=table_name)
        else:
            super().__init__(tag_name=tag_name, tags=tags, complete_tags=complete_tags)

        self._init()

    def get_table_name(self):
        return self._insertTableName

    def build(self, db, is_truncate_table=False):
        if is_truncate_table:
            db.drop(self._insertTableName)

        self.DDLTagTable.build(db=db)
        super().build(db=db, is_truncate_table=is_truncate_table)

        return self

    def _init(self):
        self.DDLTagTable = mysql.DDLCreate(table_name=self._insertTableName)

        self.DDLTagTable.add_id(name=self._keyid, is_index=True)
        self._add_fields(ddl_rule=self.DDLTagTable)
        self.DDLTagTable.add_int(name=f"{self._tagName}_{self.KeyWord}_num")
