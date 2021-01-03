from lib.db.ddl import mysql


class TagTable:
    def __init__(self, source_table_name, keyid, tag_name, tags=None):
        self._sourceTableName = source_table_name
        self._keyid = keyid
        self._tagName = tag_name
        self._tags = tags

        self.DDLRule = None  # type: mysql.DDLBuild
        self.DDLTag = None  # type:mysql.DDLBuild

        self._init()

    def build(self, db):
        self.DDLRule.build(db=db)
        self.DDLTag.build(db=db)

    def _init(self):
        self.DDLRule = mysql.DDLCreate(table_name='rule_' + self._sourceTableName + '_' + self._tagName)
        self.DDLTag = mysql.DDLCreate(table_name='tag_' + self._sourceTableName + '_' + self._tagName)

        self.DDLTag.add_id(name=self._keyid)

        fields = [
            [mysql.T_STRING, self._tagName + '_keyword'],
            [mysql.T_STRING, self._tagName + '_tag']
        ]

        if self._tags:
            for key_name in self._tags:
                fields.append([mysql.T_STRING, self._tagName + '_' + key_name])

        for field_item in fields:
            self.DDLRule.add_field(field_item[0], field_item[1])
            self.DDLTag.add_field(field_item[0], field_item[1])

        self.DDLTag.add_int(name='keyword_num')

        self.DDLRule.add_unique_index(name=self._tagName + '_keyword')
