from lib.db.ddl import mysql


class TagTable:
    def __init__(self, source_table_name, content_name, keyid):
        self._sourceTableName = source_table_name + "_" + content_name
        self._keyid = keyid

        self.DDLWords = None  # type: mysql.DDLBuild

        self._init()

    def build(self, db):
        self.DDLWords.build(db=db)

    def _init(self):
        self.DDLWords = mysql.DDLCreate(table_name=self._sourceTableName + '_words')

        self.DDLWords.add_id(name=self._keyid)
        self.DDLWords.add_string(name='word', length=30)
        self.DDLWords.add_string(name='flag', length=5)
        self.DDLWords.add_int(name='num', length=11)
