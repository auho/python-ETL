from . import process
from ..db import cassandra


class ActionInsert(process.Action):
    def __init__(self, db: cassandra.Cassandra, table_name, fields, size=5000, database_name=None, kwargs=None):
        super().__init__(kwargs=kwargs)

        self.db = db  # type: cassandra.Cassandra
        self.fields = fields
        self.database_name = database_name
        self.table_name = table_name
        self.size = size

        self.db.connect_session(key_space=database_name)

    def before_action(self):
        self.db.truncate(table_name=self.table_name)

    def after_do(self):
        if len(self._items) > self.size:
            self.after_done()

    def after_done(self):
        if self._items:
            self.db.insert_many(table_name=self.table_name, fields=self.fields, data=self._items)
            self._items = []
