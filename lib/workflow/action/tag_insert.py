from lib.dataflow import mysql
from lib.workflow.func.func import FuncInsert


class Action(mysql.ActionInsert):
    def __init__(self, db, table_name, database_name, size=1000, addition_fields=None, kwargs=None):
        super().__init__(db=db, table_name=table_name, fields=[], database_name=database_name, size=size, kwargs=kwargs)

        self._additionFields = addition_fields
        self._fields = []  # type: list
        self._func = None  # type: FuncInsert

        if self._additionFields:
            self._fields.extend(self._additionFields)

    def init_action(self):
        self.fields = self._fields

    def check_action(self):
        if not self._func:
            raise Exception('func is error!')

    def get_fields(self):
        return self._fields

    def add_func(self, func_object):
        if self._func:
            raise Exception('func is exists!')

        if not isinstance(func_object, FuncInsert):
            raise Exception('func is not func.FuncInsert!')

        self._func = func_object
        self._fields.extend(self._func.get_keys())

    def do(self, item):
        insert_item = tuple()
        if self._additionFields is not None:
            for field in self._additionFields:
                insert_item = insert_item + (item[field],)

        tags = self._func.insert(item=item)
        if not tags:
            return

        if type(tags) != list:
            print(tags, item)
            raise Exception("func result is not tuple!")

        for tag in tags:
            self.add_item(insert_item + tag)
