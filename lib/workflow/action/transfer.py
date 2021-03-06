from lib.dataflow import mysql
from lib.workflow.func.func import FuncTransfer


class Action(mysql.ActionInsert):
    def __init__(self, db, table_name, fields, database_name=None, size=1000, is_truncate=True, kwargs=None):
        super().__init__(**self._get_method_args(locals()))

        self._fields = fields
        self._func = None  # type: FuncTransfer

    def init_action(self):
        self.fields = self._fields

        print(f"action:: {self.table_name}")
        print(f"func:: {self._func.__class__.__module__}.{self._func.__class__.__name__}")

    def check_action(self):
        if not self._func:
            raise Exception('func is error!')

    def add_func(self, func_object):
        if self._func:
            raise Exception('func is exists!')

        if not isinstance(func_object, FuncTransfer):
            raise Exception('func is not func.FuncTransfer!')

        self._func = func_object
        self._fields.extend(self._func.get_keys())

        self._fields = list(set(self._fields))

    def do(self, item):
        try:
            item = self._func.transfer(item=item)  # type:dict
            if item:
                new_item = []
                for field in self._fields:
                    new_item.append(item[field])

                self.add_item(tuple(new_item))

        except Exception as e:
            print(self._fields)
            print(item)
            raise e
