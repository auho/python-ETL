from lib.dataflow import mysql
from lib.workflow.func.func import FuncTransfer


class Action(mysql.ActionInsert):
    def __init__(self, db, table_name, fields, fields_alias=None, copy_alias=None, database_name=None, size=1000, is_truncate=True, kwargs=None):
        super().__init__(db, table_name, fields, database_name, size, is_truncate, kwargs)

        self._fields = fields
        self._fields_alias = fields_alias
        self._copy_alias = copy_alias
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

        if self._fields_alias:
            for i, field in enumerate(self._fields):
                if field in self._fields_alias:
                    self._fields[i] = self._fields_alias[field]

        if self._copy_alias:
            self._fields.extend(list(self._copy_alias.values()))

    def do(self, item):
        try:
            item = self._func.transfer(item=item)  # type:dict
            if item:
                if self._fields_alias:
                    for field, field_alias in self._fields_alias.items():
                        if field == field_alias:
                            continue

                        if field in item:
                            item[field_alias] = item[field]

                if self._copy_alias:
                    for field, field_alias in self._copy_alias.items():
                        item[field_alias] = item[field]

                new_item = []
                for field in self._fields:
                    new_item.append(item[field])

                self.add_item(tuple(new_item))

        except Exception as e:
            print(self._fields)
            print(item)
            raise e
