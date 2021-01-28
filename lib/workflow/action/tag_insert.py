from lib.dataflow import mysql
from lib.workflow.func.func import FuncInsert


class Action(mysql.ActionInsert):
    def __init__(self, db, table_name, addition_fields=None, addition_alias=None, database_name=None, size=1000, is_truncate=True, kwargs=None):
        super().__init__(db=db, table_name=table_name, fields=[], database_name=database_name, size=size, is_truncate=is_truncate, kwargs=kwargs)

        self._additionFields = addition_fields
        self._additionAlias = addition_alias  # type: dict
        self._insertFields = []  # type: list
        self._func = None  # type: FuncInsert
        self._dpFields = []  # type: list

        if self._additionFields:
            self._dpFields.extend(self._additionFields)
            self._insertFields.extend(self._additionFields)

        if self._additionAlias:
            for k, v in self._additionAlias.items():
                self._dpFields.append(k)
                self._insertFields.append(v)

    def init_action(self):
        self.fields = self._insertFields

        print(f"action:: {self.table_name}")
        print(f"func:: {self._func.__class__.__module__}.{self._func.__class__.__name__}")

    def check_action(self):
        if not self._func:
            raise Exception('func is error!')

    def get_fields(self):
        if self._dpFields:
            return self._dpFields + self._func.get_fields()
        else:
            return self._func.get_fields()

    def add_func(self, func_object):
        if self._func:
            raise Exception('func is exists!')

        if not isinstance(func_object, FuncInsert):
            raise Exception('func is not func.FuncInsert!')

        self._func = func_object
        self._insertFields.extend(self._func.get_keys())

    def do(self, item):
        try:
            insert_item = tuple()
            if self._additionFields:
                for field in self._additionFields:
                    insert_item = insert_item + (item[field],)

            if self._additionAlias:
                for k in self._additionAlias.keys():
                    insert_item = insert_item + (item[k],)

            tags = self._func.insert(item=item)
            if not tags:
                return

            if type(tags) != list:
                print(tags, item)
                raise Exception("func result is not tuple!")

            for tag in tags:
                self.add_item(insert_item + tag)

        except Exception as e:
            print(self._insertFields)
            print(item)
            raise e
