from lib.dataflow import mysql
from lib.workflow.func.func import FuncUpdate


class Action(mysql.ActionUpdate):
    """
    可以多个 tag rule 同时 tag
    content_name:tag_rule

    """

    def __init__(self, db, table_name, id_name, database_name=None, size=1000, kwargs=None):
        super().__init__(db=db, table_name=table_name, id_name=id_name, database_name=database_name, size=size, kwargs=kwargs)

        self._funcs = []  # type: [FuncUpdate]

    def init_action(self):
        pass

    def check_action(self):
        if not self._funcs:
            raise Exception('func is error!')

    def get_fields(self):
        fields = []
        for func in self._funcs:
            fields.extend(func.get_fields())

        return list(set(fields))

    def add_func(self, func_object):
        if not func_object or not isinstance(func_object, FuncUpdate):
            raise Exception('func is not func.Func!')

        self._funcs.append(func_object)

    def do(self, item):
        update_item = {self.id_name: item[self.id_name]}

        is_update = False
        for func_object in self._funcs:  # type: FuncUpdate
            tag_item = func_object.update(item=item)
            if not tag_item:
                continue

            if type(tag_item) != dict:
                print(tag_item, item)
                raise Exception("function result is not dict!")

            update_item.update(tag_item)
            is_update = True

        if is_update:
            self.add_item(update_item)
