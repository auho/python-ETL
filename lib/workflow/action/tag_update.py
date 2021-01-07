from lib.dataflow import mysql
from lib.workflow.tag import interface

"""
tag_rule = tag_sole.TagRule()

a = Action(db, table_name, keyid, database_name)
a.add_rule(content_name, tag_rule)

"""


class Action(mysql.ActionUpdate):
    """
    可以多个 tag rule 同时 tag
    content_name:tag_rule

    """

    def __init__(self, db, table_name, id_name, database_name=None, size=1000, kwargs=None):
        super().__init__(db=db, table_name=table_name, id_name=id_name, database_name=database_name, size=size, kwargs=kwargs)

        self._tagRules = []  # type: [interface.TagUpdate]

    def init_action(self):
        pass

    def check_action(self):
        if not self._tagRules:
            raise Exception('tag rules is error!')

    def add_rule(self, tag_rule):
        if tag_rule or not isinstance(tag_rule, interface.TagUpdate):
            raise Exception('tag rule is not interface.TagUpdate!')

        tag_rule.main()
        self._tagRules.append(tag_rule)

    def do(self, item):
        update_item = {self.id_name: item[self.id_name]}

        is_update = False
        for tag_rule in self._tagRules:

            tag_item = tag_rule.tag_update(item)
            if not tag_item:
                continue

            update_item.update(tag_item)
            is_update = True

        if is_update:
            self.add_item(update_item)
