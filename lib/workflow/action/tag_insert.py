from lib.dataflow import mysql
from lib.workflow.tag import interface

"""
# sole
tag_rule = tag_sole.TagRule()

a = Action(db, table_name, keyid, database_name)
a.add_rule(content_name, tag_rule)


# multi
tag_rule = tag_multi.TagRule()

a = Action(db, table_name, keyid, database_name)
a.add_rule(content_name, tag_rule)

"""


class Action(mysql.ActionInsert):
    def __init__(self, db, table_name, keyid, database_name=None, size=1000, addition_fields=None, kwargs=None):
        super().__init__(db=db, table_name=table_name, fields=[], database_name=database_name, size=size, kwargs=kwargs)

        self._keyid = keyid
        self._tagRule = None  # type: interface.TagInsert
        self._additionFields = addition_fields
        self._fields = [keyid]

    def init_action(self):
        self.fields = self._fields

    def check_action(self):
        if not self._tagRule:
            raise Exception('tag rule is error!')

    def add_rule(self, tag_rule):
        if self._tagRule:
            raise Exception('tag rule is exists!')

        if not isinstance(tag_rule, interface.TagInsert):
            raise Exception('tag rule is not interface.TagInsert!')

        tag_rule.main()

        self._tagRule = tag_rule

        self._fields.extend(self._additionFields)
        self._fields.extend(tag_rule.get_keys())

    def do(self, item):
        insert_item = (item[self._keyid],)
        if self._additionFields is not None:
            for field in self._additionFields:
                insert_item = insert_item + (item[field],)

        tags = self._tagRule.tag_insert(item)
        if tags:
            for tag in tags:
                self.add_item(insert_item + tag)
