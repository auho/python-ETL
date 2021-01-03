from . import tag_append
from lib.dataflow import mysql

"""
tag_rule = tag_sole.TagRule()

a = Action(db, table_name, keyid, database_name)
a.add_rule(content_name, tag_rule)

"""


class Action(tag_append.Action):
    def _do_tags(self, tag_rule, item, content_name):
        return tag_rule.tag_cover(content_name, item[content_name])
