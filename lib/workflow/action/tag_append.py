from lib.dataflow import mysql

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

    def __init__(self, db, table_name, keyid, database_name=None, size=1000, kwargs=None):
        super().__init__(db=db, table_name=table_name, keyid=keyid, database_name=database_name, size=size, kwargs=kwargs)

        self._tagRules = []

    def init_action(self):
        pass

    def check_action(self):
        if not self._tagRules:
            raise Exception('tag rules is error!')

    def add_rule(self, content_name, tag_rule):
        if content_name and tag_rule:
            tag_rule.main()
            self._tagRules.append([content_name, tag_rule])

    def do(self, item):
        update_item = {self.keyid: item[self.keyid]}

        is_update = False
        for rule in self._tagRules:
            content_name = rule[0]
            tag_rule = rule[1]

            if content_name not in item:
                continue

            tag_item = self._do_tags(tag_rule=tag_rule, item=item, content_name=content_name)
            if not tag_item:
                continue

            update_item.update(tag_item)
            is_update = True

        if is_update:
            self.add_item(update_item)

    def _do_tags(self, tag_rule, item, content_name):
        return tag_rule.tag_append(item[content_name])
