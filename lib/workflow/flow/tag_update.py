from lib.dataflow import process
from lib.dataflow import mysql
from ..action import tag_append

"""
tag_rules = [['content', tag.TagRule()]]

tag_append.TagFlow.flow(id_name='id_name', keyid='keyid', db=db, table_name='table_name', tag_rules=tag_rules)

"""


class TagFlow:
    @staticmethod
    def flow(id_name, keyid, db, table_name, tag_rules, database_name=None):
        """

        :param id_name:
        :param keyid:
        :param db:
        :param table_name:
        :param tag_rules: [['content_name', TagRule], ...]
        :param database_name:
        :return:
        """

        fields = [id_name, keyid]

        action = tag_append.Action(db=db, table_name=table_name, keyid=keyid, database_name=database_name)
        for item in tag_rules:
            fields.append(item[0])
            action.add_rule(content_name=item[0], tag_rule=item[1])

        fields = list(set(fields))

        dp = mysql.DataProvide(db=db, table_name=table_name, id_name=id_name, database_name=database_name, fields=fields, read_page_size=2000,
                               last_id=0)

        process.DispatchCenter.dispatch(dp=dp, actions=[action])