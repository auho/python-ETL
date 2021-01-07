from lib.dataflow import process
from lib.dataflow import mysql
from ..action import tag_update

"""
tag_rules = [
    tag.TagRule()
]

tag_append.TagFlow.flow(id_name='id_name', keyid='keyid', db=db, table_name='table_name', tag_rules=tag_rules)

"""


class TagFlow:
    @staticmethod
    def flow_update(db, table_name, id_name, table_keys, tag_rules, database_name=None, dp_item_funcs=None):
        """

        :param db:
        :param table_name:
        :param id_name:
        :param table_keys:
        :param tag_rules:
        :param database_name:
        :param dp_item_funcs: [[func, ['field', ...]]]
        :return:
        """

        if type(table_keys) is not list:
            raise Exception("table keys is not list")

        fields = [id_name] + table_keys
        action = tag_update.Action(db=db, table_name=table_name, id_name=id_name, database_name=database_name)

        for tag_rule in tag_rules:
            action.add_rule(tag_rule=tag_rule)

        fields = list(set(fields))

        dp = mysql.DataProvide(db=db, table_name=table_name, id_name=id_name, database_name=database_name, fields=fields, item_funcs=dp_item_funcs,
                               read_page_size=2000, last_id=0)

        process.DispatchCenter.dispatch(dp=dp, actions=[action])
