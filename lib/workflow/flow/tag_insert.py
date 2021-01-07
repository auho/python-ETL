from lib.dataflow import process
from lib.dataflow import mysql
from lib.workflow.action import tag_insert
from lib.workflow.rule import tag_multi

"""
tag_name_items = {
    'b': 'content',
    'c': 'content',
    'd': 'content',
}

tag_insert.TagFlow.flow_with_tags(db=db, table_name='table_name', tag_name_items=tag_name_items, id_name='content_id',
                                  keyid='content_id', addition_fields=['x1', 'x2'])

tag_rules = [['content', tag.TagRule()]]

tag_insert.TagFlow.flow(db=db, id_name='id_name', keyid='keyid', table_name='table_name', tag_rules=tag_rules)

"""


class TagFlow:
    @staticmethod
    def flow_with_tags(db, table_name, tag_name_items, id_name, keyid, addition_fields=None, database_name=None):
        """

        :param db:
        :param table_name:
        :param tag_name_items: {'tag_name': 'tag content name', ...}
        :param id_name:
        :param keyid:
        :param addition_fields:
        :param database_name:
        :return:
        """
        fields = [id_name, keyid]

        action_list = []
        for tag_name, content_name in tag_name_items.items():
            keyword_name = tag_name + '_keyword'
            tag_keyword_table = 'rule_' + table_name + '_' + tag_name
            tag_table = 'tag_' + table_name + '_' + tag_name

            tag_keyword_columns = db.get_table_columns(table_name=tag_keyword_table, database_name=database_name)
            tag_keyword_columns.remove('id')
            tag_keyword_columns.remove(keyword_name)

            tag_rule = tag_multi.TagRule(db=db, table_name=tag_keyword_table, keyword_name=keyword_name, tags_name=tag_keyword_columns)
            action = tag_insert.Action(db=db, table_name=tag_table, keyid=keyid, addition_fields=addition_fields, database_name=database_name)
            action.add_rule(tag_rule=tag_rule)

            fields.append(content_name)
            action_list.append(action)

        if addition_fields:
            fields.extend(addition_fields)

        fields = list(set(fields))

        dp = mysql.DataProvide(db=db, table_name=table_name, id_name=id_name, database_name=database_name, fields=fields, read_page_size=2000,
                               last_id=0)

        process.DispatchCenter.dispatch(dp=dp, actions=action_list)

    @staticmethod
    def flow(db, table_name, id_name, keyid, table_keys, tag_table_name, tag_rule, addition_fields=None, database_name=None, dp_item_funcs=None):
        """

        :param db:
        :param table_name:
        :param id_name:
        :param keyid:
        :param table_keys:
        :param tag_table_name: 指定 tag table name 的名字
        :param tag_rule: TagRule
        :param addition_fields:
        :param database_name:
        :param dp_item_funcs: [[func, ['field', ...]]]
        :return:
        """
        if type(table_keys) is not list:
            raise Exception("table keys is not list")

        fields = [id_name, keyid] + table_keys
        tag_table = 'tag_' + table_name + '_' + tag_table_name

        action = tag_insert.Action(db=db, table_name=tag_table, keyid=keyid, addition_fields=addition_fields, database_name=database_name)
        action.add_rule(tag_rule=tag_rule)

        if addition_fields:
            fields.extend(addition_fields)

        fields = list(set(fields))

        dp = mysql.DataProvide(db=db, table_name=table_name, id_name=id_name, database_name=database_name, fields=fields, item_funcs=dp_item_funcs,
                               read_page_size=2000, last_id=0)

        process.DispatchCenter.dispatch(dp=dp, actions=[action])
