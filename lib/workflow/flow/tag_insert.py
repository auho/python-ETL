from lib.dataflow import process
from lib.dataflow import mysql
from lib.workflow.action import tag_insert


class TagFlow:
    @staticmethod
    def flow_actions(db, table_name, id_name, keyid, actions, database_name=None, dp_item_funcs=None):
        fields = [id_name, keyid]

        for action in actions:  # type: tag_insert.Action
            fields.extend(action.get_fields())

        fields = list(set(fields))

        dp = mysql.DataProvide(db=db, table_name=table_name, id_name=id_name, database_name=database_name, fields=fields, item_funcs=dp_item_funcs,
                               read_page_size=2000, last_id=0)

        process.DispatchCenter.dispatch(dp=dp, actions=[actions])

    @staticmethod
    def flow(db, table_name, id_name, keyid, tag_table_name, func, addition_fields=None, database_name=None, dp_item_funcs=None):
        """

        :param db:
        :param table_name:
        :param id_name:
        :param keyid:
        :param tag_table_name: 指定 tag table name 的名字
        :param func:
        :param addition_fields:
        :param database_name:
        :param dp_item_funcs: [[func, ['field', ...]]]
        :return:
        """

        fields = [id_name, keyid]
        tag_table = 'tag_' + table_name + '_' + tag_table_name

        action = tag_insert.Action(db=db, table_name=tag_table, keyid=keyid, addition_fields=addition_fields, database_name=database_name)
        action.add_func(func_object=func)

        if addition_fields:
            fields.extend(addition_fields)

        fields.extend(action.get_fields())
        fields = list(set(fields))

        dp = mysql.DataProvide(db=db, table_name=table_name, id_name=id_name, database_name=database_name, fields=fields, item_funcs=dp_item_funcs,
                               read_page_size=2000, last_id=0)

        process.DispatchCenter.dispatch(dp=dp, actions=[action])
