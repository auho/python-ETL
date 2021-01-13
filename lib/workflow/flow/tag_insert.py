from lib.dataflow import process
from lib.dataflow import mysql
from lib.workflow.action import tag_insert
from lib.workflow.func.func import FuncInsert


class TagFlow:
    @staticmethod
    def flow_actions(db, table_name, id_name, actions, database_name=None, dp_item_funcs=None):
        fields = [id_name]

        for action in actions:  # type: tag_insert.Action
            fields.extend(action.get_fields())

        fields = list(set(fields))

        dp = mysql.DataProvide(db=db, table_name=table_name, id_name=id_name, database_name=database_name, fields=fields, item_funcs=dp_item_funcs,
                               read_page_size=2000, last_id=0)

        process.DispatchCenter.dispatch(dp=dp, actions=actions)

    @staticmethod
    def flow(db, table_name, id_name, tag_table_name, func: FuncInsert, addition_fields, database_name=None):
        """

        :param db:
        :param table_name:
        :param id_name:
        :param tag_table_name:
        :param func:
        :param addition_fields:
        :param database_name:
        :return:
        """

        fields = [id_name]

        action = tag_insert.Action(db=db, table_name=tag_table_name, addition_fields=addition_fields, database_name=database_name)
        action.add_func(func_object=func)

        fields.extend(action.get_fields())
        fields = list(set(fields))

        dp = mysql.DataProvide(db=db, table_name=table_name, id_name=id_name, database_name=database_name, fields=fields, read_page_size=2000,
                               last_id=0)

        process.DispatchCenter.dispatch(dp=dp, actions=[action])
