from lib.dataflow import process
from lib.dataflow import mysql
from lib.workflow.action import tag_update
from lib.workflow.func.func import FuncUpdate


class TagFlow:
    @staticmethod
    def flow(db, table_name, id_name, funcs, database_name=None):
        """

        :param db:
        :param table_name:
        :param id_name:
        :param funcs:
        :param database_name:
        :return:
        """

        if type(funcs) is not list:
            raise Exception("tag rules is not list")

        fields = [id_name]
        action = tag_update.Action(db=db, table_name=table_name, id_name=id_name, database_name=database_name)

        for func in funcs:  # type: FuncUpdate
            action.add_func(func_object=func)

        fields.extend(action.get_fields())
        fields = list(set(fields))

        dp = mysql.DataProvide(db=db, table_name=table_name, id_name=id_name, database_name=database_name, fields=fields, read_page_size=2000,
                               last_id=0)

        process.DispatchCenter.dispatch(dp=dp, actions=[action])
