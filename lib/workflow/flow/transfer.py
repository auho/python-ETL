from lib.dataflow import process
from lib.dataflow import mysql
from lib.db.mysql import Mysql
from lib.workflow.action import transfer
from lib.workflow.func.func import FuncTransfer


class TagFlow:
    @staticmethod
    def flow(db: Mysql, table_name, tag_table_name, id_name, func: FuncTransfer, database_name=None):
        """

        :param db:
        :param table_name:
        :param id_name:
        :param tag_table_name:
        :param func:
        :param database_name:
        :return:
        """

        fields = db.get_table_columns(table_name=table_name, database_name=database_name)
        action = transfer.Action(db=db, table_name=tag_table_name, fields=fields, database_name=database_name, size=2000)
        action.add_func(func_object=func)

        dp = mysql.DataProvide(db=db, table_name=table_name, id_name=id_name, database_name=database_name, fields=fields, read_page_size=2000,
                               last_id=0)

        process.DispatchCenter.dispatch(dp=dp, actions=[action])
