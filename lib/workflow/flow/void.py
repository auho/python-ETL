from lib.dataflow import process
from lib.dataflow import mysql
from lib.db.mysql import Mysql
from lib.workflow.action import void
from lib.workflow.func.func import FuncVoid


class TagFlow:
    @staticmethod
    def flow(db: Mysql, table_name, id_name, funcs: [FuncVoid], exclude_fields=None, database_name=None):
        source_fields = db.get_table_columns(table_name=table_name, database_name=database_name)

        fields = source_fields.copy()
        action = void.Action()
        for func in funcs:  # type:FuncVoid
            action.add_func(func_object=func)
            fields.extend(fields)

        if exclude_fields:
            fields = list(set(fields) - set(exclude_fields))
        else:
            fields = list(set(fields))

        dp = mysql.DataProvide(db=db, table_name=table_name, id_name=id_name, database_name=database_name, fields=fields, read_page_size=2000,
                               last_id=0)

        process.DispatchCenter.dispatch(dp=dp, actions=[action])
