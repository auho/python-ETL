from lib.dataflow import process, mysql


class TagFlow:
    @staticmethod
    def flow(db, table_name, id_name, action: process.MultiAction, database_name=None):
        fields = [id_name]

        fields.extend(action.get_fields())
        fields = list(set(fields))

        dp = mysql.DataProvide(db=db, table_name=table_name, id_name=id_name, database_name=database_name, fields=fields, read_page_size=2000,
                               last_id=0)

        process.DispatchCenter.dispatch(dp=dp, actions=[action])
