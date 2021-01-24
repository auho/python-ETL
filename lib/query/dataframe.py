import pandas
from .dml import Table
from .common import CommonQuery


class Model:
    @staticmethod
    def _avg(df, avg_name, total_name, unit_name, with_avg=True):
        """
        data frame average
        :param df:
        :param avg_name: 平均数名称
        :param total_name: 总量名称
        :param unit_name: 总单位数名称
        :param with_avg: 是否进行平均数
        :return:
        """
        if with_avg:
            df[avg_name] = df.apply(lambda x: round(x[total_name] / x[unit_name], 4), axis=1)

        return df

    @staticmethod
    def _merge(all_df, df, merge_index):
        if all_df is None:
            return df
        else:
            return all_df.merge(df, how='outer', on=merge_index)


class TableTopGather:
    def __init__(self, name, values, table_name, select=None, where=None, group_fields=None, aggregation_dict=None, group_alias_dict=None,
                 order_dict=None, limit='', join_on=None, table_sql=None):

        self._tables = []

        for value in values:
            table = Table(table_name=table_name, select=select, where=where, group_fields=group_fields, aggregation_dict=aggregation_dict,
                          group_alias_dict=group_alias_dict, order_dict=order_dict, limit=limit, join_on=join_on, table_sql=table_sql)
            table.where_and(where=f"`{name}` = '{value}'")

            self._tables.append(table)

    def top_gather(self, name, query: CommonQuery):
        all_df = None
        for table in self._tables:
            df = query.get(name='', sql=table.sql(), is_to_excel=False)
            if all_df is None:
                all_df = df
            else:
                all_df = pandas.concat([all_df, df])

        query.to_excel(name=name, df=all_df)
