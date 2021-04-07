import pandas
from lib.query.dml import Table, TableJoin
from lib.query.common import CommonQuery


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
    """
    gather_items:   {
                        key : [value, value, ...],
                    }

    """

    def top_gather(self, query: CommonQuery, sheet_name, sql, gather_items):
        gather_where_items = self._generate_gather_items_for_items(gather_items=gather_items)

        return self._top_gather(query=query, sheet_name=sheet_name, sql=sql, gather_where_items=gather_where_items)

    def top_gather_segmentation(self, query: CommonQuery, sql, segmentation_key, segmentation_values, gather_items=None):
        for value in segmentation_values:
            gather_all_where_items = []
            if gather_items:
                gather_where_items = self._generate_gather_items_for_items(gather_items=gather_items)
                for gather_where_item in gather_where_items:
                    gather_where_item[segmentation_key] = value
                    gather_all_where_items.append(gather_where_item)
            else:
                gather_all_where_items.append({segmentation_key, value})

            self._top_gather(query=query, sheet_name=segmentation_key, sql=sql, gather_where_items=gather_all_where_items)

    @staticmethod
    def _top_gather(query: CommonQuery, sheet_name, sql, gather_where_items):
        all_df = None
        for item in gather_where_items:
            exec_sql = sql
            for key, value in item.items():
                exec_sql = exec_sql.replace(f"##{key}##", value)

            df = query.get(name='', sql=exec_sql, is_to_excel=False)
            if all_df is None:
                all_df = df
            else:
                all_df = pandas.concat([all_df, df])

        query.to_excel(name=sheet_name, df=all_df)

    @staticmethod
    def _generate_gather_items_for_items(gather_items):
        all_items = []
        items = []
        for key, values in gather_items.items():
            all_items = []

            if items:
                for item in items:
                    for value in values:
                        item[key] = value
                        all_items.append(item)
            else:
                for value in values:
                    all_items.append({key: value})

            items = all_items.copy()

        return all_items

    @staticmethod
    def _generate_gather_items_for_where(gather_items):
        all_where_items = []
        where_items = []
        for key, values in gather_items.items():
            if where_items:
                for where_item in where_items:
                    for value in values:
                        all_where_items.append(f"{where_item} AND `{key}` = '{value}'")
            else:
                for value in values:
                    all_where_items.append(f"`{key}` = '{value}'")

            where_items = all_where_items.copy()

        return all_where_items
