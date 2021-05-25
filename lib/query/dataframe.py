import pandas
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

    @staticmethod
    def _concat(all_df, df):
        if all_df is None:
            all_df = df
        else:
            all_df = pandas.concat([all_df, df])

        return all_df


class TableTopGather(Model):
    """
    gather_items:   {
                        key : [value, value, ...],
                    }

    """

    def top_gather_where(self, query: CommonQuery, sheet_name, sql, gather_where_items):
        """
        所有的条件组合，上下合并到一起

        :param query:
        :param sheet_name:
        :param sql:
        :param gather_where_items:
        :return:
        """
        return self._top_gather_to_excel(query=query, sheet_name=sheet_name, sql=sql, gather_where_items=gather_where_items)

    def top_gather(self, query: CommonQuery, sheet_name, sql, gather_items):
        """
        所有的条件组合，上下合并到一起

        :param query:
        :param sheet_name:
        :param sql:
        :param gather_items:
        :return:
        """
        gather_where_items = self._generate_gather_items_for_items(gather_items=gather_items)

        return self._top_gather_to_excel(query=query, sheet_name=sheet_name, sql=sql, gather_where_items=gather_where_items)

    def top_gather_segmentation_merge(self, query: CommonQuery, sheet_name, sql, gather_items, merge_index):
        """
        所有的条件组合，左右合并到一起

        :param query:
        :param sheet_name:
        :param sql:
        :param merge_index:
        :param gather_items:
        :return:
        """
        all_df = None
        for gather_where_items in self._generate_gather_items_for_items(gather_items):
            df = self._get_data_with_df(query=query, sql=sql, gather_where_items=gather_where_items)
            if all_df is None:
                all_df = df
            else:
                all_df = self._merge(all_df=all_df, df=df, merge_index=merge_index)

        query.to_excel(name=sheet_name, df=all_df)

    def top_gather_segmentation(self, query: CommonQuery, sql, segmentation_key, segmentation_values, gather_items=None):
        """
        每个 seg value 下的所有条件组合 一个 sheet

        :param query:
        :param sql:
        :param segmentation_key:
        :param segmentation_values:
        :param gather_items:
        :return:
        """
        for value, gather_all_where_items in self._generate_segmentation_gather_item(segmentation_key, segmentation_values, gather_items):
            self._top_gather_to_excel(query=query, sheet_name=value, sql=sql, gather_where_items=gather_all_where_items)

    def _generate_segmentation_gather_item(self, segmentation_key, segmentation_values, gather_items):
        """
        求出其他条件的组合
        每次返回一个 seg value 下所有的组合

        :param segmentation_key:
        :param segmentation_values:
        :param gather_items:
        :return:
        """
        for value in segmentation_values:
            gather_whole_where_items = []
            if gather_items:
                gather_where_items = self._generate_gather_items_for_items(gather_items=gather_items)
                for gather_where_item in gather_where_items:
                    gather_where_item[segmentation_key] = value
                    gather_whole_where_items.append(gather_where_item)
            else:
                gather_whole_where_items.append({segmentation_key: value})

            yield value, gather_whole_where_items

    def _top_gather_to_excel(self, query: CommonQuery, sheet_name, sql, gather_where_items):
        """

        :param query:
        :param sheet_name:
        :param sql:
        :param gather_where_items:
        :return:
        """
        df = self._get_data_with_df(query=query, sql=sql, gather_where_items=gather_where_items)

        query.to_excel(name=sheet_name, df=df)

    @staticmethod
    def _get_data_with_df(query: CommonQuery, sql, gather_where_items):
        """
        [
            { a: 1, b: 3 },
            { a: 1, b: 4 },
            { a: 2, b: 3 },
            { a: 2, b: 4 },
        ]

        :param query:
        :param sql:
        :param gather_where_items:
        :return:
        """
        all_df = None
        for item in gather_where_items:
            exec_sql = sql
            for key, value in item.items():
                exec_sql = exec_sql.replace(f"##{key}##", value)

            df = query.get(name='', sql=exec_sql, is_to_excel=False)

            if df is not None:
                columns = list(df.columns)
                for key, value in item.items():
                    if key not in columns:
                        df[key] = value

            if all_df is None:
                all_df = df
            else:
                all_df = pandas.concat([all_df, df])

        return all_df

    @staticmethod
    def _generate_gather_items_for_items(gather_items):
        """
        {
            a: [ 1 ,2 ],
            b: [ 3, 4 ]
        }

        [
            { a: 1, b: 3 },
            { a: 1, b: 4 },
            { a: 2, b: 3 },
            { a: 2, b: 4 },
        ]
        :param gather_items:
        :return:
        """
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
