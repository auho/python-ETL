import re


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
