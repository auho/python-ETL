from .model import Model


class TableInfo(Model):
    """
    保存格式化 table 信息，方便组成 sql
    """

    def __init__(self, table_name, group_dict=None, join_on=None, aggregation_dict=None, where=None, table_sql=None):
        """

        :param table_name: left join table name
        :param group_dict: 分组字典 key name:key title
        :param join_on: 全条件 `theme`.`theme_id` = `comment`.`theme_id`
        :param aggregation_dict: {'COUNT(*)': '评论量'}
        :param where:
        :param table_sql: 直接使用 sql 结果作为表，表名为 table_name

        所有配置中的关键值名称需要带上 `
        *_dict 为配置
        *_list 为实际组成 sql

        """
        self.table_name = table_name
        self.table_sql = table_sql
        self.where = where
        self.group_dict = group_dict
        self.join_on = join_on
        self.aggregation_dict = aggregation_dict

        self.select_list = []
        self.group_list = []
        self.aggregation_list = []

        self._check()
        self.parse()

    def _check(self):
        if not self.table_name:
            raise Exception('table name is Error!')

        if self.join_on is None:
            raise Exception('join on is Error!')

    def parse(self):
        if self.group_dict is not None:
            for k, v in self.group_dict.items():
                key = f"`{self.table_name}`.`{k}`"

                if k == v:
                    self.select_list.append(f"{key}")
                else:
                    self.select_list.append(f"{key} AS '{v}'")

                self.group_list.append(key)

        if self.aggregation_dict is not None:
            for k, v in self.aggregation_dict.items():
                key = self._parse_field_exp(k, self.table_name)
                self.aggregation_list.append(f"{key} AS '{v}'")


class ModelJoin(Model):
    """
    把多个 TableInfo 按照 left join 的格式拼接成 sql
    """

    @staticmethod
    def join(table_info_list):
        """

        :param table_info_list: 按照 sql 结构的 TableInfo list
        :return:

        """
        select_list = []
        group_list = []
        aggregation_list = []
        table_list = []
        where_list = []

        for index, Table_info in enumerate(table_info_list):  # type:int,Table_info
            if Table_info.select_list:
                select_list = select_list + Table_info.select_list

            if Table_info.group_list:
                group_list = group_list + Table_info.group_list

            if Table_info.aggregation_list:
                aggregation_list = aggregation_list + Table_info.aggregation_list

            if Table_info.where:
                where_list.append(Table_info.where)

            table_name = ''
            if Table_info.table_sql is None:
                table_name = f"`{Table_info.table_name}`"
            else:
                table_name = f"({Table_info.table_sql}) AS `{Table_info.table_name}`"

            if index == 0:
                table_list.append(f"FROM {table_name}")
            else:
                table_list.append(f"LEFT JOIN {table_name} ON {Table_info.join_on}")

        all_select_list = select_list + aggregation_list

        all_select_string = ','.join(all_select_list)
        table_string = ' '.join(table_list)

        where_string = ''
        if where_list:
            where_string = ' WHERE ' + ' AND '.join(where_list)

        group_sting = ''
        if group_list:
            group_sting = ' GROUP BY ' + ','.join(group_list)

        return f"SELECT {all_select_string} {table_string}{where_string}{group_sting}"
