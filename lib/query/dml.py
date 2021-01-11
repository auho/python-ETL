import re


class Model:
    @staticmethod
    def _parse_limit(query):
        if query.debug:
            return ' LIMIT 100'
        else:
            return ''

    @staticmethod
    def _parse_field_exp(field_exp, prefix):
        return re.sub(r'(`\w)', rf'`{prefix}`.\1', field_exp)


class Table(Model):
    """
    保存格式化 table 信息，方便组成 sql
    """

    def __init__(self, table_name, select=None, where=None, group_fields=None, aggregation_dict=None, group_dict=None, order_dict=None, limit='',
                 join_on=None, table_sql=None):
        """

        :param table_name: left join table name
        :param where: 字符串
        :param group_fields: [key name]
        :param aggregation_dict: {'COUNT(*)': '评论量'}
        :param group_dict: 分组字典 key name:key title
        :param order_dict: 分组字典 key name:key sort
        :param join_on: 全条件 `theme`.`theme_id` = `comment`.`theme_id`
        :param table_sql: 直接使用 sql 结果作为表，表名为 table_name

        所有配置中的关键值名称需要带上 `
        *_dict 为配置
        *_list 为实际组成 sql

        """
        self._table_name = table_name
        self._select = select
        self.where = where  # type:str
        self._group_fields = group_fields  # type:list
        self._aggregation_dict = aggregation_dict  # type:dict
        self._group_dict = group_dict  # type:dict
        self._order_dict = order_dict  # type:dict
        self.limit = limit
        self.join_on = join_on
        self.table_sql = table_sql

        self.select_list = []
        self.group_list = []
        self.aggregation_list = []
        self.order_list = []

        self._check()
        self.parse()

    def _check(self):
        if not self._table_name:
            raise Exception('table name is Error!')

        if not self._select:
            self._select = []

        if not self._group_fields:
            self.group_list = []

        if not self._group_dict:
            self._group_dict = dict()

    def parse(self):
        if self._select:
            for select in self._select:
                self.select_list.append(f"`{self._table_name}`.`{select}`")

        if self._group_fields is not None:
            for k in self._group_fields:
                key = f"`{self._table_name}`.`{k}`"

                if k in self._group_dict:
                    self.select_list.append(f"{key} AS '{self._group_dict[k]}'")
                else:
                    self.select_list.append(f"{key}")

                self.group_list.append(key)

        if self._aggregation_dict is not None:
            for k, v in self._aggregation_dict.items():
                key = self._parse_field_exp(k, self._table_name)
                self.aggregation_list.append(f"{key} AS '{v}'")

        if self._order_dict is not None:
            for k, v in self._order_dict.items():
                self.order_list.append(f"`{k}` {v}")

    def get_table_name(self):
        if self.table_sql is None:
            table_name = f"`{self._table_name}`"
        else:
            table_name = f"({self.table_sql}) AS `{self._table_name}`"

        return table_name


class TableJoin(Model):
    """
    把多个 TableInfo 按照 left join 的格式拼接成 sql
    """

    def __init__(self):
        self._tables = []
        self._select_list = []
        self._group_list = []
        self._aggregation_list = []
        self._order_list = []
        self._table_list = []
        self._where_list = []
        self._limit = ''

    def table(self, table: Table):
        self._add_table(table=table)
        self._table_list.append(f"FROM {table.get_table_name()}")

        return self

    def table_left(self, table: Table):
        self._add_table(table=table)
        self._table_list.append(f"LEFT JOIN {table.get_table_name()} ON {table.join_on}")

        return self

    def _add_table(self, table):
        self._tables.append(table)

        self.select(table.select_list)
        self.select(table.aggregation_list)
        self.where(table.where)
        self.group(table.group_list)
        self.order(table.order_list)
        self.limit(table.limit)

    def select(self, select):
        if select:
            self._select_list.extend(select)

    def _select_string(self):
        if self._select_list:
            return ','.join(self._select_list)
        else:
            return '*'

    def _table_string(self):
        return ' '.join(self._table_list)

    def where(self, where):
        if where:
            self._where_list.append(where)

    def _where_string(self):
        if self._where_list:
            return ' WHERE ' + ' AND '.join(self._where_list)
        else:
            return ''

    def group(self, group):
        if group:
            self._group_list.extend(group)

    def _group_string(self):
        if self._group_list:
            return ' GROUP BY ' + ','.join(self._group_list)
        else:
            return ''

    def order(self, order):
        if order:
            self._order_list.extend(order)

    def _order_string(self):
        if self._order_list:
            return ' ORDER BY ' + ','.join(self._order_list)
        else:
            return ''

    def limit(self, limit):
        if limit:
            self._limit = limit

    def _limit_string(self):
        if self._limit:
            return ' LIMIT ' + self._limit
        else:
            return ''

    def sql(self):
        return f"SELECT {self._select_string()} {self._table_string()}" \
               f"{self._where_string()}" \
               f"{self._group_string()}{self._order_string()}{self._limit_string()}"
