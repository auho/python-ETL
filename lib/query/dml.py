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
        return re.sub(r'([^.])(`[^`.]+`)([^.])', rf'\1`{prefix}`.\2\3', f" {field_exp} ").strip()


class Table(Model):
    """
    保存格式化 table 信息，方便组成 sql
    """

    def __init__(self, table_name, select=None, where=None, group_fields=None, aggregation_dict=None, group_alias=None, order_dict=None, limit='',
                 table_sql=None):
        """

        :param table_name: left join table name
        :param select: str|dict|list
        :param where: 字符串
        :param group_fields: [key name]
        :param aggregation_dict: {'COUNT(*)': '评论量'}
        :param group_alias: 分组字典 key name:key title
        :param order_dict: 分组字典 key name:key sort
        :param limit: str
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
        self._group_alias_dict = group_alias  # type:dict
        self._order_dict = order_dict  # type:dict
        self.limit = limit
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

        if not self._group_alias_dict:
            self._group_alias_dict = dict()

    def _parse_select(self, select):
        if select:
            if type(select) == str:
                if select == '*':
                    self.select_list.append(f"`{self._table_name}`.*")
                else:
                    self.select_list.append(self._parse_field_exp(select, self._table_name))
            elif type(select) == dict:
                for k, v in select.items():
                    self.select_list.append(f"{self._parse_field_exp(k, self._table_name)} AS '{v}'")
            elif type(select) == list:
                for s in select:
                    if s == '*':
                        self.select_list.append(f"`{self._table_name}`.*")
                    else:
                        self.select_list.append(self._parse_field_exp(f"`{s}`", self._table_name))
            else:
                print(select)
                raise Exception(f"{self._table_name} select is error!")

    def _parse_where(self, where):
        if where:
            if self.where:
                self.where = self._parse_field_exp(where, self._table_name)
            else:
                self.where = ' AND ' + self._parse_field_exp(where, self._table_name)

    def _parse_group(self, group):
        if group:
            for k in group:
                key = f"`{self._table_name}`.`{k}`"

                if k in self._group_alias_dict:
                    if k == self._group_alias_dict[k]:
                        self.select_list.append(key)
                    else:
                        self.select_list.append(f"{key} AS '{self._group_alias_dict[k]}'")
                else:
                    self.select_list.append(key)

                self.group_list.append(key)

    def _parse_order(self, order):
        if order:
            for k, v in order.items():
                self.order_list.append(f"`{k}` {v}")

    def parse(self):
        self._parse_select(select=self._select)
        self._parse_where(where=self.where)
        self._parse_group(group=self._group_fields)
        self._parse_select(select=self._aggregation_dict)
        self._parse_order(order=self._order_dict)

    def where_and(self, where):
        self._parse_where(where=where)

    def get_table_name(self):
        if self.table_sql is None:
            table_name = f"`{self._table_name}`"
        else:
            table_name = f"({self.table_sql}) AS `{self._table_name}`"

        return table_name

    def sql(self):
        return TableJoin().table(table=self).sql()


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

    def table_left(self, table: Table, join_on):
        """
        :param table:
        :param join_on: 全条件 `t`.`id` = `t1`.`id`
        """

        self._add_table(table=table)
        self._table_list.append(f"LEFT JOIN {table.get_table_name()} ON {join_on}")

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
            if type(select) == str:
                self._select_list.append(select)
            elif type(select) == list:
                self._select_list.extend(select)
            elif type(select) == dict:
                for k, v in select.items():
                    self._select_list.append(f"{k} AS {v}")
            else:
                raise Exception("table join select is error")

        return self

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
