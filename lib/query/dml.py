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

    def __init__(self, table_name, select=None, where=None, group_fields=None, aggregation_dict=None, group_alias=None, group_alias_map=None,
                 order_dict=None, limit='', select_alias=None, alias=None, table_sql=None):
        """

        :param table_name: left join table name
        :param select: str|dict|list
        :param where: 字符串
        :param group_fields: [key name]
        :param aggregation_dict: {'COUNT(*)': '评论量'}
        :param group_alias: 分组字典 key name:key title
        :param group_alias_map: 分组字典 key name:key title，只作为 alias 替换，不加入 select
        :param order_dict: 排序字典 key name:key sort
        :param limit: str
        :param select_alias: dict
        :param alias: str
        :param table_sql: 直接使用 sql 结果作为表，表名为 table_name

        所有配置中的关键值名称需要带上 `
        *_dict 为配置
        *_list 为实际组成 sql

        """
        self._table_name = table_name
        self._real_table_name = ''
        self._select = select
        self._where = where  # type:str
        self._group_fields = group_fields  # type:list
        self._aggregation_dict = aggregation_dict  # type:dict
        self._group_alias_dict = group_alias
        self._group_alias_map_dict = group_alias_map  # type:dict
        self._order_dict = order_dict  # type:dict
        self.limit = limit
        self._select_alias = select_alias
        self._table_alias = alias
        self.table_sql = table_sql

        self.select_list = []
        self.where_string = ''
        self.group_list = []
        self.aggregation_list = []
        self.order_list = []

        self._check()
        self.parse()

    def _check(self):
        if not self._table_name:
            raise Exception('table name is Error!')

        self._real_table_name = self._table_name
        if self._table_alias:
            self._table_name = self._table_alias

        if not self._select:
            self._select = []

        if not self._group_fields:
            self.group_list = []

        if not self._group_alias_map_dict:
            self._group_alias_map_dict = dict()

    def select(self, select):
        self._parse_select(select=select)

        return self

    def _parse_select_alias(self, select):
        if select is not None:
            new_select = dict()
            for k, v in select.items():
                new_select[f"`{k}`"] = v

            self._parse_select(select=new_select)

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
            if self.where_string:
                self.where_string = ' AND ' + self._parse_field_exp(where, self._table_name)
            else:
                self.where_string = self._parse_field_exp(where, self._table_name)

    def _parse_group(self, group):
        if group:
            for k in group:
                key = f"`{self._table_name}`.`{k}`"

                if k in self._group_alias_map_dict:
                    if k == self._group_alias_map_dict[k]:
                        self.select_list.append(key)
                    else:
                        self.select_list.append(f"{key} AS '{self._group_alias_map_dict[k]}'")
                else:
                    self.select_list.append(key)

                self.group_list.append(key)

    def _parse_group_alias(self, group_alias):
        if group_alias:
            for k, v in group_alias.items():
                key = f"`{self._table_name}`.`{k}`"

                self.select_list.append(f"{key} AS '{v}'")

                self.group_list.append(key)

    def _parse_order(self, order):
        if order:
            for k, v in order.items():
                self.order_list.append(f"`{k}` {v}")

    def parse(self):
        self._parse_select(select=self._select)
        self._parse_select_alias(select=self._select_alias)
        self._parse_where(where=self._where)
        self._parse_group(group=self._group_fields)
        self._parse_group_alias(group_alias=self._group_alias_dict)
        self._parse_select(select=self._aggregation_dict)
        self._parse_order(order=self._order_dict)

    def where_and(self, where):
        self._parse_where(where=where)

    def get_table_name(self):
        return self._table_name

    def get_table_name_dml(self):
        if self.table_sql is None:
            if self._real_table_name == self._table_name:
                table_dml = f"`{self._table_name}`"
            else:
                table_dml = f"`{self._real_table_name}` AS `{self._table_name}`"
        else:
            table_dml = f"({self.table_sql}) AS `{self._table_name}`"

        return table_dml

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
        self._table_list.append(f"FROM {table.get_table_name_dml()}")

        return self

    def table_left(self, table: Table, join_on):
        """
        :param table:
        :param join_on:
            全条件：`t`.`id` = `t1`.`id`
            条件字段：['id'] 上一个表
            条件字段：[['id', 'name']] 上一个表
            条件字段：['left id', 'current id'] 第一个表
            条件字段：['left table', 'left id', 'current id']
            条件字段：['left table', 'left id', 'right table', 'current id']

        """

        self._add_table(table=table)
        left_join_string = f'LEFT JOIN {table.get_table_name_dml()}'
        left_join_on_string = ''

        if type(join_on) == str:
            left_join_on_string = join_on
        if type(join_on) == list and len(join_on) == 1:
            fields = join_on[0]
            if type(fields) == str:
                left_join_on_string = f'`{self._tables[-2].get_table_name()}`.`{fields}` = `{table.get_table_name()}`.`{fields}`'
            elif type(fields) == list:
                left_join_on_list = []
                for field in fields:
                    left_join_on_list.append(f'`{self._tables[-2].get_table_name()}`.`{field}` = `{table.get_table_name()}`.`{field}`')

                left_join_on_string = ' AND '.join(left_join_on_list)
            else:
                raise Exception("join on is error!")
        elif type(join_on) == list and len(join_on) == 2:
            left_join_on_string = f'`{self._tables[0].get_table_name()}`.`{join_on[0]}` = `{table.get_table_name()}`.`{join_on[1]}`'
        elif type(join_on) == list and len(join_on) == 3:
            left_join_on_string = f'`{join_on[0].get_table_name()}`.`{join_on[1]}` = `{table.get_table_name()}`.`{join_on[2]}`'
        elif type(join_on) == list and len(join_on) == 4:
            left_join_on_string = f'`{join_on[0].get_table_name()}`.`{join_on[1]}` = `{join_on[2].get_table_name()}`.`{join_on[3]}`'
        else:
            raise Exception("join on is error!")

        self._table_list.append(left_join_string + ' ON ' + left_join_on_string)

        return self

    def _add_table(self, table):
        self._tables.append(table)

        self.select(table.select_list)
        self.select(table.aggregation_list)
        self.where(table.where_string)
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

        return self

    def _where_string(self):
        if self._where_list:
            return ' WHERE ' + ' AND '.join(self._where_list)
        else:
            return ''

    def group(self, group):
        if group:
            self._group_list.extend(group)

        return self

    def _group_string(self):
        if self._group_list:
            return ' GROUP BY ' + ','.join(self._group_list)
        else:
            return ''

    def order(self, order):
        if order:
            self._order_list.extend(order)
        return self

    def _order_string(self):
        if self._order_list:
            return ' ORDER BY ' + ','.join(self._order_list)
        else:
            return ''

    def limit(self, limit):
        if limit:
            self._limit = limit

        return self

    def _limit_string(self):
        if self._limit:
            return ' LIMIT ' + str(self._limit)
        else:
            return ''

    def sql(self):
        return f"SELECT {self._select_string()} {self._table_string()}" \
               f"{self._where_string()}" \
               f"{self._group_string()}{self._order_string()}{self._limit_string()}"


class TableInsertInto(TableJoin):
    def __init__(self, table_name):
        self._tableName = table_name

        super(TableInsertInto, self).__init__()

    def sql(self):
        fields = []
        for s in self._select_list:
            r = re.sub(r'.+\s+AS\s+\'([^\']+)\'', rf'\1', s)
            if r != s:
                fields.append(r)
                continue

            r = re.sub(r'`[^`]+`\.`([^`]+)`', rf'\1', s)
            if r != s:
                fields.append(r)
                continue

        fields_string = "', '".join(fields)
        fields_string = f"'{fields_string}'"

        return f"INSERT INTO `{self._tableName}` ({fields_string}) " + super().sql()


class TableDelete(TableJoin):
    def sql(self):
        return f"DELETE `{self._tables[0].get_table_name()}` {self._table_string()}" \
               f"{self._where_string()}{self._group_string()}{self._order_string()}{self._limit_string()}"


class TableUpdate(TableJoin):
    def __init__(self):
        super(TableUpdate, self).__init__()
        self._set_list = []
