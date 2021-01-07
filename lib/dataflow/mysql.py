from . import process
from ..db import mysql


class DataProvide(process.DataProvider):
    """
    data provider for mysql
    """

    def __init__(self, db: mysql.Mysql, table_name, id_name, fields, item_funcs=None, database_name=None, read_page_size=5000, last_id=0):
        """
        :param db:
        :param table_name: 数据表名
        :param id_name: 数据表主键
        :param fields: select field 列表
        :param item_funcs: [[func, ['field', ...]], ...]
        :param database_name:
        :param read_page_size: 每页数据 size
        :param last_id:
        """

        super().__init__()

        self._db = db  # type: mysql.Mysql
        self._table_name = table_name
        self._id_name = id_name
        self._fields = fields
        self._database_name = database_name
        self._limit = read_page_size
        self._last_id = last_id

        self._generate_item_funcs(item_funcs=item_funcs)

        self._sql = self._generate_sql()

    def next(self):
        sql = self._get_sql(self._last_id)
        items = self._db.get_all(sql=sql)
        if len(items) <= 0:
            return []

        self._last_id = items[-1][self._id_name]

        self._log(f'last id[{self._id_name}]: {self._last_id}')

        return items

    def _get_sql(self, last_id):
        return self._sql % last_id

    def _generate_item_funcs(self, item_funcs):
        if item_funcs:
            for func_item in item_funcs:
                self.add_item_func(func=func_item[0])
                self._fields.extend(func_item[1])

    def _generate_sql(self):
        if self._database_name:
            table_name = self._database_name + '`.`' + self._table_name
        else:
            table_name = self._table_name

        if isinstance(self._fields, str):
            fields = self._fields
        elif isinstance(self._fields, list):
            self._fields = list(set(self._fields))

            fields = '`' + '`, `'.join(self._fields) + '`'
        elif isinstance(self._fields, dict):
            fields_list = []
            for k, v in self._fields.items():
                fields_list.append(f"`{k}` AS `{v}`")
            fields = ', '.join(fields_list)
        else:
            raise Exception("data provide field list is error!")

        sql = f'SELECT {fields} FROM `{table_name}` WHERE `{self._id_name}` > %s ORDER BY `{self._id_name}` ASC LIMIT {self._limit}'

        return sql


class ActionInsert(process.Action):
    """
    action for mysql
    """

    def __init__(self, db, table_name, fields, database_name=None, size=5000, kwargs=None):
        super().__init__(kwargs=kwargs)

        self.db = db
        self.fields = fields
        self.database_name = database_name
        self.table_name = table_name
        self.size = size

    def before_action(self):
        self.db.truncate(table_name=self.table_name, database_name=self.database_name)

    def after_do(self):
        if len(self._items) > self.size:
            self.after_done()

    def after_done(self):
        if self._items:
            self.db.insert_many(table_name=self.table_name, fields=self.fields, data=self._items, database_name=self.database_name)
            self._items = []


class ActionUpdate(process.Action):
    def __init__(self, db, table_name, id_name, database_name=None, size=5000, kwargs=None):
        super().__init__(kwargs=kwargs)

        self.db = db  # type:mysql.Mysql
        self.database_name = database_name
        self.table_name = table_name
        self.id_name = id_name
        self.size = size

    def before_action(self):
        return True

    def after_do(self):
        if len(self._items) > self.size:
            self.after_done()

    def after_done(self):
        if self._items:
            self.db.update_many(table_name=self.table_name, id_name=self.id_name, items=self._items, database_name=self.database_name)
            self._items = []
