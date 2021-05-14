from abc import ABCMeta, abstractmethod
from lib.db.mysql import Mysql

T_Id = 'id'
T_STRING = 'string'
T_INT = 'int'


class DDLBuild(metaclass=ABCMeta):
    def __init__(self, table_name, database_name=None):
        self._sqlList = []
        self._tableName = table_name
        self._databaseName = database_name
        self._DDL = DDLGenerate()

    def add_table(self):
        sql = self._DDL.create(table_name=self._tableName)
        self._add_sql(sql=sql)

    def add_field(self, t, name):
        method_name = 'add_' + t
        method = getattr(self, method_name)
        method(name=name)

    def alter_primary_id(self, name, length=11):
        sql = self._DDL.alter_primary_id(table_name=self._tableName, name=name, length=length)
        self._add_sql(sql=sql)

    def add_id(self, name, length=20, default=0, is_index=False):
        sql = self._DDL.alter_id(table_name=self._tableName, name=name, length=length, default=default)
        self._add_sql(sql=sql)

        if is_index:
            self.add_index(name=name)

    def add_string(self, name, length=30, default='', is_index=False):
        sql = self._DDL.alter_string(table_name=self._tableName, name=name, length=length, default=default)
        self._add_sql(sql=sql)

        if is_index:
            self.add_index(name=name)

    def add_int(self, name, length=11, default=0, is_index=False):
        sql = self._DDL.alter_int(table_name=self._tableName, name=name, length=length, default=default)
        self._add_sql(sql=sql)

        if is_index:
            self.add_index(name=name)

    def add_decimal(self, name, m=10, d=0, default=0):
        sql = self._DDL.alter_decimal(table_name=self._tableName, name=name, m=m, d=d, default=default)
        self._add_sql(sql=sql)

    def add_text(self, name):
        sql = self._DDL.alter_text(table_name=self._tableName, name=name)
        self._add_sql(sql=sql)

    def add_index(self, name):
        sql = self._DDL.alter_index(table_name=self._tableName, name=name)
        self._add_sql(sql=sql)

    def add_unique_index(self, name):
        sql = self._DDL.alter_unique_index(table_name=self._tableName, name=name)
        self._add_sql(sql=sql)

    @abstractmethod
    def build(self, db):
        pass

    def _execute_ddl(self, db):
        for sql in self._sqlList:
            try:
                db.execute(sql=sql)
            except Exception as e:
                print(e)

    def _add_sql(self, sql):
        self._sqlList.append(sql)


class DDLCreate(DDLBuild):
    def __init__(self, table_name, database_name=None):
        super().__init__(table_name=table_name, database_name=database_name)

        self.add_table()

    def build(self, db: Mysql):
        db.drop(table_name=self._tableName)

        self._execute_ddl(db=db)


class DDLAlter(DDLBuild):
    def __init__(self, table_name, database_name=None):
        super().__init__(table_name=table_name, database_name=database_name)

    def build(self, db):
        self._execute_ddl(db=db)


class DDLGenerate:
    @staticmethod
    def create(table_name):
        return f"CREATE TABLE `{table_name}` (id INT(11) UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT) ENGINE = `MyISAM` DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_general_ci"

    @staticmethod
    def alter_primary_id(table_name, name, length=20):
        return f"ALTER TABLE `{table_name}` ADD `{name}` BIGINT({length}) UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT FIRST"

    @staticmethod
    def alter_id(table_name, name, length=20, default=0):
        return f"ALTER TABLE `{table_name}` ADD `{name}` BIGINT({length}) NOT NULL DEFAULT {default}"

    @staticmethod
    def alter_int(table_name, name, length=11, default=0):
        return f"ALTER TABLE `{table_name}` ADD `{name}` INT({length}) NOT NULL DEFAULT {default}"

    @staticmethod
    def alter_string(table_name, name, length, default=''):
        return f"ALTER TABLE `{table_name}` ADD `{name}` VARCHAR({length}) NOT NULL  DEFAULT '{default}'"

    @staticmethod
    def alter_decimal(table_name, name, m, d, default=0):
        return f"ALTER TABLE `{table_name}` ADD `{name}` DECIMAL({m},{d}) NOT NULL DEFAULT {default}"

    @staticmethod
    def alter_text(table_name, name):
        return f"ALTER TABLE `{table_name}` ADD `{name}` text"

    @staticmethod
    def alter_index(table_name, name):
        return f"ALTER TABLE `{table_name}` ADD INDEX (`{name}`)"

    @staticmethod
    def alter_unique_index(table_name, name):
        return f"ALTER TABLE `{table_name}` ADD UNIQUE INDEX (`{name}`)"
