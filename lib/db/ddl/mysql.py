T_Id = 'id'
T_STRING = 'string'
T_INT = 'int'


class DDLBuild:
    def __init__(self, table_name):
        self._sqlList = []
        self._tableName = table_name
        self._DDL = DDLGenerate()

    def add_table(self):
        sql = self._DDL.create(table_name=self._tableName)
        self._add_sql(sql=sql)

    def add_field(self, t, name):
        method_name = 'add_' + t
        method = getattr(self, method_name)
        method(name=name)

    def add_id(self, name, length=20):
        sql = self._DDL.alter_id(table_name=self._tableName, name=name, length=length)
        self._add_sql(sql=sql)

    def add_string(self, name, length=30):
        sql = self._DDL.alter_string(table_name=self._tableName, name=name, length=length)
        self._add_sql(sql=sql)

    def add_int(self, name, length=11):
        sql = self._DDL.alter_int(table_name=self._tableName, name=name, length=length)
        self._add_sql(sql=sql)

    def add_decimal(self, name, m=10, d=0):
        sql = self._DDL.alter_decimal(table_name=self._tableName, name=name, m=m, d=d)
        self._add_sql(sql=sql)

    def add_index(self, name):
        sql = self._DDL.alter_index(table_name=self._tableName, name=name)
        self._add_sql(sql=sql)

    def add_unique_index(self, name):
        sql = self._DDL.alter_unique_index(table_name=self._tableName, name=name)
        self._add_sql(sql=sql)

    def build(self, db):
        pass

    def _execute_ddl(self, db):
        for sql in self._sqlList:
            try:
                db.execute(sql=sql)
            except Exception as e:
                print(sql)
                print(e)

    def _add_sql(self, sql):
        self._sqlList.append(sql)


class DDLCreate(DDLBuild):
    def __init__(self, table_name):
        super().__init__(table_name=table_name)

        self.add_table()

    def build(self, db):
        db.execute(sql=f'DROP TABLE IF EXISTS `{self._tableName}`')

        self._execute_ddl(db=db)


class DDLAlter(DDLBuild):
    def __init__(self, table_name):
        super().__init__(table_name=table_name)

    def build(self, db):
        self._execute_ddl(db=db)


class DDLGenerate:
    @staticmethod
    def create(table_name):
        return f"CREATE TABLE `{table_name}` (id INT(11) UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT) ENGINE = `MyISAM`"

    @staticmethod
    def alter_id(table_name, name, length=20):
        return f"ALTER TABLE `{table_name}` ADD `{name}` BIGINT({length})  NOT NULL DEFAULT 0"

    @staticmethod
    def alter_int(table_name, name, length=11):
        return f"ALTER TABLE `{table_name}` ADD `{name}` INT({length})  NOT NULL DEFAULT 0"

    @staticmethod
    def alter_string(table_name, name, length):
        return f"ALTER TABLE `{table_name}` ADD `{name}` VARCHAR({length})  NOT NULL  DEFAULT ''"

    @staticmethod
    def alter_decimal(table_name, name, m, d):
        return f"ALTER TABLE `{table_name}` ADD `{name}` DECIMAL({m},{d}) NOT NULL DEFAULT 0"

    @staticmethod
    def alter_index(table_name, name):
        return f"ALTER TABLE `{table_name}` ADD INDEX (`{name}`)"

    @staticmethod
    def alter_unique_index(table_name, name):
        return f"ALTER TABLE `{table_name}` ADD UNIQUE INDEX (`{name}`)"
