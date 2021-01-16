import argparse
import yaml
import sys
from lib.db import mysql

parser = argparse.ArgumentParser()
parser.add_argument("--config", help="config file name", type=str, required=True)
input_args = parser.parse_args()


class PartConfig:
    def __init__(self):
        self._mysqlDbConf = {}
        self._yamlConfig = None

    def parse(self, conf_name, module_path):
        self._parse_yaml(conf_name, module_path)
        self._mysqlDbConf = self.get('mysql')

    def get(self, name):
        return self._yamlConfig[name]

    def _parse_yaml(self, conf_name, module_path):
        yaml_file = module_path + f"/conf/db_{conf_name}.yml"
        f = open(yaml_file, 'r', encoding='utf-8')
        yaml_content = f.read()
        self._yamlConfig = yaml.safe_load(yaml_content)

    @property
    def mysql_db_conf(self):
        return self._mysqlDbConf


class App:
    modulePath = None
    configName = None
    mysqlDb = None
    mysqlDbConf = None
    ENV = 'dev'
    DEBUG = True
    ENV_DEBUG = False

    def __init__(self, module_path):
        self.modulePath = module_path
        self.configName = input_args.config

        part_conf = PartConfig()  # type:PartConfig
        part_conf.parse(conf_name=input_args.config, module_path=module_path)

        self.mysqlDbConf = part_conf.mysql_db_conf
        self.mysqlDb = mysql.Mysql(self.mysqlDbConf)  # type: mysql.Mysql
        self.mysqlDb.connect()

        self.DEBUG = bool(part_conf.get('debug'))
        self.ENV = part_conf.get('env')
        if self.ENV == 'dev':
            self.ENV_DEBUG = True

        self._init_info()

    def get_data_file_path(self, file):
        return self.get_data_path() + '/' + file

    def get_data_path(self):
        return self.modulePath + '/data'

    def get_conf_path(self):
        return self.modulePath + '/conf'

    def _init_info(self):
        print("=" * 50)
        print("=" * 2 + f" MODULE PATH:: {self.modulePath}")
        print("=" * 2 + f" FILE PATH:: {sys.argv}")
        print(f" config file: {self.configName}")
        print(f" db:: {self.mysqlDbConf['db']}")
        print(f" debug:: {str(int(self.DEBUG))}")
        print(f" env_debug:: {str(int(self.ENV_DEBUG))}")
        print("=" * 50)
        print("\n")
