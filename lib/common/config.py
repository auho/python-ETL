import argparse
import yaml

from lib.db import mysql

parser = argparse.ArgumentParser()
parser.add_argument("--config_file", help="config file name", type=str, required=True)
input_args = parser.parse_args()


class PartConfig:
    def __init__(self, base_path):
        self._mysqlDbConf = {}
        self._yamlConfig = None
        self._basePath = base_path

    def parse(self, conf_file_name):
        self._init_yaml(conf_file_name)

        self._mysqlDbConf = self.get('mysql')

    def get(self, name):
        return self._yamlConfig[name]

    def _init_yaml(self, conf_file_name):
        print(f'config file: {conf_file_name}')

        yaml_file = self._basePath + f"/conf/db_{conf_file_name}"
        f = open(yaml_file, 'r', encoding='utf-8')
        yaml_content = f.read()
        self._yamlConfig = yaml.safe_load(yaml_content)

    def init_all(self, conf_file_name):
        conf_file_name = f'{conf_file_name}.yml'
        self.parse(conf_file_name)

    @property
    def mysqlDbConf(self):
        return self._mysqlDbConf


class Config:
    basePath = None
    mysqlDb = None
    mysqlDbConf = None
    DEBUG = True
    ENV_DEBUG = False

    def __init__(self, base_path):
        part_conf = PartConfig(base_path=base_path)  # type:PartConfig
        part_conf.init_all(input_args.config_file)

        self.basePath = base_path

        self.mysqlDbConf = part_conf.mysqlDbConf
        self.mysqlDb = mysql.Mysql(self.mysqlDbConf)  # type: mysql.Mysql
        self.mysqlDb.connect()

        self.DEBUG = bool(part_conf.get('debug'))

        self.ENV_DEBUG = False
        if part_conf.get('env') == 'dev':
            self.ENV_DEBUG = True
