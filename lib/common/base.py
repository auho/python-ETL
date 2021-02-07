import os
import sys
from . import app
from lib.query import common

FILE = os.path.abspath(sys.argv[0])
FILE_PATH = os.path.dirname(FILE)
MODULE_PATH = FILE_PATH

is_module_path = False
while not is_module_path:
    module_lib_path = MODULE_PATH + '/lib'
    module_conf_path = MODULE_PATH + '/conf'
    if os.path.isdir(module_lib_path) and os.path.isdir(module_conf_path):
        is_module_path = True
    else:
        MODULE_PATH = os.path.dirname(MODULE_PATH)

__file_path = os.path.abspath(__file__)
__common_path = os.path.dirname(__file_path)
LIB_PATH = os.path.dirname(__common_path)
ROOT_PATH = os.path.dirname(LIB_PATH)


def new_app():
    return app.App(MODULE_PATH, ROOT_PATH)


def new_query():
    return common.CommonQuery(app=new_app())


APP = new_app()
APP.log()

QUERY = new_query()
