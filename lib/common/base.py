import os
import sys
from . import config
from lib.query import common

FILE = os.path.abspath(sys.argv[0])
FILE_PATH = os.path.dirname(FILE)
MODULE_PATH = os.path.dirname(FILE_PATH)

__file_path = os.path.abspath(__file__)
__common_path = os.path.dirname(__file_path)
LIB_PATH = os.path.dirname(__common_path)
ROOT_PATH = os.path.dirname(LIB_PATH)

APP = config.Config(MODULE_PATH)

QUERY = common.CommonQuery(config=config)
