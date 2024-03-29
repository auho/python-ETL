from abc import ABCMeta, abstractmethod
from lib.workflow.tool.item import get_content_from_item_by_keys

"""
fields 查询字段
keys 插入字段
"""


class Func(metaclass=ABCMeta):
    @staticmethod
    def _get_content(keys, item):
        return get_content_from_item_by_keys(keys=keys, item=item)


class FuncEmpty(Func):
    @abstractmethod
    def do(self):
        pass


class FuncVoid(Func):
    @abstractmethod
    def get_fields(self):
        pass

    @abstractmethod
    def do(self, item):
        pass


class FuncTransfer(Func):
    @abstractmethod
    def get_keys(self):
        pass

    @abstractmethod
    def transfer(self, item):
        pass


class FuncInsert(Func):
    @abstractmethod
    def get_keys(self):
        pass

    @abstractmethod
    def get_fields(self):
        pass

    @abstractmethod
    def insert(self, item):
        pass


class FuncUpdate(Func):
    @abstractmethod
    def get_fields(self):
        pass

    @abstractmethod
    def update(self, item):
        pass


class FuncMulti(Func):
    @abstractmethod
    def get_keys(self):
        pass

    @abstractmethod
    def get_fields(self):
        pass

    @abstractmethod
    def do(self, item):
        pass
