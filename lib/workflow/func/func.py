from abc import ABCMeta, abstractmethod

"""
fields 查询字段
keys 插入字段
"""


class Func(metaclass=ABCMeta):
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
    def insert(self, item):
        pass

    @abstractmethod
    def get_keys(self):
        pass

    @abstractmethod
    def get_fields(self):
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
    def do(self, item):
        pass

    @abstractmethod
    def get_keys(self):
        pass

    @abstractmethod
    def get_fields(self):
        pass
