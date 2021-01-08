from abc import ABCMeta, abstractmethod


class Func(metaclass=ABCMeta):
    @abstractmethod
    def get_fields(self):
        pass


class FuncInsert(Func):
    @abstractmethod
    def insert(self, item):
        pass

    @abstractmethod
    def get_keys(self):
        pass


class FuncUpdate(Func):
    @abstractmethod
    def update(self, item):
        pass
