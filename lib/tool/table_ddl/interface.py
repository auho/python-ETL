from abc import ABCMeta, abstractmethod


class TableDDl(metaclass=ABCMeta):
    @abstractmethod
    def build(self, db):
        pass
