from abc import ABCMeta, abstractmethod


class TableDDl(metaclass=ABCMeta):
    @abstractmethod
    def get_table_name(self):
        pass

    @abstractmethod
    def build(self, db):
        pass
