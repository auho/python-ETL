from abc import ABCMeta, abstractmethod


class Tag(metaclass=ABCMeta):
    @abstractmethod
    def main(self):
        pass


class TagInsert(Tag):
    @abstractmethod
    def tag_insert(self, item):
        """
        返回多条

        :param item:
        :return: [(),...]
        """
        pass

    @abstractmethod
    def get_keys(self):
        """

        :return: [str,...]
        """
        pass


class TagUpdate(Tag):
    @abstractmethod
    def tag_update(self, item):
        """

        :param item:
        :return: dict
        """
        pass
