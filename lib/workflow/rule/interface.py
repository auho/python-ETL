from abc import ABCMeta, abstractmethod


class Tag(metaclass=ABCMeta):
    @abstractmethod
    def main(self):
        pass


class TagCover(Tag):
    @abstractmethod
    def tag_cover(self, name, content):
        """

        :param name:
        :param content:
        :return: dict
        """
        pass


class TagInsert(Tag):
    @abstractmethod
    def tag_insert(self, content):
        """
        返回多条

        :param content:
        :return: [(),...]
        """
        pass

    @abstractmethod
    def get_all_name(self):
        """

        :return: [str,...]
        """
        pass


class TagAppend(Tag):
    @abstractmethod
    def tag_append(self, content):
        """

        :param content:
        :return: dict
        """
        pass
