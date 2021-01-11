from .func import FuncInsert
from lib.workflow.rule import split_to_words


class FuncSplitToWords(FuncInsert):
    def __init__(self, key, sep, table_name):
        self._split = split_to_words.SplitToWordsRule(sep=sep)
        self._key = key
        self._table_name = table_name

    def get_table_name(self):
        return self._table_name

    def get_fields(self):
        return [self._key]

    def get_keys(self):
        return ['word']

    def insert(self, item):
        if self._key not in item:
            return None

        content = item[self._key]

        words = self._split.split(content=content)
        if not words:
            return None

        return [(x,) for x in words]
