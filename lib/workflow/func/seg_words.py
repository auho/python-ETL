from .func import FuncInsert
from lib.workflow.rule import seg_words


class FuncSegWords(FuncInsert):
    def __init__(self, key, table_name):
        self._segWords = seg_words.SegWordsRule()
        self._key = key
        self._table_name = table_name

    def get_table_name(self):
        return self._table_name

    def get_fields(self):
        return [self._key]

    def get_keys(self):
        return ['word', 'flag']

    def insert(self, item):
        if self._key not in item:
            return None

        content = item[self._key]

        return self._segWords.seg_flag(content=content)
