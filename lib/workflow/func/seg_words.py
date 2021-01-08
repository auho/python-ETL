from .func import FuncInsert
from lib.workflow.rule import seg_words


class FuncSegWords(FuncInsert):
    def __init__(self, key):
        self._segWords = seg_words.SegWordsRule()
        self._key = key

    def get_fields(self):
        return [self._key]

    def get_keys(self):
        return ['word', 'flag']

    def insert(self, item):
        if self._key not in item:
            return None

        content = item[self._key]

        return self._segWords.seg_flag(content=content)
