from .func import FuncTransfer


class FunClean(FuncTransfer):
    def __init__(self, key, rule):
        self._rule = rule
        self._key = key

    def get_keys(self):
        return []

    def transfer(self, item):
        if self._rule.tag_clean(item):
            return None

        return item
