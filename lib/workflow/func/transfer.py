from .func import FuncTransfer


class FunClean(FuncTransfer):
    def __init__(self, rule):
        self._rule = rule

    def get_keys(self):
        return []

    def transfer(self, item):
        if self._rule.tag_clean(item):
            return None

        return item


class FuncDefaultTransfer(FuncTransfer):
    def __init__(self):
        pass

    def get_keys(self):
        return []

    def transfer(self, item):
        return item
