from .func import FuncInsert, FuncUpdate


class FuncTagInsert(FuncInsert):
    def __init__(self, key, rule):
        self._rule = rule
        self._key = key

    def get_fields(self):
        return [self._key]

    def get_keys(self):
        return self._rule.get_keys()

    def insert(self, item):
        if self._key not in item:
            return None

        content = item[self._key]

        return self._rule.tag_insert(content=content)


class FuncTagUpdate(FuncUpdate):
    def __init__(self, key, rules):
        self._rules = rules
        self._key = key

    def get_fields(self):
        return [self._key]

    def update(self, item):
        if self._key not in item:
            return None

        content = item[self._key]

        update_item = dict()
        for rule in self._rules:
            u = rule.tag_update(content=content)
            if u:
                update_item.update(u)

        return update_item
