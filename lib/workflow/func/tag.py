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


class FuncTagInsertMultiField(FuncInsert):
    def __init__(self, keys, rule):
        self._rule = rule
        self._keys = keys

    def get_keys(self):
        return self._rule.get_keys()

    def get_fields(self):
        return self._keys

    def insert(self, item):
        return self._func_insert(item=item)

    def _func_insert(self, item):
        content = ''
        for key in self._keys:
            if key in item:
                if item[key] is None or item[key] == '':
                    continue

                content += ' ' + item[key]

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


class FuncTagInsertTogether(FuncInsert):
    def __init__(self, key, rules):
        self._key = key
        self._rules = rules

    def get_keys(self):
        return self._rules[0].get_keys()

    def get_fields(self):
        return [self._key]

    def insert(self, item):
        if self._key not in item:
            return None

        content = item[self._key]

        tags_items = []
        for rule in self._rules:
            tags = rule.tag_insert(content=content)
            if tags:
                tags_items.extend(tags)

        return tags_items


class FuncTagsInsertSideBySide(FuncInsert):
    def __init__(self, key, rules):
        self._key = key
        self._rules = rules
        self._tags_keys = []
        self._rules_null_tags = []

        for rule in self._rules:
            self._tags_keys.extend(rule.get_keys())
            self._rules_null_tags.append(('',) * len(rule.get_keys()))

    def get_keys(self):
        return self._tags_keys

    def get_fields(self):
        return [self._key]

    def insert(self, item):
        if self._key not in item:
            return None

        content = item[self._key]

        all_tags_items = []
        tags_items = []
        for rule_index, rule in enumerate(self._rules):
            all_tags_items = []
            rule_tags_items = rule.tag_insert(content=content)
            if not rule_tags_items:
                rule_tags_items = [self._rules_null_tags[rule_index]]

            if tags_items:
                for all_tags_item_index, all_tags_item in enumerate(tags_items):
                    for rule_tag_item_index, rule_tag_item in enumerate(rule_tags_items):
                        all_tags_items.append(all_tags_item + rule_tag_item)
            else:
                all_tags_items = rule_tags_items

            tags_items = all_tags_items.copy()

        return all_tags_items
