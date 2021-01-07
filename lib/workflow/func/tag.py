from lib.workflow.tag import tag
from lib.workflow.rule import tag_multi, tag_sole


def get_tag_multi_insert(key, rule: tag_multi.TagRule):
    def tag_multi_insert(item):
        if key not in item:
            return None

        content = item[key]

        return rule.tag_insert(content=content)

    return tag.TagInsert(rule.get_keys(), tag_multi_insert)


def get_tag_insert(key, rule: tag_sole.TagRule):
    def tag_insert(item):
        if key not in item:
            return None

        content = item[key]

        return rule.tag_insert(content=content)

    return tag.TagInsert(rule.get_keys(), tag_insert)


def get_tag_update(key, rule: tag_sole.TagRule):
    def tag_update(item):
        if key not in item:
            return None

        content = item[key]

        return rule.tag_update(content=content)

    return tag.TagInsert(rule.get_keys(), tag_update)
