from lib.workflow.tag import tag
from lib.workflow.rule import split_to_words


def get_split_to_words_tag_insert(key, sep):
    split = split_to_words.SplitToWords(sep=sep)

    def split_to_words_insert(item):
        if key not in item:
            return None

        content = item[key]

        return split.split(content=content)

    return tag.TagInsert(['word'], split_to_words_insert)
