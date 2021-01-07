from lib.workflow.tag import tag
from lib.workflow.rule import seg_words


def get_seg_words_flag_tag_insert(key):
    seg = seg_words.SegWordsRule()

    def seg_words_flag_insert(item):
        if key not in item:
            return None

        content = item[key]

        return seg.seg_flag(content=content)

    return tag.TagInsert(['word', 'flag'], seg_words_flag_insert)
