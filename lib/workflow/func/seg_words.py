from lib.workflow.rule import seg_words, fun_insert


def get_seg_words_flag_insert_func():
    seg = seg_words.SegWordsRule()

    def seg_words_flag_insert(content):
        return seg.seg_flag(content=content)

    return ['word', 'flag'], seg_words_flag_insert


def get_seg_words_flag_insert_rule():
    return fun_insert.TagRule(*get_seg_words_flag_insert_func())
