import jieba.posseg as posseg

"""
rule = seg_words.SegWordsRule()

"""


class SegWordsRule:
    def main(self):
        pass

    def seg_flag(self, content):
        """
        [(word, flag), ...]

        :param content:
        :return:
        """
        if not content:
            return []

        words_list = []
        words = posseg.cut(content)
        for word, flag in words:
            if len(word) < 2 or len(word.encode('utf-8')) < 3 or flag in ['eng', 'm']:
                continue

            words_list.append((word, flag))

        return words_list

    def seg_words(self, content):
        """
        [word, ...]

        :param content:
        :return:
        """
        words_list = self.seg_flag(content)
        if not words_list:
            return words_list

        words = []
        for item in words_list:
            words.append((item[0]))

        return words
