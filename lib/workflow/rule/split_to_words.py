"""
rule = split_to_words.SplitToWords(sep=',')

"""


class SplitToWords:
    def __init__(self, sep):
        self._sep = sep

    def main(self):
        pass

    def split(self, content):
        """

        :param content:
        :return: [,...]
        """
        all_tag_list = []
        if not content:
            return all_tag_list

        tag_list = content.split(self._sep)
        if not tag_list:
            return []

        for tag in tag_list:
            tag = tag.strip()
            if not tag:
                continue

            all_tag_list.append(tag)

        return all_tag_list
