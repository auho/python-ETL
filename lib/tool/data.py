from lib.tool.commonTool import Fast


class CommonModel:
    @staticmethod
    def rule_table_name(tag_name):
        return Fast.rule_table_name(tag_name=tag_name)


class DataModel:
    def __init__(self, data_name):
        self._name = data_name

    def tag_table_name(self, tag_name, addition=None):
        return Fast.tag_table_name(data_name=self._name, tag_name=tag_name, addition=addition)

    def tag_words_table_name(self, content_name):
        return Fast.tag_words_table_name(data_name=self._name, content_name=content_name)

    def rule_table_name(self, tag_name):
        return Fast.rule_table_name(tag_name=tag_name, data_name=self._name)
