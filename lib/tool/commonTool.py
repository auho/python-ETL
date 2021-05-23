import time


class Tool:
    @staticmethod
    def object_to_dict(self):
        d = {}
        for name in dir(self):
            value = getattr(self, name)
            if not name.startswith('__') and not callable(value):
                d[name] = value

        return d

    @staticmethod
    def log(info):
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ' ' + info)


class Fast:
    TYPE_WORDS = 'words'
    TYPE_TAG = 'tag'
    TYPE_RULE = 'rule'

    @staticmethod
    def tag_words_table_name(data_name, content_name):
        return Fast.tag_table_name(data_name=data_name, tag_name=content_name, addition=Fast.TYPE_WORDS)

    @staticmethod
    def tag_table_name(data_name, tag_name, tag_type=None, addition=None):
        if tag_type is None:
            tag_type = Fast.TYPE_TAG

        if addition:
            return f'{tag_type}_{data_name}_{tag_name}_{addition}'
        else:
            return f'{tag_type}_{data_name}_{tag_name}'

    @staticmethod
    def rule_table_name(tag_name, data_name=None):
        if data_name:
            return f'{Fast.TYPE_RULE}_{data_name}_{tag_name}'
        else:
            return f'{Fast.TYPE_RULE}_{tag_name}'
