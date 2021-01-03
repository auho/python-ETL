from lib.dataflow import mysql
from ..rule import seg_words

"""
a = Action(keyid, content_name, db, table_name, fields)
a.add_rule(seg_rule)

"""


class Action(mysql.ActionInsert):
    def __init__(self, db, table_name, keyid, content_name, database_name=None, size=10000, kwargs=None, addition_fields=None):
        super().__init__(db=db, table_name=table_name, fields=[], database_name=database_name, size=size, kwargs=kwargs)

        self._segRule = None  # type:seg_words.SegWordsRule
        self._contentName = content_name
        self._keyid = keyid
        self._additionFields = addition_fields

    def init_action(self):
        if not self._segRule:
            self._segRule = seg_words.SegWordsRule()

        self._segRule.main()

        fields = [self._keyid] + self._segRule.get_all_name()
        if self._additionFields:
            fields = fields + self._additionFields

        self.fields = fields

    def add_rule(self, seg_rule):
        if seg_rule:
            self._segRule = seg_rule

    def do(self, item):
        if self._contentName not in item:
            return

        words = self._segRule.seg_words(item[self._contentName])
        if not words:
            return

        seg_item = ()
        for word_item in words:
            seg_item = (item[self._keyid], word_item, '', 1)

            if self._additionFields is not None:
                for field in self._additionFields:
                    seg_item = seg_item + (item[field],)

            self.add_item(seg_item)

    def do_flag(self, item):
        if self._contentName not in item:
            return

        words_flag = self._segRule.seg_flag(item[self._contentName])
        if not words_flag:
            return

        seg_item = ()
        for word_flag in words_flag:
            seg_item = (item[self._keyid],) + word_flag + (1,)

            if self._additionFields is not None:
                for field in self._additionFields:
                    seg_item = seg_item + (item[field],)

            self.add_item(seg_item)
