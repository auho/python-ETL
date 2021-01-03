from . import tag, interface

"""
rule = tag_sole.TagRule(db=db, table_name='rule_one',
                   keyword_name='one_tag_keyword',
                   tags_name=['one_tag'],
                   alias={'keyword_table_name': 'content_table_name'},
                   keyword_fun_list=[tag.symbol_underline_fun])
"""


class TagRule(tag.TagRule, interface.TagInsert, interface.TagAppend):
    def _main(self):
        self._keywordFunList.append(tag.symbol_underline_fun)

    def get_all_name(self):
        return [self._keywordName] + self._tagsName

    def tag_insert(self, content):
        return self._tag_insert(content=content)

    def tag_append(self, content):
        return self._tag_append(content=content)
