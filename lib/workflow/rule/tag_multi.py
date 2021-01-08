from . import tag


class TagRule(tag.TagRule):
    def _main(self):
        self._keywordFunList.append(tag.symbol_underline_fun)

    def get_keys(self):
        return [self._keywordName, 'keyword_num'] + self._tagsName

    def tag_insert(self, content):
        return self._tag_multi_insert(content=content)
