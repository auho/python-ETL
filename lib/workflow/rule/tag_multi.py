import tag


class TagRule(tag.TagRule):
    def _main(self):
        self._keywordFunList.append(tag.symbol_underline_fun)

    def get_keys(self):
        return [self._keywordName, self._get_alias(f"{self._name}_keyword_num")] + self._get_tags_keys()

    def tag_insert(self, content):
        return self._tag_multi_insert(content=content)


class TagRuleEveryKeyword(tag.TagRule):
    def _main(self):
        self._keywordFunList.append(tag.symbol_underline_fun)

    def get_keys(self):
        return [self._keywordName, self._get_alias(f"{self._name}_keyword_num")] + self._get_tags_keys()

    def tag_insert(self, content):
        return self._tag_multi_insert_every_keyword(content=content)
