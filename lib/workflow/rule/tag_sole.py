from . import tag


class TagRule(tag.TagRule):
    def _main(self):
        self._keywordFunList.append(tag.symbol_underline_fun)

    def get_keys(self):
        return [self._keywordName] + self._get_insert_fields()

    def tag_insert(self, content):
        return self._tag_insert(content=content)

    def tag_update(self, content):
        return self._tag_update(content=content)


class TagRuleFirst(TagRule):
    INDEX = 0

    def _tag(self, content):
        keywords = self._tag_content(content=content)
        if not keywords:
            return None

        keywords_frequency = self._generate_frequency(keywords=keywords)

        keyword = keywords[self.INDEX]
        return {keyword: keywords_frequency[keyword]}


class TagRuleLast(TagRuleFirst):
    INDEX = -1
