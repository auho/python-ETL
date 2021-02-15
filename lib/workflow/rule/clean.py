from . import tag


class TagRule(tag.TagRule):
    def _main(self):
        self._keywordFunList.append(tag.symbol_underline_fun)

    def tag_clean(self, content):
        r = self._tag_update(content=content)
        if r:
            return True
        else:
            return False
