from . import interface, fun


class TagRule(fun.TagRule, interface.TagCover):
    """
    fun return: str
    """

    def tag_cover(self, name, content):
        res = self._fun(content)
        if not res:
            return None

        return {name: res}
