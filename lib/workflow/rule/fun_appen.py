from . import interface, fun


class TagRule(fun.TagRule, interface.TagAppend):
    """
    fun return: {"":"",...}
    """

    def tag_append(self, content):
        res = self._fun(content)
        if not res:
            return None

        if type(res) != dict:
            print(res, content)
            raise Exception("function result is not dict!")

        return res
