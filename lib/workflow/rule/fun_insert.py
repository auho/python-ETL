from . import interface, fun


class TagRule(fun.TagRule, interface.TagInsert):
    """
    fun return: [(),...]
    """

    def __init__(self, names, func):
        super().__init__(func=func)

        self._names = names  # list [,...]

    def get_all_name(self):
        return self._names

    def tag_insert(self, content):
        res = self._func(content)
        if not res:
            return None

        if type(res) != tuple:
            print(res, content)
            raise Exception("function result is not tuple!")

        return res,
