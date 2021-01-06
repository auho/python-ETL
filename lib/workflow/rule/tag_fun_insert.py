from . import interface, tag_fun


class TagRule(tag_fun.TagRule, interface.TagInsert):
    """
    fun return:
    """

    def __init__(self, names, fun_list):
        super().__init__(fun_list=fun_list)

        self._names = names

    def get_all_name(self):
        return self._names

    def tag_insert(self, content):
        res = self._fun(content)
        if not res:
            return None

        if type(res) != tuple:
            print(res, content)
            raise Exception("function result is not tuple!")

        return res,
