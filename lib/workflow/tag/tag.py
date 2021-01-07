from lib.workflow.tag import interface


class TagRule:
    def __init__(self, func):
        self._func = func

    def main(self):
        if not self._func or not callable(self._func):
            raise Exception("fun is Error!")


def function_insert(item):
    """

    :param item:
    :return: [(),...]
    """
    return [()]


def function_update(item):
    """

    :param item:
    :return: dict
    """
    return {
        "key1": "value1",
        "key2": "value2"
    }


class TagInsert(TagRule, interface.TagInsert):
    """
    fun return: [(),...]
    """

    def __init__(self, keys, func):
        super().__init__(func=func)

        self._keys = keys  # list [,...]

    def get_keys(self):
        return self._keys

    def tag_insert(self, item):
        res = self._func(item)
        if not res:
            return None

        if type(res) != tuple:
            print(res, item)
            raise Exception("function result is not tuple!")

        return res,


class TagUpdate(TagRule, interface.TagUpdate):
    """
    fun return: {"":"",...}
    """

    def tag_update(self, item):
        res = self._func(item)
        if not res:
            return None

        if type(res) != dict:
            print(res, item)
            raise Exception("function result is not dict!")

        return res
