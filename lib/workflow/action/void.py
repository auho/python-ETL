from lib.dataflow import process
from lib.workflow.func.func import FuncVoid


class Action(process.Action):
    def __init__(self, kwargs=None):
        super(Action, self).__init__(kwargs=kwargs)

        self._funcs = []  # type: [FuncVoid]

    def check_action(self):
        if not self._funcs:
            raise Exception('func is error!')

    def add_func(self, func_object):
        if not isinstance(func_object, FuncVoid):
            raise Exception('func is not func.FuncVoid!')

        self._funcs.append(func_object)

    def do(self, item):
        try:
            for func in self._funcs:
                func.do(item=item)
        except Exception as e:
            print(item)
            raise e
