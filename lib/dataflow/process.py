import time
from abc import ABCMeta, abstractmethod
from lib.tool import commonTool


class DataFlow:
    def __init__(self):
        self._runStartTimePoint = 0
        self._runEndTimePoint = 0

    @staticmethod
    def _get_method_args(args):
        if 'self' in args:
            del args['self']

        if '__class__' in args:
            del args['__class__']

        return args

    def start_time(self):
        self._runStartTimePoint = time.time()

    def end_time(self):
        self._runEndTimePoint = time.time()

    def duration(self):
        self.end_time()


class Action(DataFlow):
    """
    Action
    """

    def __init__(self, kwargs=None):
        super().__init__()

        self._items = []
        self._itemAmount = 0
        self._itemFuncList = []
        self._doFuncName = 'do'
        self._doFunc = None

        self._isInit = False

        self._set_args(kwargs=kwargs)
        self._build_do_func()

    def set_do_func_name(self, func_name):
        self._doFuncName = func_name
        self._build_do_func()

    def add_item_func(self, func):
        self._itemFuncList.append(func)

    def _set_args(self, kwargs):
        if kwargs:
            for k, v in kwargs.items():
                if hasattr(self, k):
                    setattr(self, k, v)

    def _build_do_func(self):
        self._doFunc = getattr(self, self._doFuncName)

    def _do_item_func(self, item):
        for func in self._itemFuncList:
            item = func(item=item)

        return item

    def init_action(self):
        """
        在 __init__ 后执行
        子类实现各自自己的逻辑
        :return:
        """
        pass

    def check_action(self):
        return True

    def before_action(self):
        """
        执行 action 之前

        :return:
        """
        pass

    def after_action(self):
        """
        执行 action 之后

        :return:
        """
        pass

    def do_action(self, item):
        item = self._do_item_func(item=item)

        self._doFunc(item=item)

    def do(self, item):
        """
        执行 action

        :param item:
        :return:
        """
        pass

    def after_do(self):
        """
        执行 do 之后
        :return:
        """

        pass

    def after_done(self):
        """
        执行 do、after do 之后
        :return:
        """
        pass

    def add_item(self, item):
        self.inr_item_amount()
        self._items.append(item)

    def add_items(self, items):
        for item in items:
            self.add_item(item=item)

    def log_item_amount(self):
        self._log("item amount: " + str(self._itemAmount))

    def inr_item_amount(self, num=1):
        self._itemAmount += num

    def _log(self, info):
        commonTool.Tool.log(self.__module__ + '.' + self.__class__.__name__ + '::' + info)


class MultiAction(Action):
    def __init__(self, kwargs=None):
        super().__init__(kwargs=kwargs)

        self._actions = []

    def _add_action(self, action):
        self._actions.append(action)

    def check_action(self):
        self._do_sub_method('check_action')

    def before_action(self):
        self._do_sub_method('before_action')

    def after_action(self):
        self._do_sub_method('after_action')

    def after_do(self):
        self._do_sub_method('after_do')

    def after_done(self):
        self._do_sub_method('after_done')

    def _do_sub_method(self, method_name, kwargs=None):
        if kwargs is None:
            kwargs = {}

        for action in self._actions:
            method = getattr(action, method_name)
            method(**kwargs)


class ComplexAction(Action):
    """
    复杂 action，有子 action
    """

    def __init__(self, actions, kwargs=None):
        super().__init__(kwargs=kwargs)

        self._actions = actions

    def check_action(self):
        self._do_sub_method('check_action')

    def before_action(self):
        self._do_sub_method('before_action')

    def after_action(self):
        self._do_sub_method('after_action')

    def do(self, item):
        data = self.do_complex(item)
        if not data:
            return

        for d in data:
            self._do_sub_action(item=d)

    def do_complex(self, item):
        """
        子类实现逻辑

        返回多条可处理数据
        :param item:
        :return:
        """
        return []

    def _do_sub_action(self, item):
        self._do_sub_method('do', {'item': item})

    def after_do(self):
        self._do_sub_method('after_do')

    def after_done(self):
        self._do_sub_method('after_done')

    def _do_sub_method(self, method_name, kwargs=None):
        if kwargs is None:
            kwargs = {}

        for action in self._actions:
            method = getattr(action, method_name)
            method(**kwargs)


class DataProvider(metaclass=ABCMeta):
    """
    data provider
    """

    def __init__(self):
        self._itemAmount = 0
        self._itemFuncList = []

    def add_item_func(self, func):
        self._itemFuncList.append(func)

    def do_item_func(self, item):
        for func in self._itemFuncList:
            item = func(item=item)

        return item

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def next(self):
        """
        下一批数据
        :return:
        """
        return []

    def log_item_amount(self):
        self._log("item amount: " + str(self._itemAmount))

    def inr_item_amount(self, num=1):
        self._itemAmount += num

    def _log(self, info):
        commonTool.Tool.log(self.__module__ + '.' + self.__class__.__name__ + '::' + info)


class DispatchCenter:
    """
    调度
    """

    def __init__(self, dp: DataProvider, actions):
        self._dataProvide = dp
        self._actions = actions

    @staticmethod
    def dispatch(dp: DataProvider, actions):
        dc = DispatchCenter(dp=dp, actions=actions)
        dc.process()

    def process(self):
        """
        从数据源循环获取数据
        循环数据列表
        传递数据，并执行 actions 的 do 方法

        :return:
        """

        self._dataProvide.start()

        start_time_log = 'start' + ' ' + time.strftime('%Y-%m-%d %H:%M:%S')

        self._do_action_method(method_name='init_action')

        self._do_action_method(method_name='check_action')

        self._do_action_method(method_name='before_action')

        while True:
            items = self._dataProvide.next()
            if not items:
                break

            for item in items:
                item = self._dataProvide.do_item_func(item=item)

                self._dataProvide.inr_item_amount()

                self._do_action_method(method_name='do_action', arg=[item])

                self._do_action_method(method_name='after_do')

            self._dataProvide.log_item_amount()

            self._do_action_method(method_name='log_item_amount')

        self._do_action_method(method_name='after_done')

        self._do_action_method(method_name='after_action')

        end_time_log = 'end' + ' ' + time.strftime('%Y-%m-%d %H:%M:%S')

        print(start_time_log + '\n' + end_time_log + '\n\n')

    def _do_action_method(self, method_name, arg=None):
        if not arg:
            arg = []

        for action in self._actions:
            method = getattr(action, method_name)
            method(*arg)
