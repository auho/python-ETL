import importlib


class Demand:
    def __init__(self, module_name):
        self._module = importlib.import_module(module_name)

    def run_file(self, file_name):
        self._get_entity(entity_name=file_name)

    def run_fun(self, fun_name, kwargs=None):
        fun = self._get_entity(entity_name=fun_name)
        if kwargs:
            return fun(**kwargs)
        else:
            return fun()

    def run_class_method(self, class_name, method_name, kwargs=None):
        entity = self._get_entity(entity_name=class_name)
        method = getattr(entity(), method_name)
        if kwargs:
            return method(**kwargs)
        else:
            return method()

    def _get_entity(self, entity_name):
        return getattr(self._module, entity_name)
