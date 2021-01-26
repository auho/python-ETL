import importlib
from lib.common.app import App


class Demand:
    def __init__(self, module_name):
        self._module = importlib.import_module(module_name)

    def run_files(self, files_names):
        for file_name in files_names:
            self.run_file(file_name)

    def run_file(self, file_name):
        importlib.import_module(self._module.__name__ + '.' + file_name)

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


class DemandModule:
    def __init__(self, app: App):
        self._APP = app

    def run_files(self, files_names, path=None):
        module_import = self._APP.moduleImport
        if path:
            module_import = module_import + '.' + path.strip('/').replace('/', '.')

        for file_name in files_names:
            importlib.import_module(module_import + '.' + file_name)
