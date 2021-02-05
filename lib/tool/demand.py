import os
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


class DemandApp:
    def __init__(self, app: App):
        self._APP = app
        self._run_items = []

    def run_dir(self, path=None):
        if path:
            if path[-1] == '/':
                path = path[:-1]
            abs_path = self._APP.modulePath + '/' + path
        else:
            abs_path = self._APP.modulePath

        path_import = self._generate_app_path_import(path=path)

        for root, dirs, files in os.walk(abs_path):
            if root.find('__pycache__') > -1:
                continue

            if root == abs_path:
                root = ''
            else:
                root = root.replace(abs_path + '/', '')
                if root[-1] != '/':
                    root = root + '/'

            for file in files:
                file = root + file[:-3]

                self._log_run_item(path=path, file=file)

                importlib.import_module(path_import + '.' + self._convert_path_to_import(path=file))

        return self

    def run_files(self, files_names, path=None):
        path_import = self._generate_app_path_import(path=path)

        for file_name in files_names:
            importlib.import_module(path_import + '.' + file_name)

        return self

    def log(self):
        print(self._run_items)

    def _generate_app_path_import(self, path):
        if path:
            return self._APP.moduleImport + '.' + self._convert_path_to_import(path=path)
        else:
            return self._APP.moduleImport

    @staticmethod
    def _convert_path_to_import(path):
        if path:
            return path.strip('/').replace('/', '.')
        else:
            return ''

    def _log_run_item(self, path, file):
        if path:
            self._run_items.append(path + '/' + file)
        else:
            self._run_items.append(file)
