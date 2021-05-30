import os
import inspect
import importlib
import threading
import queue
import time
import math
import natsort
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
    START_TIME = time.time()

    def __init__(self, app: App):
        self._APP = app
        self._run_items = dict()

    def run_sibling_path_files(self, files_names, sibling_path):
        stack = inspect.stack()
        file_path = stack[1].filename
        file_path = os.path.abspath(file_path)
        path = os.path.dirname(file_path)
        path = path.replace(self._APP.modulePath + '/', '')
        self.run_path_files(files_names=files_names, path=path + '/' + sibling_path)

        return self

    def run_sibling_path(self, sibling_path):
        stack = inspect.stack()
        file_path = stack[1].filename
        file_path = os.path.abspath(file_path)
        path = os.path.dirname(file_path)
        path = path.replace(self._APP.modulePath + '/', '')
        self.run_path(path=path + '/' + sibling_path)

        return self

    def run_path(self, path=None):
        all_files_import = self.get_files_import_of_path(path=path)
        for file_import in all_files_import:
            self.run_file_import(file_import=file_import)

        return self

    def run_path_files(self, files_names, path=None):
        all_files_import = self.get_files_import_of_files(files_names=files_names, path=path)
        for file_import in all_files_import:
            self.run_file_import(file_import=file_import)

        return self

    def run_file(self, file):
        file_import = self._generate_app_path_import(path=file)
        self.run_file_import(file_import=file_import)

    def run_file_import(self, file_import):
        print(f"EXEC:: {file_import}")

        start_time = time.time()
        importlib.import_module(file_import)
        duration = time.time() - start_time
        self._add_run_item(file_import, duration)

    def get_files_import_of_files(self, files_names, path=None):
        all_files_import = []
        path_import = self._generate_app_path_import(path=path)

        for file_name in files_names:
            all_files_import.append(path_import + '.' + file_name)

        return all_files_import

    def get_files_import_of_path(self, path=None):
        all_files_import = []
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

            dir_files_import = []
            for file in files:
                if file[-3:] != '.py':
                    continue

                file = root + file[:-3]
                file_import = path_import + '.' + self._convert_path_to_import(path=file)
                dir_files_import.append(file_import)

            dir_files_import = natsort.natsorted(dir_files_import)
            all_files_import.extend(dir_files_import)

        return all_files_import

    def state(self):
        print("\nSTATE::")
        for item, item_info in self._run_items.items():
            print(f"{item}:: {self._format_duration(item_info['duration'])}")

        duration = time.time() - self.START_TIME
        print(f"Total time: {self._format_duration(duration)}")

    def _add_run_item(self, item, duration):
        self._run_items[item] = {
            'duration': duration
        }

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

    @staticmethod
    def _format_duration(duration):
        return f"{math.floor(duration / 60)} 分 {math.ceil(duration % 60)} 秒"


class DemandThread(threading.Thread):
    def __init__(self, dq, da):
        threading.Thread.__init__(self)

        self._dq = dq  # type:DemandQueue
        self._da = da  # type:DemandApp

        self.e = None

    def check_error(self):
        if self.e:
            raise self.e

    def run(self):
        while not self._dq.can_exit():
            file_import = None
            try:
                file_import = self._dq.get()
                if not file_import:
                    break

                self._dq.done()

                self._da.run_file_import(file_import=file_import)
            except Exception as e:
                if file_import:
                    self.e = e
                else:
                    print("ERROR::", "no queue")

                break

        print("thread done")


class DemandQueue:
    def __init__(self, demand_app):
        self._filesImport = ''
        self._size = 0
        self._queue = None  # type:queue.Queue
        self._threadList = []  # type: [DemandThread]
        self._demandApp = demand_app  # type:DemandApp
        self._exitFlag = False

        self._startTime = ''
        self._endTime = ''

    def _set_files_import(self, files_import):
        self._filesImport = files_import
        self._size = len(self._filesImport)
        self._queue = queue.Queue(self._size)

    def run_path(self, path, thread_num=2):
        files_import = self._demandApp.get_files_import_of_path(path=path)
        self.run_files_import(files_import=files_import, thread_num=thread_num)

    def run_files_import(self, files_import, thread_num=2):
        self._set_files_import(files_import=files_import)

        self._startTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        for i in range(0, thread_num):
            t = DemandThread(dq=self, da=self._demandApp)
            t.start()
            self._threadList.append(t)

        for file_import in self._filesImport:
            self.put(file_import)

        self._wait()
        self.state()

    def get(self):
        return self._queue.get()

    def put(self, item):
        return self._queue.put(item)

    def done(self):
        self._queue.task_done()

    def can_exit(self):
        return self._exitFlag

    def _wait(self):
        self._queue.join()
        self._exitFlag = True

        for t in self._threadList:
            t.join()
            t.check_error()

        self._endTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    def state(self):
        print(f"start:: {self._startTime}")
        print(f"end:: {self._endTime}")

        for file_import in self._filesImport:
            print(file_import)
