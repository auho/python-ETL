import time


class Tool:
    @staticmethod
    def object_to_dict(self):
        d = {}
        for name in dir(self):
            value = getattr(self, name)
            if not name.startswith('__') and not callable(value):
                d[name] = value

        return d

    @staticmethod
    def log(info):
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ' ' + info)
