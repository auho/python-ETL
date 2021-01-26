class MysqlConf:
    KEYS = [
        'host',
        'port',
        'user',
        'passwd',
        'db',
        'charset'
    ]

    def __init__(self):
        self.host = ''
        self.port = 3306
        self.user = ''
        self.passwd = ''
        self.db = ''
        self.charset = 'utf8mb4'

    def load(self, info):
        for k, v in info.items():
            getattr(self, k)

            if v:
                setattr(self, k, v)

    def dns(self):
        dns = dict()
        for k in self.KEYS:
            dns[k] = getattr(self, k)

        return dns
