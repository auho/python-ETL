import os


class Corpus:
    def __init__(self, file_path):
        self.file_path = file_path
        self._file = None

        self._check()
        self._create_file()

    def truncate(self):
        self._file.truncate()

    def write(self, content):
        self._file.write(content)

    def close(self):
        self._file.close()

    def _create_file(self):
        self._file = open(self.file_path, 'w')

    def _check(self):
        # if not os.path.isfile(self.file_path):
        #     raise Exception(f'file path [{self.file_path}] is error')

        if os.path.exists(self.file_path):
            os.remove(self.file_path)
