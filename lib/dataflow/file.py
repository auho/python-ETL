from . import process
from ..db import corpus


class ActionWrite(process.Action):
    def __init__(self, file_name, size=100000, kwargs=None):
        super().__init__(kwargs=kwargs)

        self.file_name = file_name
        self.size = size
        self._file = None  # type:corpus.Corpus

    def before_action(self):
        self._file = corpus.Corpus(file_path=self.file_name)
        self._file.truncate()

    def after_do(self):
        if len(self._items) > self.size:
            self.after_done()

    def after_done(self):
        if self._items:
            self._file.write('\n'.join(self._items) + '\n')
            self._items = []

    def after_action(self):
        self._file.close()
