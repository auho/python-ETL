class TagRule:
    def __init__(self, func):
        self._func = func

    def main(self):
        if not self._func:
            if not callable(self._func):
                raise Exception("fun is Error!")
