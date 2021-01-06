class TagRule:
    def __init__(self, fun):
        self._fun = fun

    def main(self):
        if not self._fun:
            if not callable(self._fun):
                raise Exception("fun is Error!")
