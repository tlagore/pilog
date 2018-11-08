import mmap

class Mmapper:
    def __init__(self, fileName):
        """ """
        self._file = open(fileName, "wb")
        #0 means whole file
        self._mfile = mmap.mmap(self._file.fileno, 0)


    def __del__(self):
