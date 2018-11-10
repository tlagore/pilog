from queue import Queue, Empty
import mmap
import os
import struct
import threading
from utilities.utilities import eprint, time_message


class Mmapper:
    HEADER_SIZE = 16
    DEFAULT_WORKERS = 4

    def __init__(self, logDir, fileSize, primary, secondary):
        """ """
        self._primaryFile = os.path.join(logDir, primary)
        self._secondaryFile = os.path.join(logDir, secondary)
        self._fileSize = int(fileSize / 2)
        self._swapLock = threading.Lock()
        self._writeLock = threading.Lock()
        self._msgQueue = Queue(2500)

        self.initialize_log_dir(logDir)
        self.initialize_log_file(self._primaryFile)
        self.initialize_log_file(self._secondaryFile)
        self._index = 0
        self._bindex = 0
        self._workers = []
        self._running = True

        for i in range(0, self.DEFAULT_WORKERS):
            self._workers.append(threading.Thread(target=self._handle_messages))
            self._workers[i].start() 


    def open(self):
        try:
            self._primary = open(self._primaryFile, "r+b")
            self._secondary = open(self._secondaryFile, "r+b")
            #0 means whole file
            self._mcurrent = mmap.mmap(self._primary.fileno(), 0, access=mmap.ACCESS_WRITE)
            self._msecondary = mmap.mmap(self._secondary.fileno(), 0, access=mmap.ACCESS_WRITE)

            return True
        except Exception as ex:
            eprint(time_message("Error opening Mmapper {0}".format(ex)))
            return False     


    def _handle_messages(self):
        """ """
        while self._running:
            try:
                if self._running:
                    msg = self._msgQueue.get(True, 2)
            except Empty as ex:
                # simply means we couldn't get an item in the timeout
                continue
            except Exception as ex:
                eprint(time_message("Exception in handle_messages! {0}".format(ex)))


    def log(self, msg):
        payload = msg.payload.encode(encoding='UTF-8')
        if (self._index + (len(payload) + self.HEADER_SIZE) <= self._fileSize):
            lvl = struct.pack("I", int(msg.level.value))
            id = struct.pack("I", int(msg.id))
            length = struct.pack("I", len(payload))
            reserved = struct.pack("I", 0)

            self._mcurrent[self._index:self.INT_SIZE] = lvl
            self._mcurrent[self._in]

            self._mcurrent[self._index:len(encoded)] = encoded
            self._index += len(encoded)

    def write_field(self, field):
        self._mcurrent[self._index:len(field)]
        self._index += 

    def force_swap(self):
        self._swap()

    def _swap(self):
        """ swaps the current memory mapped file and  """
        with self._swapLock:
            temp  = self._mcurrent
            self._mcurrent = self._msecondary
            self._msecondary = temp

            self._bindex = self._index
            self._index = 0

    def write_log(self):
        """ """


    def close(self):
        """ """
        try:
            self._primaryFile.close()
            self._secondaryFile.close()
            self._mcurrent.close()
            self._msecondary.close()
        except Exception as ex:
            eprint(time_message("Error closing Mmapper {0}".format(ex)))
            return False

    def initialize_log_file(self, curfile):
        """ initializes the log file """
        try:
            if os.path.exists(curfile):
                fs = os.stat(curfile).st_size
                if fs != self._fileSize:
                    os.remove(curfile)
                    self.alloc_file(curfile, self._fileSize)
            else:
                #increase_file_size will create the file if it doesn't exist
                self.alloc_file(curfile, self._fileSize)
        except Exception as ex:
                eprint(time_message("CRTICIAL ERROR: Unable to create log file"))
                raise ex
    
            
    def alloc_file(self, file, sz):
        """ opens a file and seeks to a point to write a byte, causing it to be that size """
        with open(file, "wb") as fd:
            fd.seek(sz-1)
            fd.write(b"\0")
            fd.close()

    def initialize_log_dir(self, logDirectory):
        """ ensures that the either the supplied directory or the default store directory has been created """
        if not os.path.exists(logDirectory):
            try:
                os.makedirs(dir, 0o755)
            except:
                raise MMapperException("LogDirectory did not exist and could not be created")
        elif not os.path.isdir(logDirectory):
            raise MMapperException("Supplied log directory is not a directory")
                    
    def __del__(self):
        """ """
        self.close()

class MMapperException(Exception):
    pass