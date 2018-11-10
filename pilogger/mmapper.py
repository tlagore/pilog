from queue import Queue, Empty, Full
import mmap
import os
import struct
import threading
from utilities.utilities import eprint, time_message


class Mmapper:
    HEADER_SIZE = 16
    NUM_WORKERS = 4
    MAX_PUT_FAILS = 100

    def __init__(self, logDir, fileSize, primary, secondary):
        """ """
        self._primaryFile = os.path.join(logDir, primary)
        self._secondaryFile = os.path.join(logDir, secondary)
        self._fileSize = int(fileSize / 2)

        self._writeLock = threading.Lock()
        self._logLock = threading.Lock()
        self._iterLock = threading.Lock()

        self._msgQueue = Queue(2500)
        self._failedPuts = 0

        self.initialize_log_dir(logDir)
        self.initialize_log_file(self._primaryFile)
        self.initialize_log_file(self._secondaryFile)
        self._index = 0
        self._bindex = 0
        self._workers = []
        self._running = True

        for i in range(0, self.NUM_WORKERS):
            self._workers.append(threading.Thread(target=self._handle_messages, args=(i,)))
            self._workers[i].start() 


    def open(self):
        try:
            self._primary = open(self._primaryFile, "r+b")
            self._secondary = open(self._secondaryFile, "r+b")

            self._primary.flush()
            self._secondary.flush()
            #0 means whole file
            self._mcurrent = mmap.mmap(self._primary.fileno(), 0, access=mmap.ACCESS_WRITE)
            self._msecondary = mmap.mmap(self._secondary.fileno(), 0, access=mmap.ACCESS_WRITE)

            return True
        except Exception as ex:
            eprint(time_message("Error opening Mmapper {0}".format(ex)))
            return False     


    def _handle_messages(self, args):
        """ """
        workerNum = args

        print(time_message("Worker {0} starting up".format(workerNum))) 
        while self._running:
            try:
                if self._running:
                    msg = self._msgQueue.get(True, 2)
                    self._log(msg)
            except Empty as ex:
                # simply means we couldn't get an item in the timeout
                continue
            except Exception as ex:
                eprint(time_message("Exception in handle_messages! {0}".format(ex)))

        print(time_message("Worker {0} exitting".format(workerNum))) 


    def log(self, msg):
        """ """
        try:
            self._msgQueue.put_nowait(msg)
        except Full as ex:
            with self._iterLock:
                self._failedPuts += 1
            if (self._failedPuts > self.MAX_PUT_FAILS):
                eprint(time_message("MMapper is failing to queue messages. Could be falling behind. {0} failed puts so far.".format(self._failedPuts)))

    def _log(self, msg):
        payload = msg.payload.encode(encoding='UTF-8')

        #should decide if we want to force a write when we fill or discard the logs until write is requested
        #currently just discarding
        with self._logLock:
            if (self._index + (len(payload) + self.HEADER_SIZE) <= self._fileSize):
                lvl = struct.pack("I", int(msg.level.value))
                msgid = struct.pack("I", int(msg.id))
                length = struct.pack("I", len(payload))
                reserved = struct.pack("I", 0)

                self._write_field(lvl)
                self._write_field(msgid)
                self._write_field(length)
                self._write_field(reserved)
                self._write_field(payload)


    def _write_field(self, field):
        """ Write a field to the mapped files current index. Assumes space check has already been done """
        self._mcurrent[self._index:self._index+len(field)] = field
        self._index += len(field) + 1
        self._mcurrent.seek(self._index)

    def force_swap(self):
        self._swap()

    def _swap(self):
        """ swaps the current memory mapped file and  """
        with self._logLock:
            self._mcurrent.flush()

            temp  = self._mcurrent
            self._mcurrent = self._msecondary
            self._msecondary = temp

            self._bindex = self._index
            self._index = 0

    def write_log(self):
        """ """

    def close(self):
        """ """
        print("close()")
        try:
            self._running = False
            self._primary.close()
            self._secondary.close()
            self._mcurrent.close()
            self._msecondary.close()

            for i in range(0, self.NUM_WORKERS):
                self._workers[i].join()

        except Exception as ex:
            eprint(time_message("Error closing Mmapper {0}".format(ex)))
            return False

        print(time_message("MMapper Shutting down"))

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
        print("__del__")
        self.close()

class MMapperException(Exception):
    pass