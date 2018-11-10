from queue import Queue, Empty, Full
import mmap
import os
import struct
import threading
import traceback
from utilities.utilities import eprint, time_message


class Mmapper:
    HEADER_SIZE = 16
    NUM_WORKERS = 4
    MAX_PUT_FAILS = 100

    def __init__(self, logDir, fileSize, baseFileName):
        """ """
        self._fileBasePath = os.path.join(logDir, baseFileName)

        #we will have one primary file and one swap file for each worker. Size of total files will comprise to equal fileSize
        #(or slightly less)
        self._fileSize = int(fileSize / self.NUM_WORKERS / 2)
        self._logDirectory = logDir
    
        #create write and read lock for each worker
        self._readLocks = [threading.Lock() for i in range(0, self.NUM_WORKERS)]
        self._writeLocks = [threading.Lock() for i in range(0, self.NUM_WORKERS)]

        self._iterLock = threading.Lock()

        self._msgQueue = Queue(2500)
        self._failedPuts = 0

        self._filePaths = []
        self._files = []
        self._maps = []

        self._indices = [[0 for i in range(0, 2)] for j in range(0, self.NUM_WORKERS)]

        #active file is always 0 or 1, corresponding to primary and backup respectively
        self._activeFiles = [0 for i in range(0, self.NUM_WORKERS)]

        self.initialize_log_dir()
        self.initialize_log_files()

        self._workers = []
        self._running = True

        for i in range(0, self.NUM_WORKERS):
            self._workers.append(threading.Thread(target=self._handle_messages, args=(i,)))
            self._workers[i].start() 


    def open(self):
        try:
            #for each worker
            for i in range(0, self.NUM_WORKERS):
                self._files.append([])
                self._maps.append([])

                #open their primary and secondary log files and map to memory
                for j in range(0, 2):
                    self._files[i].append(open(self._filePaths[i][j], "r+b"))
                    self._maps[i].append(mmap.mmap(self._files[i][j].fileno(), self._fileSize, access=mmap.ACCESS_WRITE))
                    self._maps[i][j].flush()

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
                    self._log(msg, workerNum)
            except Empty as ex:
                # simply means we couldn't get an item in the timeout
                continue
            except Exception as ex:
                eprint(time_message("Exception worker thread {0} in handle_messages! {1}".format(workkerNum, ex)))
                traceback.print_exc()

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

    def _log(self, msg, worker):
        payload = msg.payload.encode(encoding='UTF-8')

        #should decide if we want to force a write when we fill or discard the logs until write is requested
        #currently just discarding
        with self._writeLocks[worker]:
            activeFile = self._activeFiles[worker]
            if (self._indices[worker][activeFile] + (len(payload) + self.HEADER_SIZE) <= self._fileSize):
                lvl = struct.pack("I", int(msg.level.value))
                msgid = struct.pack("I", int(msg.id))
                length = struct.pack("I", len(payload))
                reserved = struct.pack("I", 0)

                self._write_field(lvl, worker, activeFile)
                self._write_field(msgid, worker, activeFile)
                self._write_field(length, worker, activeFile)
                self._write_field(reserved, worker, activeFile)
                self._write_field(payload, worker, activeFile)


    def _write_field(self, field, worker, activeFile):
        """ Write a field to the mapped files current index. Assumes space check has already been done """
        index = self._indices[worker][activeFile]

        self._maps[worker][activeFile][index:index+len(field)] = field
        self._indices[worker][activeFile] += len(field) + 1

    def force_swap(self):
        for i in range(0, self.NUM_WORKERS):
            self._swap(i)

    def _swap(self, worker):
        """ swaps the current memory mapped file and  """
        #prevent swap while we are reading from secondary
        with self._readLocks[worker]:
            #prevent swap while we are writing to primary
            with self._writeLocks[worker]:
                #get current active file and swap it for this worker
                activeFile = self._activeFiles[worker]
                newIndex = 1 - activeFile

                self._maps[worker][activeFile].flush()

                self._activeFiles[worker] = newIndex
                self._indices[worker][newIndex] = 0

    def write_log(self):
        """ """

    def close(self):
        """ """
        print("close()")
        try:
            self._running = False
            # self._primary.close()
            # self._secondary.close()
            # self._mcurrent.close()
            # self._msecondary.close()

            # for i in range(0, self.NUM_WORKERS):
            #     self._workers[i].join()

        except Exception as ex:
            eprint(time_message("Error closing Mmapper {0}".format(ex)))
            return False

        print(time_message("MMapper Shutting down"))

    def initialize_log_files(self):
        """ initializes the log file """
        #create for each worker
        for i in range(0, self.NUM_WORKERS):
            self._filePaths.append([])
            
            #a primary and backup file
            for j in range(0, 2):
                curfile = "{0}{1}{2}".format(self._fileBasePath,i,j)
                self._filePaths[i].append(curfile)
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

    def initialize_log_dir(self):
        """ ensures that the either the supplied directory or the default store directory has been created """
        if not os.path.exists(self._logDirectory):
            try:
                os.makedirs(self._logDirectory, 0o755)
            except:
                raise MMapperException("LogDirectory did not exist and could not be created")
        elif not os.path.isdir(self._logDirectory):
            raise MMapperException("Supplied log directory is not a directory")
                    
    def __del__(self):
        """ """
        print("__del__")
        self.close()

class MMapperException(Exception):
    pass