from datetime import datetime
import os
import socket
import sys
import time
import traceback
import errno

from message_socket.message_socket import MessageSocket, MessageType, MessageLevel, PiLogMsg

class PiLogger():
    DEFAULT_DIR = os.path.join(os.getcwd(), "pilog_logs/")
    LOG_FILE = "pilog_tmp_log"
    LOG_FILE_SIZE = 10*1024*1024

    def __init__(self, config): #host, port, logDirectory = None):
        """ """
        self._initialized = False

        try:
            if config.get("verbose") and config["verbose"].lower() == 'true':
                self._verbose = True
            else:
                self._verbose = False

            if config.get("log_file_size"):
                try:
                    self._logFileSize = int(config["log_file_size"])
                except ValueError as e:
                    eprint("PiLogger: passed in log_file_size could no tbe converted to integer. Using default size of {0}".format(self.LOG_FILE_SIZE))
                    self._logFileSize = self.LOG_FILE_SIZE
            else:
                self._logFileSize = self.LOG_FILE_SIZE

            self._host = config["host"]
            self._port = config["port"]
            self.initialize_log_dir(config.get("log_directory"))
            self._logFilePath = os.path.join(self._logDirectory, self.LOG_FILE)
            self.initialize_log_file()

            if config.get("log_info_messages") and config["log_info_messages"].lower() == "true":
                self._logInfoMessages = True
            else:
                self._logInfoMessages = False

            if self._verbose:
                self.tprint("Starting PiLogger with arguments:")
                self.tprint("Host: {0}".format(self._host))
                self.tprint("Port: {0}".format(self._port))
                self.tprint("Log Directory: {0}".format(self._host))
                self.tprint("Host: {0}".format(self._host))
                self.tprint("Host: {0}".format(self._host))

            self.intialize(self._host, self._port)

        except KeyError as ex:
            cause = ex.args[0]
            self.eprint(self.time_message("Required configuration key \"{0}\" was not supplied.".format(cause)))

    
    def initialize_log_file(self):
        """ initializes the log file """
        try:
            if os.path.exists(self._logFilePath):
                fs = os.stat(self._logFilePath).st_size
                if fs < self.LOG_FILE_SIZE:
                    self.increase_file_size(self._logFilePath, self.LOG_FILE_SIZE)
            else:
                #increase_file_size will create the file if it doesn't exist
                self.increase_file_size(self._logFilePath, self.LOG_FILE_SIZE)
        except Exception as ex:
                self.eprint(self.time_message("CRTICIAL ERROR: Unable to create log file"))
                raise ex
    
            
    def increase_file_size(self, file, sz):
        """ opens a file and seeks to a point to write a byte, causing it to be that size """
        with open(file, "wb") as fd:
            fd.seek(sz-1)
            fd.write(b"\0")
            fd.close()

    def tprint(self, msg):
        print(time_print(msg))

    def initialize_log_dir(self, logDirectory):
        """ ensures that the either the supplied directory or the default store directory has been created """
        if logDirectory:
            if not os.path.exists(logDirectory):
                self.try_mkdir(logDirectory, errorMsg="LogDirectory did not exist and could not be created")
                self._logDirectory = logDirectory
            elif not os.path.isdir(logDirectory):
                raise PiLogError("Supplied log directory is not a directory")
        else: 
            if not os.path.exists(self.DEFAULT_DIR):
                self.try_mkdir(self.DEFAULT_DIR, errorMsg="No log directory supplied and default directory could not be created")
            self._logDirectory = self.DEFAULT_DIR
                    
    def try_mkdir(self, dir, errorMsg="Could not make directory"):
        """ wraps mkdir in a try block with optional specified error message and mode """
        try:
            os.mkdir(dir, 0o755)
            #os.chmod(dir, 0o664)
        except:
            raise PiLogError(errorMsg)

    def initialize(self):
        """ attempts to reinitialize with the currently specified host and port """
        if not self._host:
            raise PiLogError("Host not defined, use intialize(host, port) instead")

        if not self._port:
            raise PiLogError("Port not defined, use intialize(host, port) instead")

        self.initialize(self._host, self._port)

    def intialize(self, host, port):
        """ attempts to intiailze with a specified host and port """
        try:
            ip = socket.gethostbyname(host)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            self._socket = MessageSocket(sock)
            self._initialized = True
        except socket.gaierror as e:
            self.eprint(self.time_message("Error initializing socket. Invalid hostname: {0}".format(host)))
        except ConnectionRefusedError as e:
            self.eprint(self.time_message("{0}:{1} - Connection refused.".format(ip,port)))
        except Exception as e:
            self.eprint(self.time_message("error initializing PiLogger"))
            raise e

    def info(self, msg):
        """ log an info message """
        self._log_helper(msg, MessageLevel.INFO)

    def warning(self, msg):
        """ log a warning message """
        self._log__helper(msg, MessageLevel.WARNING)

    def error(self, msg):
        """ log an error """
        self._log_helper(msg, MessageLevel.ERROR)

    def stop(self):
        """ """
        msg = PiLogMsg(mType=MessageType.DISCONNECT, mPayload=None, mMessageLevel=MessageLevel.INFO)
        self._socket.send_message(msg)

    def _log_helper(self, msg, MessageLevel):
        msg = PiLogMsg(mType=MessageType.LOG, mPayload=msg, mMessageLevel=MessageLevel.ERROR)
        try:
            self._socket.send_message(msg)
        except Exception as e:
            self.eprint(self.time_message(e))

    def start(self):
        """ starts the client thread  """
        if not _initialized:
            self.eprint("Not initalized. Run initialize(host, port) or try to reinitialize with initialize()")

        try:
            for i in range(0, 10):
                msg = PiLogMsg(mType=MessageType.METRIC, mPayload=i)
                self._socket.send_message(msg)
                time.sleep(2)

            msg = PiLogMsg(mType=MessageType.DISCONNECT)
            self._socket.send_message(msg)
            time.sleep(2)
        except Exception as e:
            if e.errno == 32:
                print("Server closed the connection")
            else:
                print("Exception in client! Errno: {0}".format(errno.errorcode))
                traceback.print_exc()

    def eprint(self, *args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)

    def time_message(self, message):
        return datetime.now().strftime("!! %H:%M:%S - ") + "{0}".format(message)


class PiLogError(Exception):
    def __init__(self, msg):
        super(msg)
        self._msg = msg

    def __str__(self):
        return repr(self._msg)