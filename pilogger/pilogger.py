from datetime import datetime
import os
import socket
import sys
import time
import traceback
import errno

from message_socket.message_socket import MessageSocket, MessageType, PiLogMsg

class PiLogger():
    DEFAULT_DIR = os.path.join(os.getcwd(), "pilog_logs/")

    def __init__(self, host, port, logDirectory = None):
        """ """
        self._host = host
        self._port = port
        self._initialized = False
        self.initialize_log_dir(logDirectory)

        return
        self.intialize(host, port)

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
                    
    def try_mkdir(self, dir, mode=664, errorMsg="Could not make directory"):
        """ wraps mkdir in a try block with optional specified error message and mode """
        try:
            os.mkdir(dir, mode)
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


    def error(self, msg):
        """ log an error """

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

    def listen(self):
        """ """
        while(True):
            self._socket.listen(5)
            (client, addr) = self._socket.accept()
            clientThread = threading.Thread(target=work, args=((client, addr),))
            clientThread.start()

    def eprint(self, *args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)

    def time_message(self, message):
        return datetime.now().strftime("!! %H:%M:%S - ") + message

class PiLogError(Exception):
    def __init__(self, msg):
        super(msg)
        self._msg = msg

    def __str__(self):
        return repr(self._msg)