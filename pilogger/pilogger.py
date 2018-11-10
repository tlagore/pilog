from datetime import datetime
import os
import socket
import sys
import time
import time
import traceback
import errno

from utilities.utilities import tprint, eprint, time_message
from message_socket.message_socket import MessageSocket, MessageType, MessageLevel, PiLogMsg
from .mmapper import Mmapper 

class PiLogger():
    DEFAULT_DIR = os.path.join(os.path.expanduser("~"), "logs", "pilog_logs/")
    #DEFAULT_DIR = os.path.join(os.getcwd(), "pilog_logs/")
    LOG_FILE_PRIMARY = "pilog_tmp_log"
    LOG_FILE_SECONDARY = "pilog_tmp_log2"
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

            logDir = config.get("log_directory")
            if not logDir:
                logDir = self.DEFAULT_DIR

            self._logger = Mmapper(logDir, self.LOG_FILE_SIZE, self.LOG_FILE_PRIMARY, self.LOG_FILE_SECONDARY)

            if not self._logger.open():
                raise PiLogError("Failed to open memory mapped file.")

            for i in range(0, 25):
                msg = PiLogMsg(mType=MessageType.LOG, mMessageLevel=MessageLevel.INFO, mId=i, mPayload="Hello First World {0}!".format(i))
                self._logger.log(msg)

            self._logger.force_swap()

            for i in range(0, 25):
                msg = PiLogMsg(mType=MessageType.LOG, mMessageLevel=MessageLevel.INFO, mId=i, mPayload="Hello buggeroo Second World {0}!".format(i))
                self._logger.log(msg)

            time.sleep(5)
            self._logger.close()
            return

            if config.get("log_info_messages") and config["log_info_messages"].lower() == "true":
                self._logInfoMessages = True
            else:
                self._logInfoMessages = False

            if self._verbose:
                tprint("Starting PiLogger with arguments:")
                tprint("Host: {0}".format(self._host))
                tprint("Port: {0}".format(self._port))
                tprint("Log Directory: {0}".format(self._host))
                tprint("Host: {0}".format(self._host))
                tprint("Host: {0}".format(self._host))

            self.intialize(self._host, self._port)

        except KeyError as ex:
            cause = ex.args[0]
            eprint(time_message("Required configuration key \"{0}\" was not supplied.".format(cause)))

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
            eprint(time_message("Error initializing socket. Invalid hostname: {0}".format(host)))
        except ConnectionRefusedError as e:
            eprint(time_message("{0}:{1} - Connection refused.".format(ip,port)))
        except Exception as e:
            eprint(time_message("error initializing PiLogger"))
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
            eprint(time_message(e))

    def start(self):
        """ starts the client thread  """
        if not _initialized:
            eprint("Not initalized. Run initialize(host, port) or try to reinitialize with initialize()")

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

    def __del__(self):
        """ """
        try:
            self._logger.close()
        except Exception as ex:
            eprint(time_message(ex))


class PiLogError(Exception):
    pass