from pilogger.pilogger import PiLogger
import socket
import time

logger = PiLogger(None)
_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
_sock.bind(("0.0.0.0", 12346))

time.sleep(10)