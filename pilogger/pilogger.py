import socket
import time
import traceback
import errno
from message_socket.message_socket import MessageSocket, MessageType, PiLogMsg

class PiLogger():
    def __init__(self, host, port):
        """ """

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            self._socket = MessageSocket(sock)
            self.work()
        except:
            print("error initializing PiLogger")
            traceback.print_exc()

    def work(self):
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