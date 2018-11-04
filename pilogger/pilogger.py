from message_socket.message_socket import MessageSocket

class PiLogger():
    def __init__(self, host):
        """ """
        self._socket = MessageSocket(None)
