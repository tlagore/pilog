import socket
import time
import threading
import traceback
from message_socket.message_socket import MessageSocket, MessageType, PiLogMsg

class PiLoggee():
    def __init__(self):
        """ """
        self._client_lock = threading.Lock()
        self._num_clients = 0    
        self._running = False

    def start_server(self):
        """ """
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(("0.0.0.0", 12346))
        self._running = True
        self._listen_thread = threading.Thread(target=self.listen)
        self._listen_thread.start()

    def stop_server(self):
        print("Received shutdown signal")
        if self._running:
            self._running = False
            self._socket.shutdown(socket.SHUT_RDWR)

    def handle_client(self, args):
        """  """
        self.increment_clients()

        (client, addr) = args
        client_sock = MessageSocket(client)
        try:
            msg = client_sock.recv_message()
            while(msg.type != MessageType.DISCONNECT and self._running):
                self.handle_message(client, msg)
                msg = client_sock.recv_message()

            if not self._running:
                client_sock.close()
            else:
                print("Client {0} has disconneccted".format(addr[0]))
        except:
            print("Error handling client in PiLogee.work. Disconnecting from client {0}".format(addr[0]))

            traceback.print_exc()

        self.decrement_clients()

    def increment_clients(self):
        with self._client_lock:
            self._num_clients += 1

    def decrement_clients(self):
        with self._client_lock:
            self._num_clients -= 1

    def handle_message(self, client, msg):
        """ handles a message received by a client """
        print("from client {0}, payload: {1}".format(client, msg.payload))


    def listen(self):
        """ listens for clients """
        try:
            while(self._running):
                self._socket.listen(5)
                (client, addr) = self._socket.accept()
                clientThread = threading.Thread(target=self.handle_client, args=((client, addr),))
                clientThread.start()
        except:
            if self._running:
                print("Exception in listen.")
                traceback.print_exc()
            else:
                print("Shutting down")