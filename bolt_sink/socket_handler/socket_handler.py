'''
File: socket_handler.py
Description: Socket handling mechanism for Bolt Sink
Author: Saurabh Badhwar <sbadhwar@redhat.com>
Date: 23/10/2017
'''
import os
import socket
import threading

class SocketHandler(object):
    """Socket Handler for handling incoming/outgoing socket connections

    Bolt sink is responsible for handling the incoming message metrics and
    outgoing message confirmations.
    """

    def __init__(self):
        """Initialize the socket handler

        Initialize the socket handler interface for
        """

        self.bolt_sink_host = os.getenv('BOLT_SINK_HOST', '127.0.0.1')
        self.bolt_sink_port = os.getenv('BOLT_SINK_PORT', 5201)
        self.bolt_server_host = os.getenv('BOLT_SERVER_HOST', '127.0.0.1')
        self.bolt_server_port = os.getenv('BOLT_SERVER_PORT', 5200)
        self.bolt_sink_queue_size = os.getenv('BOLT_SINK_QUEUE_SIZE', 1024)

        self.listen = True

        #Initialize the clients dict
        self.clients = {}

        #Register the generic message handler
        self.register_handler(self.__generic_handler)

    def register_handler(self, message_handler):
        """Register a new message handler for the handling of the messages

        Keyword arguments:
        message_handler -- The message handler object to handle the message
        """

        self.message_handler = message_handler

    def start_sink(self):
        """Start the bolt sink

        Start the sink service by initiating the sink server and also subscribing
        to the bolt server.
        """

        self.listener_thread = threading.Thread(target=self.__bind_interface)
        self.listener_thread.daemon = True

        self.listener_thread.start()
        self.__start_publisher()

    def send_message(self, message):
        """Send a message through the publisher interface

        Keyword arguments:
        message -- The message to be published

        Raises:
            OverflowError if the message exceeds the buffer length
        """

        if len(message) > 32000:
            raise OverflowError("Message exceeds the buffer length")

        self.publisher.sendall(message)

    def __bind_interface(self):
        """Bind to the interface to listen for the incoming connections

        We need to bind to an interface to which the clients can connect to
        publish the results and outputs.
        """

        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind((self.bolt_sink_host, self.bolt_sink_port))
        self.listener.listen(self.bolt_sink_queue_size)

        #Start accepting connections
        while self.listen:
            conn, addr = self.listener.accept()
            client_thread = threading.Thread(target=self.__client_listener, args=(conn,))
            client_thread.daemon = True
            self.clients[conn] = client_thread
            client_thread.start()

    def __client_listener(self, client):
        """Start a listener process to listen to the client messages

        Keyword arguments:
        client -- The client for which we needs the process to run
        """

        while True:
            message = client.recv(32000)
            if not message:
                break
            self.message_handler(message)   #Handover the message to handler

    def __start_publisher(self):
        """Start the results publishing service

        We need to publish the results to bolt server for the dependency graph
        to work properly. Publisher connects to the bolt server for the purpose
        of handling this use case.
        """

        self.publisher = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.publisher.connect((self.bolt_server_host, self.bolt_server_port))

        #Subscribe as a topic for bolt sink
        self.publisher.sendall('SINK: ' + self.bolt_sink_host)

    def __generic_handler(self, message):
        """Generic incoming message handler

        Handles the incoming in the generic way by printing them.

        Keyword arguments:
        message -- The incoming message
        """

        print message
