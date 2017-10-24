'''
File: data_dispatcher.py
Description: Data dispatcher class for providing common functionality to be built
             upon by the other dispatcher classes.
Author: Saurabh Badhwar <sbadhwar@redhat.com>
Date: 24/10/2017
'''
import socket
import time

class DataDispatcher(object):
    """Common class for inheriting by the other specific dispatchers

    Provides some of the common requested functionality such as socket handling,
    file handling and management, and data writes.
    """

    def __init__(self):
        """Initialize the Data Dispatcher"""

        self.error_log = []
        self.socket = None
        self.file_desc = None

    def tcp_connect(self, host, port):
        """Establish a TCP connection with a remote server

        Keyword arguments:
        host -- The address of the remote host to connect to
        port -- The port to establish a connection on

        Returns:
            Boolean
        """

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.socket.connect((host, port))
        except Exception:
            self.error_log.append("Unable to establish a remote connection")
            return False

        return True

    def udp_connect(self, host, port):
        """Establish a UDP connection to the remote server

        Keyword arguments:
        host -- The remote host to establish the connection to
        port -- The port to establish the connection with

        Returns:
            Boolean
        """

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.socket.connect((host, port))
        except Exception:
            self.error_log.append("Unable to establish a remote connection")
            return False

        return True

    def socket_write(self, message):
        """Write some data to the socket

        Keyword arguments:
        message -- The message to be written to the socket

        Returns:
            Boolean
        """

        if self.socket == None:
            self.error_log.append("No socket endpoint connected")
            return False

        self.socket.sendall(message)
        return True

    def file_open(self, filename):
        """Open a file for reading or writing of data

        Keyword arguments:
        filename -- The fully qualified pathname of the file to be worked upon

        Returns:
            Boolean
        """

        try:
            self.file_desc = open(filename, 'a+')
        except Exception:
            self.error_log.append("Unable to open the requested file")
            return False

        return True

    def file_write(self, data):
        """Write data to the file

        Keyword arguments:
        data -- The data to be written to the file

        Returns:
            Boolean
        """

        if self.file_desc == None:
            self.error_log.append('Unable to write data to the file')
            return False

        self.file_desc.write(data)
        return True

    def socket_close(self):
        """Close the socket"""

        if self.socket != None:
            self.socket.close()

    def file_close(self):
        """Close the opened file descriptor"""

        if self.file_desc != None:
            self.file_desc.close()
