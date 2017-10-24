'''
File: graphite_dispatcher.py
Description: Dipatch data to graphite
Author: Saurabh Badhwar <sbadhwar@redhat.com>
Date: 24/10/2017
'''
from data_dispatcher import DataDispatcher

class GraphiteDispatcher(DataDispatcher):
    """Encapsulates the logic for gathering and sending data to graphite"""

    def __init__(self, host, port):
        """Initialize the graphite data dispatcher

        Keyword arguments:
        host -- The graphite backend to connect to
        port -- The port on which the graphite backend is listening for data
        """

        super(GraphiteDispatcher, self).__init__()
        ret = self.tcp_connect(host, port)
        if not ret:
            self.error_log.append("Error establishing a connection to graphite backend")
            return None

    def write_data(self, metric_data):
        """Write data to the graphite backend

        Keyword arguments:
        metric_data -- The metric data to be written
        """

        write_format = "%s %s %d\n"

        for name in metric_data.keys():
            for data in metric_data[name]:
                write_data = write_format.format(name, data[0], data[1])
                self.socket_write(write_data)
