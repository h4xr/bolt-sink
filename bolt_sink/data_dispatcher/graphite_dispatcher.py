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

    def write_data(self, hostname, message_id, metric_data):
        """Write data to the graphite backend

        Keyword arguments:
        hostname -- The host on which the data was generated
        message_id -- The id of the message generating the data
        metric_data -- The metric data to be written
        """

        write_format = "{} {} {}\n"

        for name in metric_data.keys():
            for data in metric_data[name]:
                metric_name = hostname + '_' + message_id + '_' + name
                write_data = write_format.format(metric_name, data[1], int(data[0]))
                self.socket_write(write_data)
