'''
File: structures.py
Description: Message handling structures
Author: Saurabh Badhwar <sbadhwar@redhat.com>
Date: 23/10/2017
'''

class MetricMessage(object):
    """Structure represting the Metric Message

    Handles the storage and retrival of the metric type messages
    """

    def __init__(self, message):
        """Initialize the metric type message

        Keyword arguments:
        message -- The metric message
        """

        self.message = message

    def __parse_message(self):
        """Parse the message to divide it into functional components"""

        self.message_id = self.message['message_id']
        self.metrics = self.message['metrics']

    def get_message_id(self):
        """Get the message id

        Return:
            String
        """

        return self.message_id

    def get_message_metrics(self):
        """Get the message metrics

        Return:
            Dict
        """

        return self.metrics

class MetricGroup(object):
    """Structure for grouping up of metrics

    Since different hosts returns metrics in a separate message packet, we need
    to group the metrics based on the message id before we send them for processing
    or hold them on.
    """

    def __init__(self):
        """Initialize the MetricGroup"""

        #Initialize the metric group structure
        self.metric_group = {}

    def add_metric(self, message_id, metrics):
        """Add a new metric to the metric group

        Keyword arguments:
        message_id -- The id of the message
        metrics -- The metric to be associated with the message id
        """

        self.add_message(message_id)
        self.metric_group[message_id].append(metrics)

    def add_message(self, message_id):
        """Add a new message to the grouping

        Keyword arguments:
        message_id -- The id of the message to be added
        """

        if message_id not in self.metric_group.keys():
            self.metric_group[message_id] = []

    def get_messages(self):
        """Get the list of all collected matrices

        Returns:
            List
        """

        return self.metric_group.keys()

    def get_metrics(self):
        """Get all the collected metrics grouped by the message id

        Returns:
            Dict
        """

        return self.metric_group
