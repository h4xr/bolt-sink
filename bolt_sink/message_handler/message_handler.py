'''
File: message_handler.py
Description: The message handler interface
Author: Saurabh Badhwar <sbadhwar@redhat.com>
Date: 23/10/2017
'''
from structures import MetricMessage, MetricGroup, MessageReport
import json

class MessageHandler(object):
    """Message handler class

    Handles the incoming messages to be processed by the Bolt Sink
    """

    def __init__(self, socket_handler):
        """Initialize the message handler

        Keyword arguments:
        socket_handler -- The Socket handler object
        """

        self.socket_handler = socket_handler
        self.metric_group = MetricGroup()
        self.message_report = MessageReport()

        #Register our message handler interface
        self.socket_handler.register_handler(self.handle_message)

    def handle_message(self, message):
        """Handle the incoming set of messages

        Keyword arguments:
        message -- The incoming message to be processed

        Returns:
            Bool
        """

        try:
            message_dict = json.loads(message)
        except:
            return False

        if 'message_id' not in message_dict.keys() or 'metrics' not in message_dict.keys():
            return False

        if 'error' not in message_dict.keys():
            metric = MetricMessage(message_dict)
            self.metric_group(metric.get_message_id(), metric.get_message_metrics())
            self.message_report.p_vote(metric.get_message_id())
        else:
            self.handle_erroroneous(message_id)

        return True

    def handle_erroroneous(self, message_id):
        """If the message has some error, handle it accordingly.

        Keyword arguments:
        message_id -- The id of the message to be handled
        """

        self.message_report.n_vote(message_id)
