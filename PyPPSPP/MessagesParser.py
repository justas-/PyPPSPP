import logging

from struct import unpack
from collections import deque

from Messages.MsgHandshake import MsgHandshake
from Messages.MsgHave import MsgHave
from Messages.MsgData import MsgData
from Messages.MsgIntegrity import MsgIntegrity
from Messages.MessageTypes import MsgTypes as MT

class MessagesParser(object):
    """A parser for PPSPP messages. Binary data -> Msg Objects"""

    def ParseData(received_data):
        """Parse received messages to corresponding message objects"""
        data_rx = len(received_data)
        data_parsed = 0
        logging.info("Parsing {0}B of received data".format(data_rx))

        my_channel = unpack('>I', received_data[0:4])[0]
        data_parsed = data_parsed + 4

        messages = deque()

        # Parse messages based on
        while data_parsed < data_rx:
             
            type = received_data[data_parsed]
            data_parsed = data_parsed + 1

            message = None
            
            if type == MT.HANDSHAKE:
                their_channel = unpack('>I', received_data[data_parsed:data_parsed+4])[0]
                data_parsed = data_parsed + 4
                message = MsgHandshake()
                data_read = message.ParseReceivedData(received_data[data_parsed:])
                message.our_channel = my_channel
                message.their_channel = their_channel
                data_parsed = data_parsed + data_read
                messages.append(message)
            elif type == MT.DATA:
                message = MsgData()
                data_read = message.ParseReceivedData(received_data[data_parsed:])
                data_parsed = data_parsed + data_read
                messages.append(message)
            elif type == MT.ACK:
                pass
            elif type == MT.INTEGRITY:
                message = MsgIntegrity()
                data_read = message.ParseReceivedData(received_data[data_parsed:])
                data_parsed = data_parsed + data_read
                messages.append(message)
            elif type == MT.HAVE:
                message = MsgHave()
                message.ParseReceivedData(received_data[data_parsed:data_parsed+8])
                data_parsed = data_parsed + 8
                messages.append(message)
            
            if message is None:
                logging.info("Unknown type {0} !!!".format(type))

            logging.info("Received: {0}".format(message))

        logging.info("Parsed {0}B. My channel: {1}; Received {2} message(s)."
                     .format(data_parsed, my_channel, len(messages)))
        return (my_channel, messages)