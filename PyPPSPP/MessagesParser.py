import logging

from struct import unpack
from collections import deque

from Messages.MsgHandshake import MsgHandshake
from Messages.MsgHave import MsgHave
from Messages.MsgData import MsgData
from Messages.MsgIntegrity import MsgIntegrity
from Messages.MsgAck import MsgAck
from Messages.MessageTypes import MsgTypes as MT

class MessagesParser(object):
    """A parser for PPSPP messages. Binary data -> Msg Objects"""

    def ParseData(peer_scope, received_data):
        """Parse received messages to corresponding message objects"""
        data_rx = len(received_data)
        data_parsed = 0

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
                message = MsgIntegrity(peer_scope.hash_type)
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

        return messages