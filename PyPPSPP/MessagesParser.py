"""
PyPPSPP, a Python3 implementation of Peer-to-Peer Streaming Peer Protocol
Copyright (C) 2016,2017  J. Poderys, Technical University of Denmark

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import logging

from struct import unpack
from collections import deque

from Messages import *
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
                message = MsgHandshake.MsgHandshake()
                data_read = message.ParseReceivedData(received_data[data_parsed:])
                message.our_channel = my_channel
                message.their_channel = their_channel
                if their_channel == 0:
                    message._is_goodbye = True
                data_parsed = data_parsed + data_read
                messages.append(message)
            elif type == MT.DATA:
                message = MsgData.MsgData(peer_scope.chunk_size, peer_scope.chunk_addressing_method)
                data_read = message.ParseReceivedData(received_data[data_parsed:])
                data_parsed = data_parsed + data_read
                messages.append(message)
            elif type == MT.ACK:
                message = MsgAck.MsgAck()
                message.ParseReceivedData(received_data[data_parsed:])
                data_parsed = data_parsed + 16
                messages.append(message)
            elif type == MT.INTEGRITY:
                message = MsgIntegrity.MsgIntegrity(peer_scope.hash_type)
                data_read = message.ParseReceivedData(received_data[data_parsed:])
                data_parsed = data_parsed + data_read
                messages.append(message)
            elif type == MT.HAVE:
                message = MsgHave.MsgHave()
                message.ParseReceivedData(received_data[data_parsed:data_parsed+8])
                data_parsed = data_parsed + 8
                messages.append(message)
            elif type == MT.REQUEST:
                message = MsgRequest.MsgRequest()
                message.ParseReceivedData(received_data[data_parsed:])
                data_parsed = data_parsed + 8
                messages.append(message)
            
            if message is None:
                logging.info("Unknown type {0} !!!".format(type))

            #logging.info("Received: {0}".format(message))

        return messages
