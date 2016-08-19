import logging
import asyncio
import binascii
import struct

import Swarm
import SwarmMember


class PeerProtocol(object):
    """A class for use with Python asyncio library"""

    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self._num_msg_rx = 0

    def connection_made(self, transport):
        # Called on acquiring the socket
        self.transport = transport
        logging.info("connection_made callback")

        # We have connected socket. Start creating a swarm
        # TODO: Now only one swarm is supported
        # TODO: Now swarm ID is hardcoded
        swarm_id = binascii.unhexlify("82d3614b17dcac7624e58b2bee9bca1580a87b75")
        swarm_filename = "HelloWorld.txt"
        swarm_file_size = 33
        self.swarm = Swarm.Swarm(self.transport, swarm_id, swarm_filename, swarm_file_size)

        # TODO: Now we add the bootstrap member
        m = self.swarm.AddMember("127.0.0.1", 8888)
        m.SendHandshake()

    def datagram_received(self, data, addr):
        # Called on incomming datagram
        self._num_msg_rx = self._num_msg_rx + 1
        logging.info("Datagram received ({0}). From: {1}; Len: {2}B"
                     .format(self._num_msg_rx, addr, len(data)))

        # Get the channel number
        my_channel = struct.unpack('>I', data[0:4])[0]

        if my_channel == 0:
            # This is new peer making connection to us
            new_member = self.swarm.AddMember(addr[0], addr[1])
            new_member.ParseData(data)
        else:
            # Tr to find requested channel
            member = self.swarm.GetMemberByChannel(my_channel)

            if member != None:
                if len(data) == 4:
                    # This is keepalive
                    member.GotKeepalive()
                else:
                    member.ParseData(data)
            else:
                logging.warning("Received data to non-existant channel: {0}".format(my_channel))

    def error_received(self, exc):
        logging.warning("Error received: {0}".format(exc))

    def connection_lost(self, exc):
        logging.critical("Socket closed: {0}".format(exc))
        loop.stop()
        