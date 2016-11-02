import logging
import asyncio
import binascii
import struct

import Swarm
import SwarmMember


class PeerProtocolUDP(asyncio.DatagramProtocol):
    """A class for use with Python asyncio library"""

    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self._num_msg_rx = 0
        self._logger = logging.getLogger()

    def connection_made(self, transport):
        # Called on acquiring the socket
        self.transport = transport
        logging.info("connection_made callback")

    def init_swarm(self, args):
        """Initialize the swarm"""
        self.swarm = Swarm.Swarm(self.transport, args)

    def datagram_received(self, data, addr):
        # Called on incomming datagram
        self._num_msg_rx = self._num_msg_rx + 1
        self.swarm._all_data_rx += len(data)

        # Keep this check
        if self._logger.isEnabledFor(logging.DEBUG):
            logging.debug("Datagram received ({0}). From: {1}; Len: {2}B"
                        .format(self._num_msg_rx, addr, len(data)))

        # Get the channel number
        my_channel = struct.unpack('>I', data[0:4])[0]

        if my_channel == 0:
            # This is new peer making connection to us
            new_member = self.swarm.AddMember(addr[0], addr[1])
            if new_member is None:
                logging.warn("NOT IMPLEMENTED: Clean old member. add this one!!!")
                return
            new_member.ParseData(data)
        else:
            # Try to find requested channel
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

    def pause_writing(self):
        logging.warn("PEER PROTOCOL IS OVER THE HIGH-WATER MARK")
    
    def resume_writing(self):
        logging.warn("PEER PROTOCL IS DRAINED BELOW THE HIGH-WATER MARK")

    def CloseProtocol(self):
        self.swarm.CloseSwarm()
        