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
import asyncio
import binascii
import struct

import Swarm
import SwarmMember


class PeerProtocolLedbatUDP(asyncio.DatagramProtocol):
    """A class for use with Python asyncio library"""

    def __init__(self, hive):
        self.loop = asyncio.get_event_loop()
        self._num_msg_rx = 0
        self._logger = logging.getLogger()
        self._hive = hive

    def connection_made(self, transport):
        # Called on acquiring the socket
        self.transport = transport
        logging.info("LEDBAT UDP Proto Connection Made")

    def datagram_received(self, data, addr):
        # Called on incomming datagram
        self._num_msg_rx = self._num_msg_rx + 1

        # Get the channel number
        my_channel = struct.unpack('>I', data[0:4])[0]
        member = self._hive.get_member_by_channel(my_channel)

        if member != None:
            if len(data) == 4:
                # This is keepalive
                member.GotKeepalive()
            else:
                member.ParseData(data)
        else:
            logging.warning("LEDBAT Socket data received but member not found. Channel: %s; Received from: %s; Datalen: %s",
                            my_channel, addr, len(data))

    def send_data(self, data, addr):
        """Send data over the socket"""
        #print("ledbat transport sent {} B data to {}".format(len(data), addr))
        self.transport.sendto(data, addr)

    def error_received(self, exc):
        logging.exception("LEDBAT Socket Error received", exc_info=exc)

    def connection_lost(self, exc):
        logging.exception("LEDBAT Socket closed", exc_info=exc)

    def pause_writing(self):
        logging.warn("PEER PROTOCOL IS OVER THE HIGH-WATER MARK")
    
    def resume_writing(self):
        logging.warn("PEER PROTOCL IS DRAINED BELOW THE HIGH-WATER MARK")
        