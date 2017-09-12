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

import asyncio
import json
from struct import pack
from Framer import Framer

class TrackerClientProtocol(asyncio.Protocol):
    """Asyncio client protocol implementation for TCP connection to PPSPP tracker"""

    def __init__(self, tracker):
        self._transport = None
        self._tracker = tracker
        self._framer = Framer(self._OnData)

    def connection_made(self, transport):
        """Called when connection to the tracker is established"""
        self._transport = transport

    def connection_lost(self, exc):
        """Called when connection to Tracekr is gone"""
        self._tracker.connection_lost()

    def data_received(self, data):
        """Pass all received bytes to framer"""
        self._framer.DataReceived(data)
        
    def SendData(self, data):
        """Write bytes to the TCP stream"""
        obj_bytes = json.dumps(data).encode()

        packet = bytearray()
        packet.extend(pack('>I', len(obj_bytes)))
        packet.extend(obj_bytes)

        self._transport.write(packet)

    def _OnData(self, data):
        """Called with single serialized message from the framer"""
        self._tracker.data_received(json.loads(data.decode()))