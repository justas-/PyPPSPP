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
import os
import json
from struct import pack

from Framer import Framer
from Tracker import Tracker

class TrackerServerProtocol(asyncio.Protocol):
    def __init__(self, tracker):
        """Ctor"""
        logging.debug("TSP - ctor")
        self._tracker = tracker
        self._framer = Framer(self._OnData)
        self._transport = None

    def connection_made(self, transport):
        """Called when new PPSPP client connects"""
        logging.debug("TSP - connection made. Transport: {0}".format(transport))
        self._transport = transport
        self._tracker.ConnectionCreated(self)
        
    def data_received(self, data):
        """Called when data is received from the PPSPP node"""
        logging.debug("TSP - data received. Data: {0}".format(data))
        self._framer.DataReceived(data)

    def connection_lost(self, exc):
        """Connection from a node is lost"""
        logging.debug("TSP - connection lost. Exc: {0}".format(exc))
        self._tracker.ConnectionClosed(self)

    def SendData(self, data):
        """Send binary data to the connected peer"""
        self._transport.write(self._Serialize(data))

    def _OnData(self, data):
        """Helper to pass TPS object to tracker instance"""
        self._tracker.DataReceived(self, self._Deserialize(data))

    def _Serialize(self, obj):
        """Serialize given object to binary data"""
        obj_bytes = json.dumps(obj).encode()

        packet = bytearray()
        packet.extend(pack('>I', len(obj_bytes)))
        packet.extend(obj_bytes)

        return packet

    def _Deserialize(self, bytes):
        """Deserialize given bytes to an object"""
        return json.loads(bytes.decode())

# Configure logger
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
logging.info("PPSPP Tracker server starting")

# Asyncio event loop
loop = asyncio.get_event_loop()
loop.set_debug(True)

# Create a tracker instance
tracker = Tracker()

coro = loop.create_server(lambda: TrackerServerProtocol(tracker), '0.0.0.0', 6777)
server = loop.run_until_complete(coro)

# Schedule wakeups to catch Ctrl+C in Win32
# This should be fixed in Python 3.5 
# Ref: http://stackoverflow.com/questions/24774980/why-cant-i-catch-sigint-when-asyncio-event-loop-is-running
if os.name == 'nt':
    def wakeup():
        # Call again later
        loop.call_later(0.5, wakeup)
    loop.call_later(0.5, wakeup)

try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

server.close()
loop.run_until_complete(server.wait_closed())
loop.close()