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