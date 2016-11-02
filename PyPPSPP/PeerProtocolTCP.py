import logging
import asyncio
import binascii
import struct

import Swarm
import SwarmMember
import Framer

class PeerProtocolTCP(asyncio.Protocol):
    """TCP based communication protocol between the peers"""

    def __init__(self, hive):
        self._is_orphan = True
        self._transport = None
        self._hive = hive
        self._members = {}
        self._framer = Framer.Framer(self.data_deserialized)
        self._ip = None
        self._port = None

    def connection_made(self, transport):
        self._transport = transport
        self._ip, self._port = transport.get_extra_info('peername')
        self._hive.add_orphan_connection(self)
        logging.info("New TCP connection from {}:{}".format(self._ip, self._port))

    def send_data(self, data):
        """Wrap data in framer's header and send it"""
        packet = bytearray()
        packet.extend(pack('>I', len(data)))
        packet.extend(data)

        self._transport.write(packet)

    def data_received(self, data):
        """Called when data is received from the socket"""
        self._framer.DataReceived(data)

    def eof_received(self):
        return True

    def connection_lost(self, exc):
        logging.info("Connection lost")

    def pause_writing(self):
        logging.warn("PEER PROTOCOL IS OVER THE HIGH-WATER MARK")

    def resume_writing(self):
        logging.warn("PEER PROTOCL IS DRAINED BELOW THE HIGH-WATER MARK")

    def data_deserialized(self, data):
        """Called when Framer has enough data"""
        my_channel = struct.unpack('>I', data[0:4])[0]

        if my_channel != 0 and self._is_orphan:
            logging.warn("Orphan connection not sending a handshake!")
            return

        if my_channel != 0:
            # Find the required member by channel ID
            if my_channel in self._members:
                self._members[my_channel].ParseData(data)
            else:
                logging.warn("Got data for channel {}, but channel is not there!".format(my_channel))
        else:
            # Start creating new member in a swarm
            swarm_id_len = struct.unpack('>H', data[14:16])[0]
            swarm_id = data[16:16+swarm_id_len]

            swarm = self._hive.get_swarm(swarm_id)
            if swarm is None:
                logging.warn("Did not find swarm with ID: {}".format(binascii.hexlify(swarm_id)))
            else:
                swarm.AddMember(self._ip, self._port, self)