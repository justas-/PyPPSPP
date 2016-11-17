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
        self._throttle = False

    def connection_made(self, transport):
        self._transport = transport
        self._ip, self._port = transport.get_extra_info('peername')

        logging.info("New TCP connection from {}:{}".format(self._ip, self._port))

        list_waiting = self._hive.check_if_waiting(self._ip, self._port)
        if list_waiting is None:
            self._hive.add_orphan_connection(self)
        else:
            for swarm_id in list_waiting:
                swarm = self._hive.get_swarm(swarm_id)
                m = swarm.AddMember(self._ip, self._port, self)
                if m is not None:
                    m.SendHandshake()

    def send_data(self, data):
        """Wrap data in framer's header and send it"""
        packet = bytearray()
        packet.extend(struct.pack('>I', len(data)))
        packet.extend(data)

        try:
            self._transport.write(packet)
        except Exception as e:
            logging.warn("Exception when sending: {}".format(e))
            self.remove_all_members()

    def data_received(self, data):
        """Called when data is received from the socket"""
        self._framer.DataReceived(data)

    def eof_received(self):
        self.remove_all_members()
        return True

    def connection_lost(self, exc):
        logging.info("Connection lost: {}".format(exc))
        self.remove_all_members()

    def pause_writing(self):
        logging.warn("PEER PROTOCOL IS OVER THE HIGH-WATER MARK")
        self._throttle = True

    def resume_writing(self):
        logging.warn("PEER PROTOCL IS DRAINED BELOW THE HIGH-WATER MARK")
        self._throttle = False

    def data_deserialized(self, data):
        """Called when Framer has enough data"""
        my_channel = struct.unpack('>I', data[0:4])[0]

        if my_channel != 0 and self._is_orphan:
            logging.warn("Orphan connection not sending a handshake!")
            return

        if my_channel != 0:
            # Find the required member by channel ID
            if my_channel in self._members:
                member = self._members[my_channel]
                member._swarm._all_data_rx += len(data)
                member.ParseData(data)
            else:
                logging.warn("Got data for channel {}, but channel is not there!".format(my_channel))
        else:
            # Start creating new member in a swarm
            swarm_id_len = struct.unpack('>H', data[14:16])[0]
            swarm_id = data[16:16+swarm_id_len]

            swarm_id_str = binascii.hexlify(swarm_id).decode('ascii')

            swarm = self._hive.get_swarm(swarm_id_str)
            if swarm is None:
                logging.warn("Did not find swarm with ID: {}".format(binascii.hexlify(swarm_id)))
            else:
                swarm._all_data_rx += len(data)
                m = swarm.AddMember(self._ip, self._port, self)
                if m is not None:
                    m.ParseData(data)

    def register_member(self, member):
        """Link a member object to a connection"""
        if member.local_channel in self._members:
            logging.warn("Trying to register the same meber twice!")
            return
        else:
            self._members[member.local_channel] = member
            self._is_orphan = False

    def remove_all_members(self):
        """Unlink this proto from all members and remove all members from swarms"""

        members_copy = self._members.copy()
        for member in members_copy:
            # Destroy the member and don't send disconnect because socket is gone
            member.destroy(False)
            member._swarm.RemoveMember(member)

    def remove_member(self, member):
        """Remove given member from a list of linked members"""
        if member.local_channel in self._members:
            member._proto = None
            del self._members[member.local_channel]

        if not any(self._members):
            self._transport.close()
