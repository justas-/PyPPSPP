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
import Framer

class PeerProtocolTCP(asyncio.Protocol):
    """TCP based communication protocol between the peers"""

    def __init__(self, hive, is_out = False):
        self._is_orphan = True
        self._transport = None
        self._hive = hive
        self._members = {}
        self._framer = Framer.Framer(self.data_deserialized)
        self._ip = None
        self._port = None
        self._throttle = False
        self._connection_id = hive._next_conn_id
        hive._next_conn_id += 1
        self._is_closed = False                     # Prevent closing socket after indication that it is already closed
        
        self._is_out = is_out

    @property
    def connection_id(self):
        """Return connection ID"""
        return self._connection_id

    def connection_made(self, transport):
        """Callback on connection establishment"""
        self._transport = transport
        self._ip, self._port = transport.get_extra_info('peername')

        # Do the logging
        if self._is_out:
            str_dir = 'OUTGOING'
        else:
            str_dir = 'INCOMING'
        logging.info('Established %s TCP connection (%s) with %s:%s',
                     str_dir, self._connection_id, self._ip, self._port)
        
        # If connection is incoming - place in the orphan list
        if not self._is_out:
            logging.info('Added connection %s to orphan list', self._connection_id)
            self._hive.add_orphan_connection(self)
            return

        # Notify all waiting swarms about new outgoing connection
        list_waiting = self._hive.check_if_waiting(self._ip, self._port)
        if list_waiting is not None:
            
            # Track of progress
            swarms_added = []
            swarms_failed = []

            for swarm_id in list_waiting:
                
                logging.info('Found swarm {} waiting for the connection'.format(swarm_id))
                swarm = self._hive.get_swarm(swarm_id)
                m = swarm.AddMember(self._ip, self._port, self)
                if isinstance(m, str):
                    logging.info('Swarm %s failed to add member: %s', swarm_id, m)
                    swarms_failed.append(swarm_id)
                else:
                    m.SendHandshake()
                    swarms_added.append(swarm_id)

            # Process results
            any_added = any(swarms_added)
            [list_waiting.remove(s) for s in swarms_added]
            [list_waiting.remove(s) for s in swarms_failed]

            # If no swarms added this as a member - disconnect connection
            if not any_added:
                self.force_close_connection()
        else:
            # Nobody is waiting for this connection
            self._hive.remove_orphan_connection(self)
            self._is_closed = True
            self._transport.close()
            
    def send_data(self, data):
        """Wrap data in framer's header and send it"""
        packet = bytearray()
        packet.extend(struct.pack('>I', len(data)))
        packet.extend(data)

        try:
            self._transport.write(packet)
        except Exception as exc:
            #logging.warn("Conn: {} Exception when sending: {}"
            #             .format(self._connection_id, e))
            logging.exception('Conn: %s Exception while sending', self._connection_id, exc_info=exc)
            self.remove_all_members()

    def data_received(self, data):
        """Called when data is received from the socket"""
        self._framer.DataReceived(data)

    def eof_received(self):
        logging.info('Connection: %s EOF received', self._connection_id)
        self.remove_all_members()
        return True

    def connection_lost(self, exc):
        logging.info("Connection %s lost: %s", self._connection_id, exc)
        self._is_closed = True

        # If exc is None - we closed it ourselves
        if exc is None:
            return

        self.remove_all_members()

    def pause_writing(self):
        logging.warn("PEER PROTOCOL IS OVER THE HIGH-WATER MARK")
        self._throttle = True

    def resume_writing(self):
        logging.warn("PEER PROTOCL IS DRAINED BELOW THE HIGH-WATER MARK")
        self._throttle = False

    def data_deserialized(self, data):
        """Called when Framer has enough data"""

        try:
            my_channel = struct.unpack('>I', data[0:4])[0]
        except Exception as exp:
            logging.error('Exception acessing deser data (my channel). Exception: {}; Data: {}; Peer: {}:{}'
                          .format(exp, data, self._ip, self._port))
            return

        if my_channel != 0 and self._is_orphan:
            logging.error("Orphan connection not sending a handshake!")
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

            try:
                # Goodbye?
                if data[4] == 0 and struct.unpack('>I', data[5:9])[0] == 0:
                    logging.info('Received goodbye from %s:%s (%s)', 
                        self._ip, self._port, self._connection_id)
                    self.remove_all_members()
                    return
            except Exception as exc:
                logging.error('Exception checking for goodbye message. Exception: {}; Data: {}; Peer: {}:{}'
                          .format(exp, data, self._ip, self._port))
                self.force_close_connection()
                return 
            
            try:
                swarm_id_len = struct.unpack('>H', data[14:16])[0]
                swarm_id = data[16:16+swarm_id_len]
            except Exception as exp:
                logging.error('Exception acessing deser data (swarm id len). Exception: {}; Data: {}; Peer: {}:{}'
                          .format(exp, data, self._ip, self._port))
                self.force_close_connection()
                return

            swarm_id_str = binascii.hexlify(swarm_id).decode('ascii')

            swarm = self._hive.get_swarm(swarm_id_str)
            if swarm is None:
                logging.warn("Did not find swarm with ID: {}".format(binascii.hexlify(swarm_id)))
            else:
                swarm._all_data_rx += len(data)
                m = swarm.AddMember(self._ip, self._port, self)
                if isinstance(m, str):
                    self.force_close_connection()
                    return
                else:
                    m.ParseData(data)

    def register_member(self, member):
        """Link a member object to a connection"""
        logging.info('Registering member: %s; Conn: %s', member, self._connection_id)

        if member.local_channel in self._members:
            logging.warning("Trying to register the same meber twice!")
            return
        else:
            self._members[member.local_channel] = member
            self._is_orphan = False

    def remove_all_members(self):
        """Unlink this proto from all members and remove all members from swarms"""

        logging.info('Removing all members for connection %s', self._connection_id)
        members_copy = self._members.copy()
        for member in members_copy.values():
            # Destroy the member and don't send disconnect because socket is gone
            member.destroy(False)
            member._swarm.RemoveMember(member)

        self._members.clear()

    def remove_member(self, member):
        """Remove given member from a list of linked members"""
        logging.info('Request to remove member %s from connection %s:%s (%s)',
                     member, self._ip, self._port, self._connection_id)

        if member.local_channel in self._members:
            member._proto = None
            del self._members[member.local_channel]

        if not any(self._members) and not self._is_closed:
            self._transport.close()

    def force_close_connection(self):
        """Close connection without any extra actions"""
        logging.info('Force-closing connection ({}) to: {}:{}'
                     .format(self._connection_id, self._ip, self._port))
        self._hive.remove_orphan_connection(self)
        self.remove_all_members()

        if not self._is_closed:
            self._transport.close()
