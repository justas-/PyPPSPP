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

import binascii
import logging
import asyncio
import traceback

from Swarm import Swarm
from PeerProtocolTCP import PeerProtocolTCP

class Hive(object):
    """Hive stores all the Swarms operating in this node"""

    def __init__(self):
        self._swarms = {}
        self._orphan_connections = []
        self._pending_connection = {}
        self._next_conn_id = 1

    def create_swarm(self, socket, args):
        """Initialize a new swarm in this node"""
        swarm_id = args.swarmid

        if swarm_id in self._swarms:
            logging.warn("Trying to add same swarm twice! Swarm: {}".format(swarm_id))
            return None

        self._swarms[swarm_id] = Swarm(socket, args)

        return self._swarms[swarm_id]

    def get_swarm(self, swarm_id):
        """Get the indicated swarm from the swarms storage"""
        return self._swarms.get(swarm_id)

    def add_orphan_connection(self, proto):
        """Add a connection until it is owned"""
        self._orphan_connections.append(proto)

    def remove_orphan_connection(self, proto):
        """Remove connection once it is owned"""
        
        try:
            self._orphan_connections.remove(proto)
        except:
            pass

    def get_proto_by_address(self, ip, port):
        """Get connection to given peer if present"""
        for swarm in self._swarms.values():
            for member in swarm._members:
                if member.ip_address == ip and member.udp_port == port and member._is_udp == False:
                    return member._proto

        return None

    def make_connection(self, ip, port, swarm_id):
        """Strat the outgoing connection and inform the given swarm once done"""

        swarm_id_str = binascii.hexlify(swarm_id).decode('ascii')
        logging.info('Request for connection to: {}:{} for swarm: {}'
                     .format(ip, port, swarm_id_str))
        
        # Check if swarm id is valid
        swarm = self.get_swarm(swarm_id_str)
        if swarm is None:
            logging.warn('Swarm {} not found. Connection will not be made!'.format(swarm_id_str))
            return

        # Check if connection is already initiated
        pending = self._pending_connection.get((ip, port))
        if pending is not None:
            # There are already connection pending to the endpoint
            if swarm_id_str in pending:
                logging.info('Connection to {}:{} for swarm {} is already pending'
                             .format(ip, port, swarm_id_str))
                return

        # Make the connection
        asyncio.get_event_loop().create_task(self.do_safe_conn(ip, port))
        logging.info('Connection coro to {}:{} created'.format(ip, port))

        # Add to a list of pending connectiosns
        if pending is None:
            # No connections pending - make list with our swarm id
            self._pending_connection[(ip, port)] = [swarm_id_str]
        else:
            # Some connections already pending - append our ID string
            self._pending_connection[(ip, port)].append(swarm_id_str)

    @asyncio.coroutine
    def do_safe_conn(self, ip, port):
        """Coro wrapper to catch exceptions, when the remote peer timesout"""

        loop = asyncio.get_event_loop()

        try:
            yield from loop.create_connection(
                lambda: PeerProtocolTCP(self, True), ip, port)
        except OSError as exc:
            logging.info('Consumed OS Exception: %s', exc)
            logging.info('Removing %s:%s from the pending connections list', ip, port)
            del self._pending_connection[(ip, port)]
        except:
            traceback.print_exc()

    def check_if_waiting(self, ip, port):
        """Check if given connection is being awaited by any swarm"""
        return self._pending_connection.get((ip, port))

    def close_all_swarms(self):
        """Close all swarms in the Hive"""

        for swarm in self._swarms.values():
            swarm.close_swarm()

        self._swarms.clear()
