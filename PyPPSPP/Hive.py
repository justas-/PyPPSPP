import binascii
import logging
import asyncio

from Swarm import Swarm
from PeerProtocolTCP import PeerProtocolTCP

class Hive(object):
    """Hive stores all the Swarms operating in this node"""

    def __init__(self):
        self._swarms = {}
        self._orphan_connections = []
        self._pending_connection = {}

    def create_swarm(self, socket, args):
        """Initialize a new swarm in this node"""
        swarm_id = args.swarm_id

        if swarm_id in self._swarms:
            logging.warn("Trying to add same swarm twice! Swarm: {}".format(swarm_id))
            return None

        self._swarms[swarm_id] = Swarm(socket, args)

        return self._swarms[swarm_id]

    def get_swarm(self, swarm_id):
        """Get the indicated swarm from the swarms storage"""
        if swarm_id in self._swarms:
            return self._swarms[swarm_id]
        else:
            return None

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
        logging.info("Making connection to: {}:{}".format(ip, port))
        swarm = self.get_swarm(swarm_id)
        socket = swarm._socket
        
        # Make the connection
        loop = asyncio.get_event_loop()
        connect_coro = loop.create_connection(lambda: PeerProtocolTCP(self), ip, port)
        loop.create_task(connect_coro)

        logging.info("Connection initiated")

        # Add to a list of pending connectiosns
        # TODO: Check for duplicates
        self._pending_connection[(ip, port)] = [swarm_id]

    def check_if_waiting(self, ip, port):
        """Check if given connection is being awaited by any swarm"""
        if (ip, port) in self._pending_connection:
            return self._pending_connection[(ip, port)]
        else:
            return None
