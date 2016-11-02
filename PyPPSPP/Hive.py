import binascii
import logging

from Swarm import Swarm

class Hive(object):
    """Hive stores all the Swarms operating in this node"""

    def __init__(self):
        self._swarms = {}
        self._orphan_connections = []

    def create_swarm(self, socket, args):
        """Initialize a new swarm in this node"""
        bin_swarmid = binascii.unhexlify(args.swarmid)
        if bin_swarmid in self._swarms:
            logging.warn("Trying to add same swarm twice! Swarm: {}".format(args.swarmid))
            return None

        self._swarms[bin_swarmid] = Swarm(socket, args)

        return self._swarms[bin_swarmid]

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
        