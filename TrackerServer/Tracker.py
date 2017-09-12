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
import json

import TrackedSwarm

# Tracker does not care about formats - it expects deserialized objects
# and give unserialized data for tsp to deliver.
class Tracker(object):
    """PPSPP clients tracker"""

    def __init__(self):
        self.swarms = {} # swarm_id -> TrackedSwarm

    def ConnectionCreated(self, tsp):
        return None

    def DataReceived(self, tsp, data):
        logging.debug("Tracker data received. tsp: {0}, data: {1}".format(tsp, data))

        if 'swarm_id' not in data:
            logging.warn("Received data without swarm identifier!")
            return

        if 'type' not in data:
            logging.warn("Received data without type identifier!")
            return

        swarm_id = data['swarm_id']
        type = data['type']

        # Create new swarm if not present
        if swarm_id not in self.swarms:
            if type == 'register':
                self.swarms[swarm_id] = TrackedSwarm.TrackedSwarm(swarm_id)
            else:
                logging.warn("Swarm not known and first message is not register!")
                return

        swarm = self.swarms[swarm_id]

        if type == 'register':
            self.handle_register(swarm, data, tsp)
        elif type == 'unregister':
            self.handle_unregister(swarm, data, tsp)
        elif type == 'get_peers':
            self.handle_get_peers(swarm, data, tsp)
        else:
            logging.warn("Unknown received message type!")
        
    def handle_register(self, swarm, data, proto):
        """Register a member in the swarm.
           Distribute this information to other members
        """

        # Add a new member
        swarm.add_member(data['endpoint'][0], data['endpoint'][1], proto)

        # Create message for other peers
        msg = {}
        msg['swarm_id'] = swarm.swarm_id
        msg['type'] = 'new_node'
        msg['endpoint'] = tuple(data['endpoint'])

        # Send this to all others
        for other_peer in [x for x in swarm.members.values() if x != proto]:
            other_peer.SendData(msg)

    def handle_unregister(self, swarm, data, proto):
        """Handle the unregister message received from the node"""

        # Remove the member from the members list
        swarm.remove_member(data['endpoint'][0], data['endpoint'][1])

        # Build a leaving message
        msg = {}
        msg['swarm_id'] = swarm.swarm_id
        msg['type'] = 'remove_node'
        msg['endpoint'] = tuple(data['endpoint'])

        # Inform other nodes
        for peer in swarm.members.values():
            peer.SendData(msg)

    def handle_get_peers(self, swarm, data, proto):
        """Handle the get peers message"""
        
        # Create the message object
        msg = {}
        msg['swarm_id'] = swarm.swarm_id
        msg['type'] = 'other_peers'
        msg['details'] = []

        # Update the members part in the message
        for k in [k for k,v in swarm.members.items() if v != proto]:
            msg['details'].append(k)

        # Send the final message
        proto.SendData(msg)

    def ConnectionClosed(self, tsp):
        # TODO: Decide should we inform that node is gone if connection is gone
        pass
