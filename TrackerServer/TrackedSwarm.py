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

class TrackedSwarm(object):
    """A swarm in the PPSPP tracker"""

    def __init__(self, swarm_id):
        self.swarm_id = swarm_id
        self.members = {} # (IP, PORT) -> PROTO

    def add_member(self, ip, port, proto):
        """Add connection to the member"""

        if (ip, port) in self.members:
            logging.info("Member at {}:{} already present! Overwriting"
                         .format(ip, port))
        else:
            logging.info("Adding member {}:{}".format(ip, port))

        self.members[(ip, port)] = proto

    def remove_member(self, ip, port):
        """Remove the given member connection"""
        if (ip, port) in self.members:
            logging.info("Removing member {}:{}".format(ip, port))
            del self.members[(ip, port)]
        else:
            logging.info("Asked to remove non existing member: {}:{}"
                         .format(ip, port))

    def get_all_members_list(self):
        """Get a list with all member details"""
        return list(self.members.keys())
