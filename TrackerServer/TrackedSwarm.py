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
