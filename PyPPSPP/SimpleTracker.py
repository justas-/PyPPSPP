import logging
import socket
import random

class SimpleTracker(object):
    """This class abstracts a simple tracket. It can later be replaced with PPSP-TP"""

    def __init__(self):
        self._tracekr_protocol = None
        self._myip = self._get_my_ip()
        self._hive = None

    def set_hive(self, hive):
        """Link tracker to the hive"""
        self._hive = hive

    def SetTrackerProtocol(self, proto):
        self._tracekr_protocol = proto

    def ConnectionLost(self):
        """Connection to the tracker was lost..."""
        # We can continue operating even if connection to the tracker is lost
        return None

    def _get_my_ip(self):
        """Get My IP address. This is an awful hack"""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('1.1.1.1', 0))
        return s.getsockname()[0]

    def DataReceived(self, data):
        """Called with deserialized message from the tracker server"""

        swarm_id = None
        if 'swarm_id' not in data:
            logging.warn("Received data from tracker without swarm identifier. Data: {}"
                         .format(data))
            return

        swarm_id = data['swarm_id']
        swarm = self._hive.get_swarm(swarm_id)

        if swarm is None:
            logging.warn("Received data from tracker for unknown swarm! Swarm ID:{}"
                         .format(swarm_id))
            return

        if data['type'] == 'other_peers':
            self.handle_other_peers(swarm, data)
        else:
            logging.warn('Received unknown message from tracker! Message: {}'
                         .format(data))

    def handle_other_peers(self, swarm, message):
        """Handle 'Other peers' message"""        
        # We got information about other peers in the system
        if not any(message['details']):
            return

        # Add members in random order
        mem_copy = message['details']
        random.shuffle(mem_copy)

        for member in mem_copy:
            if swarm._args.tcp:
                # Check if we have connection already
                proto = self._hive.get_proto_by_address(member[0], member[1])
                if proto is not None:
                    # Connection to the given peer is already there - start handshake
                    member = swarm.AddMember(member[0], member[1], proto)
                    if member is not None:
                        member.SendHandshake()
                else:
                    # Initiate a new coonection to the given peer
                    self._hive.make_connection(member[0], member[1], swarm.swarm_id)
            else:
                m = swarm.AddMember(member[0], member[1])
                if m != None:
                    m.SendHandshake()

    def register_in_tracker(self, swarm_id: str, port: int):
        """Register with the tracker"""

        data = {}
        data['type'] = 'register'
        data['swarm_id'] = swarm_id
        data['endpoint'] = (self._myip, port)

        self._tracekr_protocol.SendData(data)

    def unregister_from_tracker(self, swarm_id: str):
        """Inform tracker that we are leaving"""
        
        data = {}
        data['type'] = 'unregister'
        data['swarm_id'] = swarm_id
        data['endpoint'] = (self._myip, 6778)

        self._tracekr_protocol.SendData(data)

    def get_peers(self, swarm_id: str):
        """Request list of peers from the tracker"""
        
        data = {}
        data['type'] = 'get_peers'
        data['swarm_id'] = swarm_id

        self._tracekr_protocol.SendData(data)