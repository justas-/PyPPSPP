import logging
import json

# Tracker does not care about formats - it expects deserialized objects
# and give unserialized data for tsp to deliver.
class Tracker(object):
    """PPSPP clients tracker"""

    def __init__(self):
        self._clients = {}  # (ip, port) -> tsp
        return None

    def ConnectionCreated(self, tsp):
        return None

    def DataReceived(self, tsp, data):
        logging.debug("Tracker data received. tsp: {0}, data: {1}".format(tsp, data))
        if data['type'] == 'register':
            self._HandleRegister(tsp, data)
            self._InformAboutOthers(tsp)
            return
        if data['type'] == 'unregister':
            endpoint = tuple(data['endpoint'])
            self._HandleUnregister(endpoint)
            return

    def ConnectionClosed(self, tsp):
        # Find the TSP and inform everyone about lost node
        for k, v in self._clients.items():
            if v == tsp:
                self._HandleUnregister(k)
                return

    def _HandleRegister(self, tsp, data):
        # Add new TSP to a dict of peers
        endpoint = tuple(data['endpoint'])
        self._clients[endpoint] = tsp

        logging.info("Registered node residing at: {0}".format(endpoint))

        # Create message about new node
        msg = {}
        msg['type'] = 'new_node'
        msg['endpoint'] = endpoint

        # Send it to all other nodes
        for other_tsp in [x for x in self._clients.values() if x != tsp]:
             other_tsp.SendData(msg)

    def _HandleUnregister(self, endpoint):
        tsp = self._clients.pop(endpoint)

        logging.info("Unregistered node residing at: {0}".format(endpoint))

        # Create message about new node
        msg = {}
        msg['type'] = 'remove_node'
        msg['endpoint'] = endpoint

        # Send it to all other nodes
        for other_tsp in self._clients.values():
             other_tsp.SendData(msg)
        
    def _InformAboutOthers(self, who_tsp):
        """Inform node at who_tsp about other nodes"""

        # Create message object
        msg = {}
        msg['type'] = 'other_peers'
        msg['details'] = []
        
        # Fill in details of other nodes
        for k in [k for k,v in self._clients.items() if v != who_tsp]:
            msg['details'].append(k)

        who_tsp.SendData(msg)
