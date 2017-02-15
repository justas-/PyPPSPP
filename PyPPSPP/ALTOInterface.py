"""
Swarm to ALTO server interface.
"""
import logging
import ipaddress
import asyncio
import functools

import requests

class ALTOInterface(object):
    """Interface with ALTO server"""

    def __init__(self, alto_url, self_ip):
        """Initialize the class"""
        self._alto_url = alto_url
        self._self_ip = self_ip

        self._network_map = None
        self._cost_map = None

    @asyncio.coroutine
    def do_alto_post(self, endpoint, data):
        """ALTO post to the given endpoint with given data"""

        loop = asyncio.get_event_loop()
        url = self._alto_url + endpoint
        alto_resp_future = loop.run_in_executor(None, functools.partial(
            requests.post, url, json = data))
        alto_resp = yield from alto_resp_future
        return alto_resp

    @asyncio.coroutine
    def do_alto_get(self, endpoint, data, callback):
        """ALTO gert to the given endpoint with given data"""

        loop = asyncio.get_event_loop()
        url = self._alto_url + endpoint

        # Execute request in the task executor
        alto_resp_future = loop.run_in_executor(None, functools.partial(
            requests.get, url, json = data))
        alto_resp = yield from alto_resp_future

        # Deliver back the data
        callback(alto_resp)

    @asyncio.coroutine    
    def get_costs(self, source, destination):
        """Get costs from [source] to [destination] IPs"""

        yield from do_alto_post('/costs', {})
        logging.info(alto_resp)


    def get_networkmap(self):
        """Download the network map from the ALTO server"""
        # Get data from ALTO
        url = self._alto_url + "/networkmap"
        resp_raw = requests.get(url)
        response = resp_raw.json()

        # Parse into Python objects
        nm = {}
        for pid_name, v in response['network-map'].items():
            subnets = []
            for subnet in v['ipv4']:
                subnets.append(ipaddress.IPv4Network(subnet))
            nm[pid_name] = subnets

        # Set for use later
        self._network_map = nm

    def get_costmap(self):
        """Download the cost map from the ALTO server"""
        url = self._alto_url + "/costmap/numerical/routingcost"
        resp_raw = requests.get(url)
        response = resp_raw.json()
        self._cost_map = response['cost-map']
        
    def get_cost_by_ip(self, from_ip, to_ip):
        """Get routing cost by given IP addresses"""
        # Convert from-IP to from-PID
        pid_from = self.get_pid_by_ip(from_ip)
        if pid_from is None:
            return None

        # Convert to-IP to to-PID
        pid_to = self.get_pid_by_ip(to_ip)
        if pid_to is None:
            return None

        return self.get_cost_by_pid(pid_from, pid_to)

    def get_cost_by_pid(self, from_pid, to_pid):
        """Get routing cost by given PIDs"""
        if from_pid in self._cost_map:
            from_branch = self._cost_map[from_pid]
            if to_pid in from_branch:
                return from_branch[to_pid]
        
        return None

    def get_pid_by_ip(self, ip):
        """Get PID by given IP address""" # ipnet.network_address._ip == ipadr._ip & ipnet.netmask._ip
        ip_addr = ipaddress.IPv4Address(ip)
        for pid_name, net_list in self._network_map.items():
            for net in net_list:
                if net.network_address._ip == ip_addr._ip & net.netmask._ip:
                    return pid_name
        return None