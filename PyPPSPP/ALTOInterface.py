"""
Swarm to ALTO server interface.
"""
import logging
import ipaddress
import asyncio
import functools
import socket

import requests

class ALTOInterface(object):
    """Interface with ALTO server"""

    @staticmethod
    def get_my_ip():
        """Get My IP address. This is an awful hack"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(('1.1.1.1', 0))
        return sock.getsockname()[0]

    def __init__(self, alto_url, self_ip=None):
        """Initialize the class"""
        self._alto_url = alto_url
        if self_ip is None:
            self._self_ip = ALTOInterface.get_my_ip()
            logging.info('ALTO service detected self IP: %s', self._self_ip)
        else:
            self._self_ip = self_ip
        self._loop = asyncio.get_event_loop()

    def rank_sources(self, sources, cost_metric, callback):
        """Rank given sources to self_ip as destination using 
        the given cost type. Return results to callback"""

        # Build sources and destinations strings
        str_src = ['ipv4:{}'.format(ip) for ip in sources]
        str_dst = ['ipv4:{}'.format(self._self_ip)]

        # Make JSON serializable object
        req_obj = {
            "cost-type":{
                "cost-mode":"numerical",
                "cost-metric":cost_metric
            },
            "endpoints":{
                "srcs":str_src,
                "dsts":str_dst
            }
        }

        # Create task
        task = self._loop.create_task(self._alto.do_alto_post(
            '/endpointcost/lookup', req_obj, callback))

    @asyncio.coroutine
    def do_alto_post(self, endpoint, data, callback):
        """ALTO post to the given endpoint with given data"""

        # Make HTTP POST to ALTO
        url = self._alto_url + endpoint
        try:
            alto_resp_future = loop.run_in_executor(None, functools.partial(
                requests.post, url, json=data))
            alto_resp = yield from alto_resp_future
        except OSError as exc:
            logging.info('Consumer OSErrro while connecting to ALTO server')
            return

        # Process peers
        ranked_peers = self._process_alto_response(alto_resp)

        # Return results to swarm
        callback(ranked_peers)

    def _process_alto_response(self, alto_response):
        """Process data returned from ALTO"""

        if alto_response.status_code != requests.codes.ok:
            logging.warn('ALTO response HTTP code: %s', alto_response.status_code)
            return None

        data = alto_response.json()
        if not any(data['endpoint-cost-map']):
            logging.info('ALTO returned empty cost-map')
            return None

        results = {}
        for key, val in data['endpoint-cost-map']:
            # Extract source IP
            key_ip = key.strip('ipv4')
            (dest_ip, cost) = val.popitem()
            # Ensure that data is valid
            if dest_ip != self._self_ip:
                logging.warning('Costmap Destination not the same as our IP! %s != %s',
                                dest_ip, self._self_ip)
                continue
            results[key_ip] = cost

        # Return results if any
        if not any(results):
            return None
        else:
            return results
