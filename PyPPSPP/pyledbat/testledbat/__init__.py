"""
Copyright 2017, J. Poderys, Technical University of Denmark

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
"""
Ledbat testing application.
"""

import logging
import asyncio
import socket
import os

from testledbat import udpserver
from testledbat import clientrole
from testledbat import serverrole

UDP_PORT = 6888

def test_ledbat(params):
    """
    Entry function for LEDBAT testing application.
    """

    # Validate the params
    if params.role == 'client' and params.remote is None:
        logging.error('Address of the remote server must be provided for the client role!')
        return

    # Prevent negative test times
    if not params.time or params.time < 0:
        params.time = None

    # Run at least one client
    if not params.parallel or params.parallel < 1:
        params.parallel = 1

    ledbat_params = None

    # Print debug information
    if params.role == 'client':
        # Extract any ledbat overwrites
        ledbat_params = extract_ledbat_params(params)

        if params.time:
            str_test_len = '{} s.'.format(params.time)
        else:
            str_test_len = 'Unlimited'

        logging.info('Starting LEDBAT test client. Remote: %s; Length: %s;',
                     params.remote, str_test_len)
    else:
        logging.info('Starting LEDBAT test server.')

    if params.makelog:
        log_info = ''
        if params.log_dir:
            if params.log_name:
                log_info = ' ({})'.format(os.path.join(params.log_dir, params.log_name+'.csv'))
            else:
                log_info = ' ({})'.format(params.log_dir)
        else:
            if params.log_name:
                log_info = ' ({})'.format(params.log_name+'.csv')

        logging.info('Run-time values will be saved to the log file%s', log_info)

    # Init the events loop and udp transport
    loop = asyncio.get_event_loop()
    listen = loop.create_datagram_endpoint(udpserver.UdpServer, local_addr=('0.0.0.0', UDP_PORT))
    transport, protocol = loop.run_until_complete(listen)

    # Enable Ctrl-C closing in WinNT
    # Ref: http://stackoverflow.com/questions/24774980/why-cant-i-catch-sigint-when-asyncio-event-loop-is-running
    if os.name == 'nt':
        def wakeup():
            loop.call_later(0.5, wakeup)
        loop.call_later(0.5, wakeup)

    # Start the instance based on the type
    if params.role == 'client':
        # Run the client
        client = clientrole.ClientRole(protocol)
        client.start_client(remote_ip=params.remote,
                            remote_port=UDP_PORT,
                            make_log=params.makelog,
                            log_name=params.log_name,
                            log_dir=params.log_dir,
                            test_len=params.time,
                            ledbat_params=ledbat_params,
                            parallel=params.parallel)
    else:
        # Do the Server thing
        server = serverrole.ServerRole(protocol)
        server.start_server()

    # Wait for Ctrl-C
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    if params.role == 'client':
        client.stop_all_tests()

    # Cleanup
    transport.close()
    loop.close()

def extract_ledbat_params(parameters):
    """Extract LEDBAT settings"""

    ledbat_params = {}

    for attr, value in vars(parameters).items():
        try:
            if attr.startswith('ledbat') and value is not None:
                ledbat_params[attr[attr.index('_')+1:]] = value
        except IndexError:
            # Better check your typos next time...
            logging.info('Failed to parse LEDBAT param: %s', attr)

    return ledbat_params
