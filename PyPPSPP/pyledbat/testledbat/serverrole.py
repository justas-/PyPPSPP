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
Server class for LEDBAT test. Server acts as a "dumb" client by ACKIN data only.
All protocol intelligence is in the client. One server can be replying to multipe
clients concurrently.
"""
import logging
import struct
import random
import time

from testledbat import ledbat_test
from testledbat import baserole

class ServerRole(baserole.BaseRole):
    """description of class"""

    def start_server(self):
        """Start acting as a server"""
        pass

    def datagram_received(self, data, addr):
        """Process the received datagram"""

        # Take time msg received for later use
        rx_time = time.time()

        # Extract the header
        (msg_type, rem_ch, loc_ch) = struct.unpack('>III', data[0:12])

        # Either init new test or get the running test
        if msg_type == 1 and rem_ch == 0:
            self._test_init_req(loc_ch, addr)
            return
        else:
            # All other combinations must have remote_channel set
            this_test = self._tests.get(rem_ch)

            if this_test is None:
                logging.warning('Could not find ledbat test with our id: %s', rem_ch)
                return

            if msg_type == 1:
                logging.warning('Server should not receive INIT-ACK')
            elif msg_type == 2:
                this_test.data_received(data[12:], rx_time)
            elif msg_type == 3:
                logging.warning('Server should not receive ACK message')
            else:
                logging.warning('Discarded unknown message type (%s) from %s', msg_type, addr)

    def _test_init_req(self, their_channel, addr):
        """Initialize new test as requested"""

        # This is attempt to start a new test
        test_args = {
            'is_client':False,
            'remote_ip':addr[0],
            'remote_port':addr[1],
            'owner':self,
            'make_log':None,
            'log_name':None,
            'ledbat_params':{},
            'log_dir':None,
        }
        lebat_test = ledbat_test.LedbatTest(**test_args)
        lebat_test.remote_channel = their_channel
        lebat_test.local_channel = random.randint(1, 65534)

        # Add to a list of tests
        self._tests[lebat_test.local_channel] = lebat_test

        # Send INIT-ACK
        lebat_test.send_init_ack()
