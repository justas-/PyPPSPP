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
Implementation of the client role in the test application.
"""

import asyncio
import random
import struct
import logging
import time

from testledbat import baserole
from testledbat import ledbat_test

class ClientRole(baserole.BaseRole):
    """description of class"""

    def datagram_received(self, data, addr):
        """Process the received datagram"""

        # save time of reception
        rx_time = time.time()

        # Extract the header
        (msg_type, rem_ch, loc_ch) = struct.unpack('>III', data[0:12])

        if msg_type == 1 and rem_ch == 0:
            logging.warning('Client should not get INIT messages')
            return

        # Get the LEDBAT test
        ledbattest = self._tests.get(rem_ch)
        if ledbattest is None:
            logging.warning('Could not find ledbat test with our id: %s', rem_ch)
            return

        if msg_type == 1:       # INIT-ACK
            ledbattest.init_ack_received(loc_ch)
        elif msg_type == 2:     # DATA
            logging.warning('Client should not receive DATA messages')
        elif msg_type == 3:     # ACK
            ledbattest.ack_received(data[12:], rx_time)
        else:
            logging.warning('Discarded unknown message type (%s) from %s', msg_type, addr)

    def start_client(self, **kwargs):
        """Start the functioning of the client by starting a new test"""

        # Create instance of this test
        test_args = {
            'is_client':True,
            'remote_ip':kwargs.get('remote_ip'),
            'remote_port':kwargs.get('remote_port'),
            'owner':self,
            'make_log':kwargs.get('make_log'),
            'log_name':kwargs.get('log_name'),
            'ledbat_params':kwargs.get('ledbat_params'),
            'log_dir':kwargs.get('log_dir'),
            'stream_id':None,
        }

        total_streams = kwargs.get('parallel')

        # Run required number of tests
        for stream_id in range(0, total_streams):

            # Leave stream id -> None if running only one
            if total_streams != 1:
                test_args['stream_id'] = stream_id

            ledbattest = ledbat_test.LedbatTest(**test_args)
            ledbattest.local_channel = random.randint(1, 65534)

            # Save in the list of tests
            self._tests[ledbattest.local_channel] = ledbattest

            # Schedule test stop if required
            test_len = kwargs.get('test_len')
            if test_len:
                ledbattest.stop_hdl = asyncio.get_event_loop().call_later(
                    test_len, self._stop_test, ledbattest)

            # Send the init message to the server
            ledbattest.start_init()

    def _stop_test(self, test):
        """Stop the given test"""
        logging.info('Time to stop test: %s', test)
        test.stop_test()
        test.dispose()

    def remove_test(self, test):
        """Extend remove_test to close client when the last test is removed"""
        super().remove_test(test)

        if not self._tests:
            logging.info('Last test removed. Closing client')
            asyncio.get_event_loop().stop()

    def stop_all_tests(self):
        """Request to stop all tests"""

        # Make copy not to iterate over list being removed
        tests_copy = self._tests.copy()
        for test in tests_copy.values():
            test.stop_test()
            test.dispose()
