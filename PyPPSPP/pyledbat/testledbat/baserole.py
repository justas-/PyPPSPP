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
"""Base class that is extended by Client and Server roles"""

class BaseRole(object):
    """BaseRole with minimal actions what are common
       for both client and the server.
    """

    def __init__(self, udp_protocol):
        # Save reference to the receiver
        self._udp_protocol = udp_protocol

        # Inform receiver to deliver data to us
        self._udp_protocol.register_receiver(self)

        # Keep all tests here. LocalID -> ledbat_test
        self._tests = {}

    def datagram_received(self, data, addr):
        """Callback on received datagram"""
        pass

    def send_data(self, data, addr):
        """Send the data to the indicated addr"""
        self._udp_protocol.send_data(data, addr)

    def remove_test(self, test):
        """Remove the given test from the list of tests"""
        del self._tests[test.local_channel]
