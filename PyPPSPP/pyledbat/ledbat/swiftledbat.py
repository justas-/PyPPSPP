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
LEDBAT Implementation following libswift[1] approach.

NOTES: set SwiftLedbat.last_send_time = NOW when
actually sending data.

Most of the implementation of libswift's LEDBAT is in [2].

[1] - https://github.com/libswift/libswift
[2] - https://github.com/libswift/libswift/blob/master/send_control.cpp
"""
import time

from ledbat import baseledbat

class SwiftLedbat(baseledbat.BaseLedbat):
    """Extends the BaseLedbat class to implement features
       not specified in the [RFC6817]
    """

    def __init__(self):
        """Extend the class with our specific parameters"""

        # Swiftish
        self.last_send_time = None      # Last data out timestamp

        self._next_send_time = None     # Next time data should go out
        self._last_data_time = None     # Last time data out was requested
        self._reschedule_delay = 0      # Delay adjustment if this is delayed call

        # Init the base class
        super().__init__()

    def data_sent(self, data_len):
        """Inform LEDBAT about data sent to the network"""

        self._flightsize += data_len
        self._next_send_time = time.time()

    def try_sending(self, data_len):
        """Check if data can be sent. If data can be sent now, (True, None) will be returned.
           If data cannot be sent now - (False, time) will be returned. Send after time.
           After data was sent and new data piece is ready - start over.
        """

        # Swiftish implementation
        t_now = time.time()

        # Last client wanted to send data
        self._last_data_time = t_now

        # Check for extreme congestion
        if (self._last_ack_received != None and
                self._flightsize > 0 and
                t_now - self._last_ack_received > self._cto):

            # Ack wasn't there...
            self._no_ack_in_cto()

        # Check if we have any RT measurements? (Slow start some-day)
        if self._rtt is None:
            # Send now
            self._flightsize += data_len
            self._next_send_time = t_now
            return (True, None)

        # Check for Reschedule delay
        if self.last_send_time is not None and self._next_send_time is not None:
            if self.last_send_time > self._next_send_time and self._next_send_time < t_now:
                self._reschedule_delay = t_now - self._next_send_time

        # Get next send time
        send_interval = self._rtt / (self._cwnd / data_len)
        if (self._flightsize + data_len < self._cwnd or
                self._cwnd >= baseledbat.BaseLedbat.MIN_CWND * baseledbat.BaseLedbat.MSS):

            # Calculate when next send can happen (might be in the past)
            self._next_send_time = self._last_data_time + send_interval - self._reschedule_delay
        else:
            # ??
            self._next_send_time = self._last_data_time + 1.0   # X + ack_timeout()

        t_dif = self._next_send_time - t_now
        if t_dif <= 0:
            # Send now
            self._flightsize += data_len
            self._next_send_time = t_now
            return (True, None)
        else:
            # Send later
            return (False, t_dif)
