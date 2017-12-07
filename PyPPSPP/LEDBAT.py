"""
PyPPSPP, a Python3 implementation of Peer-to-Peer Streaming Peer Protocol
Copyright (C) 2016,2017  J. Poderys, Technical University of Denmark

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import time
import datetime
import math
import logging

class LEDBAT(object):
    """Implementation of LEDBAT [RFC6817] protocol"""
    TARGET = 100 * 1000 # MAX queing delay LEDBAT can introduce x1000 since all other in uS
    GAIN = 1            # cwnd to delay response rate
    BASE_HISTORY = 10   #
    CURRENT_FILTER = 4  #
    INIT_CWND = 2       #
    MIN_CWND = 2        #
    MSS = 1500          # Sender's Maximum Segment Size (Using MTU of Ethernet)
    ALLOWED_INCR = 1

    ALPHA = 1/8
    BETA = 1/4
    K = 4

    def __init__(self, data_size = 1000):
        """Init protocol instance"""
        
        self._current_delays = []
        self._current_delays += LEDBAT.CURRENT_FILTER * [1000000]

        self._base_delays = []
        self._base_delays += LEDBAT.BASE_HISTORY * [float("inf")]

        self._last_rollover = time.time()
        self._flightsize = 0
        self._cwnd = LEDBAT.INIT_CWND * LEDBAT.MSS  # Amount of data that is outstanding in an RTT
        self._cto = 1                               # Congestion timeout value
        self._data_per_ack = data_size              # Data acked in each delay measurement
        self._last_ack_rx = None
        self._qd = 0

        self._first_est = True
        self._rttvar = 0
        self._srtt = 0
        self._g = 10

        self._last_dataloss = 0
        self._last_datasend = 0

    def get_delay(self, outbound_data):
        """Get delay before sending the data"""
        # No data in flight - we can send now
        if self._flightsize == 0:
            self._flightsize += outbound_data
            self._last_datasend = time.time()
            return 0

        # Data in flight is lower that cwnd - send now
        self._flightsize += outbound_data
        if self._flightsize < self._cwnd:
            self._last_datasend = time.time()
            return 0

        # This was okeyish
        return self._cto / 1000000

        # Send interval since last send 
        #send_interval = (self._qd / 10) / self._cwnd
        #delay = (self._last_datasend + send_interval) - time.time()
        #return max([0, delay])

    def data_loss(self, retransmit = True):
        """Reduce cwnd if experiencing data loss"""
        if self._last_dataloss == 0 or time.time() - self._last_dataloss > (self._qd / 1000000):
            self._cwnd = min([
                self._cwnd, 
                max([self._cwnd / 2, LEDBAT.MIN_CWND * LEDBAT.MSS])])
            self._last_dataloss = time.time()
            #logging.info("Data loss experienced")

    def feed_ack(self, delays, num_acked = None): # Delays is [Delay]
        """Feed in one-way delay data to the protocol instance"""
        t_now = time.time()
        if self._last_ack_rx is not None and (t_now - self._last_ack_rx) > self._cto:
            self._no_ack_in_cto()
        self._last_ack_rx = t_now

        for d in delays:
            self._update_base_delay(d)
            self._update_current_delay(d)

        # Acknowledged data
        if num_acked == None:
            bytes_newly_acked = LEDBAT.MSS # Allowed in RFC 2.4.2
        else:
            bytes_newly_acked = num_acked * self._data_per_ack

        queuing_delay = self._filter(self._current_delays) - min(self._base_delays)
        self._qd = queuing_delay
        off_target = (LEDBAT.TARGET - queuing_delay) / LEDBAT.TARGET
        self._cwnd += LEDBAT.GAIN * off_target * bytes_newly_acked * LEDBAT.MSS / self._cwnd
        max_allowed_cwnd = self._flightsize + LEDBAT.ALLOWED_INCR * LEDBAT.MSS
        self._cwnd = min([self._cwnd, max_allowed_cwnd])
        self._cwnd = max([self._cwnd, LEDBAT.MIN_CWND * LEDBAT.MSS])
        self._flightsize = max([0, self._flightsize - bytes_newly_acked]) # Prevent negative flightsizes
        self._update_cto()

    def _no_ack_in_cto(self):
        # JP: OK
        self._cwnd = 1 * LEDBAT.MSS
        self._cto *= 2

    def _update_cto(self):
        # JP: One big WTF
        if self._first_est:
            self._srtt = self._qd
            self._rttvar = self._qd / 2
            self._cto = self._srtt + max([self._qd, LEDBAT.K * self._rttvar])
        else:
            self._rttvar = (1 - LEDBAT.BETA) * self._rttvar + LEDBAT.BETA * math.fabs(self._srtt - self._qd)
            self._srtt = (1 - LEDBAT.ALPHA) * self._srtt + LEDBAT.ALPHA * self._qd
            self._cto = self._srtt + max([self._g, LEDBAT.K * self._rttvar])

    def _filter(self, data):
        # JP: OK
        # Filter function in LEDBAT.
        # Using MIN function over ceil(BASE_HISTORY / 4)
        num_to_filter = -1 * math.ceil(LEDBAT.BASE_HISTORY / 4)
        return min(data[num_to_filter:])

    def _update_current_delay(self, delay):
        # JP: OK
        # Maintain a list of CURRENT_FILTER last dealys observed
        self._current_delays = self._current_delays[1:]
        self._current_delays.append(delay)

    def _update_base_delay(self, delay):
        # JP: OK
        # Maintain BASE_HISTORY delay-minima
        # Each minimum is measured over a period of a minute
        minute_now = datetime.datetime.fromtimestamp(time.time()).minute
        minute_then = datetime.datetime.fromtimestamp(self._last_rollover).minute
        if minute_now != minute_then:
            self._last_rollover = time.time()
            self._base_delays = self._base_delays[1:]
            self._base_delays.append(delay)
        else:
            # last = min(last, delay)
            self._base_delays[-1] = min([self._base_delays[-1], delay])
