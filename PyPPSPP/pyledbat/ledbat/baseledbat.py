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
This is a base implementation of LEDBAT following the [RFC6817] for LEDBAT
specification. This file is not enough on its own, and must be extended to
gate the sending. An example of such extending is provided by simpleledbat
implementation and in the test application.
"""

import time
import datetime
import math
import logging

class BaseLedbat(object):
    """Base class with constante defined"""

    CURRENT_FILTER = 8          # Number of elements in current delay filter
    BASE_HISTORY = 10           # Number of elements in base delay history
    INIT_CWND = 2               # Number of MSSes in initial cwnd value
    MSS = 1500                  # Maximum segment size
    TARGET = 50                 # Target in milliseconds. Per [RFC6817] must be <= 100ms
    GAIN = 1                    # Congestion window to delay response rate
    ALLOWED_INCREASE = 1
    MIN_CWND = 2
    LOSS_CWND_FRAC = 0.5        # Fraction of CWND change on data loss

    def __init__(self, **kwargs):
        """Initialize the instance"""
        self._current_delays = BaseLedbat.CURRENT_FILTER * [1000000]
        self._base_delays = BaseLedbat.BASE_HISTORY * [float('inf')]
        self._flightsize = 0
        self._cwnd = BaseLedbat.INIT_CWND * BaseLedbat.MSS  # Congestion window
        self._last_rollover = time.time()                   # Time last base-delay rollover occured
        self._cto = 1                                       # Congestion timeout (seconds)
        self._queuing_delay = 0
        self._rtt = None                                # Round Trip Time
        self._last_data_loss = 0                        # When was latest dataloss event observed
        self._last_ack_received = None                  # When was the last ACK received

        # Change defaults if given:
        for key, value in kwargs.items():
            if key == 'set_current_filter':
                BaseLedbat.CURRENT_FILTER = value
            elif key == 'set_base_history':
                BaseLedbat.BASE_HISTORY = value
            elif key == 'set_init_cwnd':
                BaseLedbat.INIT_CWND = value
            elif key == 'set_mss':
                BaseLedbat.MSS = value
            elif key == 'set_target':
                BaseLedbat.TARGET = value
            elif key == 'set_gain':
                BaseLedbat.GAIN = value
            elif key == 'set_allowed_increase':
                BaseLedbat.ALLOWED_INCREASE = value
            elif key == 'set_min_cwnd':
                BaseLedbat.MIN_CWND = value
            elif key == 'set_loss_cwnd_frac':
                BaseLedbat.LOSS_CWND_FRAC = value
            else:
                # Fall through option so logging is not done
                continue

            logging.info('LEDBAT parameter changed: %s => %s', key, value)

        # Callback on no ACK in CTO
        self._cb_cto = kwargs.get('cb_cto')

    @property
    def filter_current(self):
        """Get the output of the filter algorithm"""
        return self._filter_alg(self._current_delays)

    @property
    def min_base(self):
        """Get the minimal value of base delays"""
        return min(self._base_delays)

    def _ack_received(self, bytes_acked, ow_delays, rtt_delays):
        """Parse the received delay sample(s)
           delays is milliseconds, rt_measurements in seconds!
        """

        # Process all received delay samples
        for delay_sample in ow_delays:
            self._update_base_delay(delay_sample)
            self._update_current_delay(delay_sample)

        # Update values
        self._queuing_delay = self._filter_alg(self._current_delays) - min(self._base_delays)
        off_target = (BaseLedbat.TARGET - self._queuing_delay) / BaseLedbat.TARGET
        self._cwnd += int(BaseLedbat.GAIN * off_target * bytes_acked * BaseLedbat.MSS / self._cwnd)
        max_allowed_cwnd = self._flightsize + BaseLedbat.ALLOWED_INCREASE * BaseLedbat.MSS
        self._cwnd = min([self._cwnd, max_allowed_cwnd])
        self._cwnd = max([self._cwnd, BaseLedbat.MIN_CWND * BaseLedbat.MSS])
        self._flightsize = max([0, self._flightsize - bytes_acked])

        self._update_cto(rtt_delays)

    def data_loss(self, will_retransmit=True, loss_size=None):
        """Reduce cwnd if data loss is experienced"""

        # Get the current time
        t_now = time.time()

        if loss_size is None:
            loss_size = BaseLedbat.MSS

        # Prevent calling too often
        if self._last_data_loss != 0:
            if t_now - self._last_data_loss < self.rtt:
                # At most once per RTT
                return

        # Save time when last dataloss event happened
        self._last_data_loss = t_now

        # Reduce the congestion window size
        self._cwnd = min([
            self._cwnd,
            int(max([
                self._cwnd * BaseLedbat.LOSS_CWND_FRAC,
                BaseLedbat.MIN_CWND * BaseLedbat.MSS]))
        ])

        # Account for data in-flight
        if not will_retransmit:
            self._flightsize = self._flightsize - loss_size

    def _no_ack_in_cto(self):
        """Update CWND if no ACK was received in CTO"""

        self._cwnd = 1 * BaseLedbat.MSS
        self._cto = 2 * self._cto

        # Any actions?
        if self._cb_cto is not None:
            self._cb_cto()

    def _update_cto(self, rtt_values):
        """Calculate congestion timeout (CTO)"""
        pass

    def _filter_alg(self, filter_data):
        """Implements FILTER() algorithm"""

        # Implemented per [RFC6817] MIN filter over a small window
        # multiplied by -1 to get latest window_size values
        window_size = -1 * math.ceil(self.BASE_HISTORY/4)
        return min(filter_data[window_size:])

    def _update_base_delay(self, delay):
        """Update value in base_delay tracker list"""

        t_now = time.time()

        # Implemented per [RFC6817]
        minute_now = datetime.datetime.fromtimestamp(t_now).minute
        minute_then = datetime.datetime.fromtimestamp(self._last_rollover).minute

        if minute_now != minute_then:
            # Shift value at next minute
            self._last_rollover = t_now
            self._base_delays = self._base_delays[1:]
            self._base_delays.append(delay)
        else:
            # For each measurements during the same minute keep minimum value
            # at the end of the list
            self._base_delays[-1] = min([self._base_delays[-1], delay])

    def _update_current_delay(self, delay):
        """Add new value to the current delays list"""

        # Implemented per [RFC6817]
        self._current_delays = self._current_delays[1:]
        self._current_delays.append(delay)
