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
Wrapper class implementing simple RT and RTT measurements.
"""
import time
import math
import enum

from . import baseledbat

class FailReason(enum.Enum):
    """Fail Reason Enumerator"""
    NOFAIL = 0
    CTO = 1
    CWND = 2

class SimpleLedbat(baseledbat.BaseLedbat):
    """Simple implementation of LEDBAT"""

    # [RFC6298] values
    COEF_G = 0.1
    COEF_K = 4

    # alpha, beta per Jacobson, V. "Congestion avoidance and control"
    # doi: 10.1145/52325.52356
    COEF_ALPHA = 0.125
    COEF_BETA = 0.25

    @property
    def cwnd(self):
        """Get Congestion Window Size"""
        return self._cwnd

    @property
    def flightsize(self):
        """Get amount of data in-flight (sent but not ACKed)"""
        return self._flightsize

    @property
    def rtt(self):
        """Get Round-trip time estimate"""
        if self._rtt is None:
            return 0
        else:
            return self._rtt

    @property
    def queuing_delay(self):
        """Get queuing delay estimate"""
        return self._queuing_delay

    @property
    def srtt(self):
        """Get smoothed-rtt value"""
        return self._srtt

    @property
    def rttvar(self):
        """Get rtt variance value"""
        return self._rttvar

    @property
    def cto(self):
        """Get Congestion timeout value"""
        return self._cto

    def __init__(self, **kwargs):
        """Init the required variables"""

        self._last_send_time = None     # When data was actually sent last time
        self._last_cto_fail_time = None # When last CTO fail happened
        self._in_cto = False            # Are we in CTO mode

        # [RFC6298]
        self._rt_measured = False  # Flag to check if the first measurement was done
        self._srtt = None
        self._rttvar = None

        super().__init__(**kwargs)

    def try_sending(self, data_len):
        """Implement data sending gating and congestion check"""

        time_now = time.time()

        if self._last_send_time is None:
            # By definition we can *always* send the first segment
            self._flightsize += data_len
            self._last_send_time = time_now
            return (True, FailReason.NOFAIL)

        # CTO check
        if self._last_ack_received is not None:
            # At least one ACK was received

            # Check if we are under heavy congestion
            if self._in_cto:
                # We are in CTO at the moment

                if self._last_ack_received > time_now - self._cto:
                    # We RXd some ACKs within CTO -> no longer in CTO, check CWND
                    self._in_cto = False

                else:
                    # No ACKs in CTO, Further checking

                    if self._last_cto_fail_time + self._cto > time_now:
                        # We are still in CTO, do not send
                        return (False, FailReason.CTO)
                    else:
                        # We are out of congestion, try sending if CWND allows
                        self._in_cto = False
            
            else:
                # We are not in congestion, check if there is congestion
                if ((self._last_ack_received + self._cto < time_now) and    # The actual congestion check
                    (self._last_send_time + (2 * self.rtt) > time_now)):   # Allow some time after we leave CTO for ACKs to arrive
                    
                    # Congestion

                    if (self._last_cto_fail_time is None or
                        self._last_cto_fail_time + self._cto < time_now):
                        # We never failed CTO check before
                        self._last_cto_fail_time = time_now
                        self._no_ack_in_cto()
                    
                    # Congested -> No sending
                    self._in_cto = True
                    return (False, FailReason.CTO)

                else:
                    # No Congestion
                    pass
        else:
            # So far no ACKs received - assume no congestion, check CWND
            pass

        # Check congestion window check
        if self._flightsize + data_len <= self._cwnd:
            # We can send data
            self._flightsize += data_len
            self._last_send_time = time_now
            return (True, FailReason.NOFAIL)
        else:
            # Will have to wait
            return (False, FailReason.CWND)

    def update_measurements(self, data_acked, ow_times, rt_times):
        """Update LEDBAT calculations. data_acked - number of bytes acked,
        if None, will be num of ow_times * MSS, ow_limes - array of one-way
        delay measurements (oldest to newest), rt_time - round-trip time
        measurements, oldest to newest"""

        if data_acked is None:
            num_data = len(ow_times) * self.MSS
        else:
            num_data = data_acked

        self._ack_received(num_data, ow_times, rt_times)

    def _update_cto(self, rtt_values):
        """Calculate Congestion Timeout value"""

        # Code here is lifted from [RFC6298] with RTO in [RFC6298]
        # meaning self._cto here. rt_measurements is [float]

        # Filter value to the lowest value, as some measurements should be void
        # when using delayed ACKs

        # NOP if no valid rtt_values (Karn's Algorithm)
        if not any(rtt_values):
            return

        rtt = min(rtt_values)

        if not self._rt_measured:
            # Set the params per [RFC6298]
            self._srtt = rtt
            self._rttvar = rtt / 2
            self._cto = self._srtt + max([self.COEF_G, self.COEF_K * self._rttvar])

            # Update state
            self._rt_measured = True

        else:
            # Update CTO based on round trip time measurements
            self._rttvar = (1 - self.COEF_BETA) * self._rttvar + self.COEF_BETA * math.fabs(self._srtt - rtt)
            self._srtt = (1 - self.COEF_ALPHA) * self._srtt + self.COEF_ALPHA * rtt
            self._cto = self._srtt + max(self.COEF_G, self.COEF_K * self._rttvar)

        # Per [RFC6298] p2.4
        if self._cto < 1.0:
            self._cto = 1.0

        self._rtt = rtt
