import time
import math
import logging

class LEDBAT(object):
    """Implementation of LEDBAT [RFC6817] protocol"""
    TARGET = 100        # MAX queing delay LEDBAT can introduce (ms)
    GAIN = 1            # cwnd to delay response rate
    BASE_HISTORY = 10   #
    CURRENT_FILTER = 4  #
    INIT_CWND = 2       #
    MIN_CWND = 2        #
    MSS = 1500          # Sender's Maximum Segment Size (Using MTU of Ethernet)
    ALLOWED_INCR = 1

    def __init__(self, data_size = 1000):
        """Init protocol instance"""
        
        self._current_delays = []
        self._current_delays += LEDBAT.CURRENT_FILTER * [None]

        self._base_delays = []
        self._base_delays += LEDBAT.BASE_HISTORY * [float("inf")]

        self._last_rollover = -1 * float("inf")
        self._flightsize = 0
        self._cwnd = LEDBAT.INIT_CWND * LEDBAT.MSS  # Amount of data that is outstanding in an RTT
        self._cto = 1                               # Congestion timeout value
        self._data_per_ack = data_size              # Data acked in each delay measurement
        self._last_ack_rx = None

    def GetDelay(self, outbound_data):
        """Get delay before sending the data"""
        if self._last_ack_rx != None:
            if time.time() - self._last_ack_rx > self._cto:
                NoAckInCTO()

        # No data in flight - we can send now
        if self._flightsize == 0:
            self._flightsize += outbound_data
            return 0

        return 0.2

    def DataLoss(self, retransmit = True):
        # At most once per RTT
        self._cwnd = min([self._cwnd, max([self._cwnd / 2, LEDBAT.MIN_CWND * LEDBAT.MSS])])
        #if not retransmit:
        #    self._flightsize -= self._bytes_newly_ackd

    def NoAckInCTO(self):
        self._cwnd = 1 * LEDBAT.MSS
        self._cto *= 2
        logging.info("LEDBAT: No ACK in CTO")

    def FeedAck(self, delays, num_acked = None): # Delays is [Delay]
        """Feed in one-way delay data to the protocol instance"""
        self._last_ack_rx = time.time()

        for d in delays:
            self._update_base_delay(d)
            self._update_current_delay(d)

        # Acknowledged data
        if num_acked == None:
            bytes_newly_acked = LEDBAT.MSS # Allowed in RFC 2.4.2
        else:
            bytes_newly_acked = num_acked * self._data_per_ack

        queuing_delay = self._filter(self._current_delays) - min(self._base_delays)
        off_target = (LEDBAT.TARGET - queuing_delay) / LEDBAT.TARGET
        self._cwnd += LEDBAT.GAIN * off_target * bytes_newly_acked * LEDBAT.MSS / self._cwnd
        max_allowed_cwnd = self._flightsize + LEDBAT.ALLOWED_INCR * LEDBAT.MSS
        self._cwnd = min([self._cwnd, max_allowed_cwnd])
        self._cwnd = max([self._cwnd, LEDBAT.MIN_CWND * MSS])
        self._flightsize = max([0, self._flightsize - bytes_newly_acked]) # Prevent negative flightsizes
        self._update_cto()

    def _update_cto():
        pass
    
    def _filter(self, data):
        # Filter function in LEDBAT.
        # Using MIN function over ceil(BASE_HISTORY / 4)
        num_to_filter = math.ceil(LEDBAT.BASE_HISTORY / 4)
        return min(data[-num_to_filter:])

    def _update_current_delay(self, delay):
        # Maintain a list of CURRENT_FILTER last dealys observed
        self._current_delays = self._current_delays[1:]
        self._current_delays.append(delay)

    def _update_base_delay(self, delay):
        # Maintain BASE_HISTORY delay-minima
        # Each minimum is measured over a period of a minute
        if int(time.time()) != int(self._last_rollover):
            self._last_rollover = time.time()
            self._base_delays = self._base_delays[1:]
            self._base_delays.append(delay)
        else:
            tail = len(self._base_delays) - 1   #### <= this is head, not tail
            self._base_delays[tail] = min([self._base_delays[tail], delay])
