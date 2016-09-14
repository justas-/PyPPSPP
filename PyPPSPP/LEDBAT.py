import time
import math

class LEDBAT(object):
    """Implementation of LEDBAT [RFC6817] protocol"""
    TARGET = 0          # MAX queing delay LEDBAT can introduce
    GAIN = 0            # cwnd to delay response rate
    BASE_HISTORY = 16   #
    CURRENT_FILTER = 16 #
    INIT_CWND = 0       #
    MIN_CWND = 0        #
    MSS = 0             #

    def __init__(self):
        """Init protocol instance"""
        
        self._current_delays = []
        self._current_delays += LEDBAT.CURRENT_FILTER * [None]

        self._base_delays = []
        self._base_delays += LEDBAT.BASE_HISTORY * [float("inf")]

        self._last_rollover = -1 * float("inf")
        self._flightsize = 0
        self._cwnd = LEDBAT.INIT_CWND * LEDBAT.MSS
        self._cto = 1


    def GetDelay(self):
        """Get delay before sending the data"""
        pass

    def DataLoss(self, retransmit):
        # At most once per RTT
        self._cwnd = min([
            self._cwnd,
            max([
                self._cwnd / 2,
                LEDBAT.MIN_CWND * LEDBAT.MSS])])
        if not retransmit:
            self._flightsize -= self._bytes_newly_ackd

    def NoAckInCTO(self):
        self._cwnd = 1 * LEDBAT.MSS
        self._cto *= 2

    def FeedAck(self, delay):
        """Feed in one-way delay data to the protocol instance"""
        pass

    def _UpdateCurrentDelay(self, delay):
        # Maintain a list of CURRENT_FILTER last dealys observed
        self._current_delays = self._current_delays[1:]
        self._current_delays.append(delay)

    def _UpdateBaseDelay(self, delay):
        # Maintain BASE_HISTORY delay-minima
        # Each minimum is measured over a period of a minute
        if int(time.time()) != int(self._last_rollover):
            self._last_rollover = time.time()
            self._base_delays = self._base_delays[1:]
            self._base_delays.append(delay)
        else:
            tail = len(self._base_delays) - 1
            self._base_delays[tail] = min([
                self._base_delays[tail],
                delay])
