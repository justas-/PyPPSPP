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
LedbatTest - class representing a single test instance and used in both client
and server.
"""
import logging
import asyncio
import struct
import time
import csv
import os

from ledbat import simpleledbat
from .inflight_track import InflightTrack

T_INIT_ACK = 5.0    # Time to wait for INIT-ACK
T_INIT_DATA = 5.0   # Time to wait for DATA after sending INIT-ACK
T_IDLE = 10.0       # Time to wait when idle before destroying

SZ_DATA = 1024      # Data size in each message
OOO_THRESH = 3      # When to declare dataloss
PRINT_EVERY = 5000  # Print debug every this many packets sent
LOG_INTERVAL = 0.1  # Log every 0.1 sec

class LedbatTest(object):
    """An instance representing a single LEDBAT test"""

    def __init__(self, **kwargs):

        self._is_client = kwargs.get('is_client')
        self._remote_ip = kwargs.get('remote_ip')
        self._remote_port = kwargs.get('remote_port')
        self._owner = kwargs.get('owner')
        self._make_log = kwargs.get('make_log')
        self._ledbat_params = kwargs.get('ledbat_params')
        self._log_dir = kwargs.get('log_dir')
        self._log_name = kwargs.get('log_name')
        self._stream_id = kwargs.get('stream_id')

        self._ev_loop = asyncio.get_event_loop()

        self._num_init_sent = 0
        self._hdl_init_ack = None       # Receive INIT-ACK after ACK

        self._num_init_ack_sent = 0
        self._hdl_act_to_data = None    # Receive DATA after INIT-ACK

        self._hdl_send_data = None      # Used to schedule data sending
        self._hdl_idle = None           # Idle check handle

        self._ledbat = simpleledbat.SimpleLedbat(**self._ledbat_params)
        self._next_seq = 1

        self._inflight = InflightTrack()
        self._cnt_ooo = 0               # Count of Out-of-Order packets

        self.is_init = False
        self.local_channel = None
        self.remote_channel = None

        self._time_start = None
        self._time_stop = None
        self._time_last_rx = None

        self.stats = {}
        self.stats['Init'] = False
        self.stats['Sent'] = 0
        self.stats['Ack'] = 0
        self.stats['Resent'] = 0
        self.stats['OooPkt'] = 0
        self.stats['DupPkt'] = 0
        self.stats['LostPkt'] = 0
        self.stats['SentPrev'] = 0
        self.stats['AckPrev'] = 0
        self.stats['ResentPrev'] = 0
        self.stats['TPrev'] = 0
        self.stats['GateSent'] = 0
        self.stats['GateWaitCTO'] = 0
        self.stats['GateWaitCWND'] = 0
        self.stats['GateSentPrev'] = 0
        self.stats['GateWaitCTOPrev'] = 0
        self.stats['GateWaitCWNDPrev'] = 0
        self.stats['OooPktPrev'] = 0
        self.stats['DupPktPrev'] = 0
        self.stats['LostPktPrev'] = 0

        # Run periodic checks if object should be removed due to being idle
        # JP: Disable as test timeout in severe congestionconditions
        self._hdl_idle = None # self._ev_loop.call_later(T_IDLE, self._check_for_idle)

        self._hdl_log = None
        self._log_data_list = []

        self.stop_hdl = None    # Stop event handle (if any)

    def start_init(self):
        """Start the test initialization procedure"""

        # We can start init only when er are the client
        assert self._is_client

        # Prevent re-starting the test
        assert not self.is_init

        # Send the init message
        self._build_and_send_init()

        # Schedule re-sender / disposer
        self._hdl_init_ack = self._ev_loop.call_later(T_INIT_ACK, self._check_for_idle)

    def _check_for_idle(self):
        """Periodic check to see if idle test should be removed"""

        if self._time_last_rx is None or time.time() - self._time_last_rx > T_IDLE:
            logging.info('%s Destroying due to being idle', self)
            self.dispose()
        else:
            self._hdl_init_ack = self._ev_loop.call_later(T_INIT_ACK, self._check_for_idle)

    def _build_and_send_init(self):
        """Build and send the INIT message"""

        # Build the message
        msg_bytes = bytearray(12)
        struct.pack_into('>III', msg_bytes, 0,
                         1,                     # Type - ACK
                         0,                     # Remote Channel
                         self.local_channel     # Local channel
                        )

        # Send it to the remote
        self._owner.send_data(msg_bytes, (self._remote_ip, self._remote_port))
        self._num_init_sent += 1

        # Print log
        logging.info('%s Sent INIT message (%s)', self, self._num_init_sent)

    def _init_ack_missing(self):
        """Called by when init ACK not received within time interval"""

        # Keep resending INIT for up to 3 times
        if self._num_init_sent < 3:
            self._build_and_send_init()
            self._hdl_init_ack = self._ev_loop.call_later(T_INIT_ACK, self._init_ack_missing)
        else:
            # After 3 time, dispose the test
            logging.info('%s INI-ACK missing!', self)
            self.dispose()

    def send_init_ack(self):
        """Send the INIT-ACK reply to INIT"""

        # Send INIT-ACK
        self._build_and_send_init_ack()

        # Start timer to wait for data
        self._hdl_act_to_data = self._ev_loop.call_later(T_INIT_DATA, self._init_data_missing)

    def _init_data_missing(self):
        """Called when DATA is not received after INIT-ACK"""

        # Keep resending INIT-ACK up to 3 times
        if self._num_init_ack_sent < 3:
            self._build_and_send_init_ack()
            self._hdl_act_to_data = self._ev_loop.call_later(T_INIT_DATA, self._init_data_missing)
        else:
            # After 3 times dispose
            logging.info('%s DATA missing after 3 INIT-ACK', self)
            self.dispose()

    def _build_and_send_init_ack(self):
        """Build and send INI-ACK message"""

        # Build message bytes
        msg_bytes = bytearray(12)
        struct.pack_into('>III', msg_bytes, 0,
                         1,                     # Type
                         self.remote_channel,   # Remote channel
                         self.local_channel     # Local channel
                        )

        # Send it
        self._owner.send_data(msg_bytes, (self._remote_ip, self._remote_port))
        self._num_init_ack_sent += 1

        # Print log
        logging.info('%s Sent INIT-ACK message (%s)', self, self._num_init_ack_sent)

    def init_ack_received(self, remote_channel):
        """Handle INIT-ACK message from remote"""

        # Update time of latest datain
        self._time_last_rx = time.time()

        # Check if we are laready init
        if self.is_init:
            # Ignore message, most probably duplicate
            return
        else:
            # Save details. We are init now
            self.remote_channel = remote_channel
            self.is_init = True

            logging.info('%s Test initialized', self)

            # Cancel timer
            self._hdl_init_ack.cancel()
            self._hdl_init_ack = None

            # Start the TEST
            self._start_test()

    def _start_test(self):
        """Start testing"""

        # Take time when starting
        self._time_start = time.time()

        # Scedule sending event on the loop
        logging.info('%s Starting test', self)
        self._hdl_send_data = self._ev_loop.call_soon(self._try_next_send)

        self._log_data()

    def _log_data(self):
        """Make LOG entry"""

        # Cancel pending if any
        if self._hdl_log is not None:
            self._hdl_log.cancel()

        time_now = time.time()

        if not self.stats['Init']:
            stats = {
                'Time': time_now,
                'dT': 0,
                'Sent': self.stats['Sent'],
                'Resent': self.stats['Resent'],
                'Acked': self.stats['Ack'],
                'OooPkt': self.stats['OooPkt'],
                'DupPkt': self.stats['DupPkt'],
                'LostPkt': self.stats['LostPkt'],
                'Cwnd': self._ledbat.cwnd,
                'FlightSz': self._ledbat.flightsize,
                'QueuingDly': 0,
                'Rtt': 0,
                'Srtt': 0,
                'Rttvar': 0,
                'Cto': self._ledbat.cto,
                'dSent': 0,
                'dResent': 0,
                'dAck': 0,
                'dGateSent': 0,
                'dGateWaitCTO': 0,
                'dGateWaitCWND': 0,
                'dOooPkt': 0,
                'dDupPkt': 0,
                'dLostPkt' : 0,
            }
            self.stats['Init'] = True
        else:
            stats = {
                'Time': time_now,
                'dT': time_now - self.stats['TPrev'],
                'Sent': self.stats['Sent'],
                'Resent': self.stats['Resent'],
                'Acked': self.stats['Ack'],
                'OooPkt': self.stats['OooPkt'],
                'DupPkt': self.stats['DupPkt'],
                'LostPkt': self.stats['LostPkt'],
                'Cwnd': self._ledbat.cwnd,
                'FlightSz': self._ledbat.flightsize,
                'QueuingDly': self._ledbat.queuing_delay,
                'Rtt': self._ledbat.rtt,
                'Srtt': self._ledbat.srtt,
                'Rttvar': self._ledbat.rttvar,
                'Cto': self._ledbat.cto,
                'dSent': self.stats['Sent'] - self.stats['SentPrev'],
                'dResent': self.stats['Resent'] - self.stats['ResentPrev'],
                'dAck': self.stats['Ack'] - self.stats['AckPrev'],
                'dGateSent': self.stats['GateSent'] - self.stats['GateSentPrev'],
                'dGateWaitCTO': self.stats['GateWaitCTO'] - self.stats['GateWaitCTOPrev'],
                'dGateWaitCWND': self.stats['GateWaitCWND'] - self.stats['GateWaitCWNDPrev'],
                'dOooPkt': self.stats['OooPkt'] - self.stats['OooPktPrev'],
                'dDupPkt': self.stats['DupPkt'] - self.stats['DupPktPrev'],
                'dLostPkt' : self.stats['LostPkt'] - self.stats['LostPktPrev'],
            }

        self._log_data_list.append(stats)

        self.stats['TPrev'] = time_now
        self.stats['SentPrev'] = self.stats['Sent']
        self.stats['AckPrev'] = self.stats['Ack']
        self.stats['ResentPrev'] = self.stats['Resent']
        self.stats['GateSentPrev'] = self.stats['GateSent']
        self.stats['GateWaitCTOWPrev'] = self.stats['GateWaitCTO']
        self.stats['GateWaitCWNDPrev'] = self.stats['GateWaitCWND']
        self.stats['OooPktPrev'] = self.stats['OooPkt']
        self.stats['DupPktPrev'] = self.stats['DupPkt']
        self.stats['LostPktPrev'] = self.stats['LostPkt']

        # Schedule next call
        self._hdl_log = self._ev_loop.call_later(LOG_INTERVAL, self._log_data)

    def _save_log(self):
        """Save log to the file"""

        if self._log_name:
            if self._stream_id is None:
                filename = '{}.csv'.format(self._log_name)
            else:
                filename = '{}-stream-{}.csv'.format(
                    self._log_name, self._stream_id)
        else:
            if self._stream_id is None:
                filename = '{}-{}-{}.csv'.format(
                    int(self._time_start),
                    self._remote_ip,
                    self._remote_port)
            else:
                filename = '{}-{}-{}-stream-{}.csv'.format(
                    int(self._time_start),
                    self._remote_ip,
                    self._remote_port,
                    self._stream_id)

        if self._log_dir:
            filepath = os.path.join(self._log_dir, filename)
        else:
            filepath = filename

        with open(filepath, 'w', newline='') as fp_csv:
            fields = list(self._log_data_list[0].keys())
            csvwriter = csv.DictWriter(fp_csv, fieldnames = fields)
            csvwriter.writeheader()

            # Write all rows
            for row in self._log_data_list:
                csvwriter.writerow(row)

    def stop_test(self):
        """Stop the test and print results"""

        logging.info('%s Request to stop!', self)
        self._time_stop = time.time()
        self._print_status()

        # Make the last log entry
        if self._log_data:
            self._log_data()
            if self._hdl_log is not None:
                self._hdl_log.cancel()
            self._save_log()

    def _try_next_send(self):
        """Try sending next data segment. This implementation sends data
           as fast as possible using polling. A more elegant solution using
           semaphore would be nicer.
        """

        # SZ_DATA + 24 Bytes for header
        (can_send, reason) = self._ledbat.try_sending(SZ_DATA + 24)
        if can_send:
            self.stats['GateSent'] += 1
            self._build_and_send_data()

            # Print stats
            if self.stats['Sent'] % PRINT_EVERY == 0:
                self._print_status()
        else:
            if reason == simpleledbat.FailReason.CTO:
                self.stats['GateWaitCTO'] += 1
            elif reason == simpleledbat.FailReason.CWND:
                self.stats['GateWaitCWND'] += 1

        self._hdl_send_data = self._ev_loop.call_soon(self._try_next_send)

    def _print_status(self):
        """Print status during sending"""

        # Calculate values
        if self._time_start is None:
            test_time = 0
        else:
            test_time = time.time() - self._time_start

        # Prevent div/0 early on
        if test_time == 0:
            return

        all_sent = self.stats['Sent'] + self.stats['Resent']
        tx_rate = all_sent / test_time

        # Print data
        logging.info('Time: %.2f TX/ACK/RES: %s/%s/%s TxR: %.2f',
                     test_time, self.stats['Sent'], self.stats['Ack'],
                     self.stats['Resent'], tx_rate)

    def _send_data(self, seq_num, time_sent, data):
        """Frame given data and send it"""

        # Build the header
        msg_data = bytearray()
        msg_data.extend(struct.pack(
            '>IIIIQ', # Type, Rem_ch, Loc_ch, Seq, Timestamp
            2,
            self.remote_channel,
            self.local_channel,
            seq_num,
            int(time_sent * 1000000)))

        if data is None:
            msg_data.extend(SZ_DATA * bytes([127]))
        else:
            msg_data.extend(data)

        # Send the message
        self._owner.send_data(msg_data, (self._remote_ip, self._remote_port))

    def _build_and_send_data(self):
        """Build and send data message"""

        # Set useful vars
        time_now = time.time()
        seq_num = self._next_seq
        self._next_seq += 1

        # Build and send message
        self._send_data(seq_num, time_now, None)

        # Add to in-flight tracker
        self._inflight.add(seq_num, time_now, None)

        # Update stats
        self.stats['Sent'] += 1

    def data_received(self, data, receive_time):
        """Handle the DATA message for this test"""

        # Update time of latest datain
        self._time_last_rx = time.time()

        # If we are acceptor, update the stat
        if not self._is_client and not self.is_init:
            if self._hdl_act_to_data is not None:
                self._hdl_act_to_data.cancel()
                self._hdl_act_to_data = None

            self.is_init = True
            logging.info('%s Got first data. Test is init', self)

        # data is binary data _without_ the header
        (seq, time_stamp) = struct.unpack('>IQ', data[0:12])

        # Get the delay
        one_way_delay = (receive_time * 1000000) - time_stamp

        # Send ACK, no delays/grouping
        self._send_ack(seq, seq, [one_way_delay])

    def _send_ack(self, ack_from, ack_to, one_way_delays):
        """Build and send ACK message"""

        msg_bytes = bytearray()

        # Header
        msg_bytes.extend(struct.pack('>III', 3, self.remote_channel, self.local_channel))

        # ACK data
        msg_bytes.extend(struct.pack('>II', ack_from, ack_to))

        # Delay samples
        num_samples = len(one_way_delays)
        msg_bytes.extend(struct.pack('>I', num_samples))
        for sample in one_way_delays:
            msg_bytes.extend(struct.pack('>Q', int(sample)))

        # Send ACK
        self._owner.send_data(msg_bytes, (self._remote_ip, self._remote_port))

    def _resend_indicated(self, resendable_list):
        """Resend items with given SEQ numbers"""

        for seq_num in resendable_list:
            (_, _, data) = self._inflight.get_item(seq_num)
            self._send_data(seq_num, time.time(), data)
            self._inflight.set_resent(seq_num)
            self.stats['Resent'] += 1

    def ack_received(self, ack_data, rx_time):
        """Handle the ACK"""

        delays = []
        rtts = []

        # Update time of latest datain
        self._time_last_rx = rx_time

        # Extract the data
        (ack_from, ack_to, num_delays) = struct.unpack('>III', ack_data[0:12])

        # Do not process duplicates
        if ack_to < self._inflight.peek():
            self.stats['DupPkt'] += 1
            #logging.info('Duplciate ACK packet. ACKed: %s:%s; Head: %s',
            #        ack_from, ack_to, self._inflight.peek())
            return

        # Check for out-of-order and calculate rtts
        for acked_seq_num in range(ack_from, ack_to + 1):

            if acked_seq_num == self._inflight.peek():
                (time_stamp, resent, _) = self._inflight.pop()
                if resent:
                    pass
                    #logging.info('Cleared resent from head: %s', acked_seq_num)
                else:
                    # Reset if clearing non-resends from head of line
                    self._cnt_ooo = 0
            else:
                (time_stamp, resent, _, is_ooo) = self._inflight.pop_given(acked_seq_num)
                if is_ooo:
                    self._cnt_ooo += 1
                    self.stats['OooPkt'] += 1

            self.stats['Ack'] += 1
            last_acked = acked_seq_num

            if not resent:
                rtts.append(rx_time - time_stamp)

        if self._cnt_ooo >= OOO_THRESH:
            resendable = self._inflight.get_resendable(last_acked)
            self._resend_indicated(resendable)
            self._ledbat.data_loss()
            self._cnt_ooo = 0

        # Extract list of delays
        for dalay in range(0, num_delays):
            delays.append(int(struct.unpack('>Q', ack_data[12+dalay*8:20+dalay*8])[0]))

        # Move to milliseconds from microseconds
        delays = [x / 1000 for x in delays]

        # Feed new data to LEDBAT
        self._ledbat.update_measurements(((ack_to - ack_from + 1) * SZ_DATA) + 24, delays, rtts)

    def dispose(self):
        """Cleanup this test"""

        # Log information
        logging.info('%s Disposing', self)

        # Cancel all event handles
        if self._hdl_init_ack is not None:
            self._hdl_init_ack.cancel()
            self._hdl_init_ack = None

        if self._hdl_act_to_data is not None:
            self._hdl_act_to_data.cancel()
            self._hdl_act_to_data = None

        if self._hdl_send_data is not None:
            self._hdl_send_data.cancel()
            self._hdl_send_data = None

        if self._hdl_idle is not None:
            self._hdl_idle.cancel()
            self._hdl_idle = None

        if self._hdl_log is not None:
            self._hdl_log.cancel()
            self._hdl_log = None

        if self.stop_hdl is not None:
            self.stop_hdl.cancel()
            self.stop_hdl = None

        # Remove from the owner
        self._owner.remove_test(self)

    def __str__(self, **kwargs):
        return 'TEST: LC:{} RC: {} ({}:{}):'.format(
            self.local_channel, self.remote_channel,
            self._remote_ip, self._remote_port)
