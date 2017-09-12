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

import logging
import asyncio
import pathlib
import sys
import os
import time
import collections
import struct
import argparse

if __name__ == "__main__" and __package__ is None:
    top = pathlib.Path(__file__).resolve().parents[1]
    sys.path.append(str(top))
    from LEDBAT import LEDBAT

class PeerProtocol(asyncio.DatagramProtocol):
    LOGINT = 1

    def __init__(self, args):
        self._peer_addr = (args.target_ip, 6778)
        self._transport = None
        self._send_handle = None
        self._stat_handle = None
        self._ledbat = LEDBAT()
        self._loop = asyncio.get_event_loop()
        self._next_id = 1

        self._in_flight = set()
        self._ret_control = collections.deque(5*[None], 5)

        self._start_time = None
        self._int_time = None
        self._sent_data = 0
        self._int_data = 0
        self._num_retrans = 0
        self._int_retrans = 0
        self._delays = collections.deque(10*[None], 10)

    def connection_made(self, transport):
        logging.info("Connection made callback")
        self._transport = transport
        self.start_sending()

    def datagram_received(self, data, addr):
        type = data[0]
        seq, ts = struct.unpack('>IQ', data[1:13])
        assert type == 2
        self._in_flight.discard(seq)
        self._ledbat.feed_ack([ts], 1)

    def error_received(self, exc):
        logging.warning("Error received: {0}".format(exc))

    def connection_lost(self, exc):
        logging.critical("Socket closed: {0}".format(exc))

    def pause_writing(self):
        logging.warn("UDP SOCKET OVER HIGH-WATER MARK")

    def resume_writing(self):
        logging.warn("UDP SOCKET DRAINED")

    def start_sending(self):
        """Start generating and sending data using LEDBAT
        to limit the frequency of sending
        """
        assert self._send_handle is None
        self._start_time = time.time()
        self._int_time = time.time()
        self._send_handle = self._loop.call_soon(self.__send_next)
        self._stat_handle = self._loop.call_later(PeerProtocol.LOGINT, self.__print_stats)

    def stop_sending(self):
        """Stop sending data"""
        assert self._send_handle is not None
        self._send_handle.cancel()
        self._send_handle = None
        self._stat_handle.cancel()
        self._stat_handle = None

    def __print_stats(self):
        t_now = time.time()
        speed_avg = self._sent_data / (t_now - self._start_time)
        speed_int = (self._sent_data - self._int_data) / (t_now - self._int_time)
        retr_int = self._num_retrans - self._int_retrans

        self._int_data = self._sent_data
        self._int_time = t_now
        self._int_retrans = self._num_retrans

        avg_delay = 0
        num_delay = 0
        for i in self._delays:
            if i is not None:
                avg_delay += i
                num_delay += 1
        avg_delay = avg_delay / num_delay

        logging.info("Num in flight: {}; Speed (avg/int): {}/{}; qd: {}; cwnd: {}; retr: {} avg_del: {}; next_seq: {};"
                     .format(
                         len(self._in_flight),
                         int(speed_avg),
                         int(speed_int),
                         self._ledbat._qd,
                         self._ledbat._cwnd,
                         retr_int,
                         avg_delay,
                         self._next_id
                         ))
        self._stat_handle = self._loop.call_later(PeerProtocol.LOGINT, self.__print_stats)

    def __build_msg(self, seq):
        """Build a fake message with the given seq number"""
        msg_bin = bytearray()
        msg_bin.extend(struct.pack('>cIQ',
                                    bytes([1]),                    # MSG TYPE
                                    seq,                           # SEQ
                                    int((time.time() * 1000000)))) # TIMESTAMP
        msg_bin.extend(1024 * bytes([127]))                        # DATA

        return msg_bin

    def __send_next(self):
        """Perform generating and sending of data"""
        msg_bin = bytearray()

        min_in_flight = None
        if len(self._in_flight) > 0:
            min_in_flight = min(self._in_flight)

        if min_in_flight is None:
            # All is acknowledged
            msg_bin = self.__build_msg(self._next_id)
            self._ret_control.appendleft(self._next_id)
            self._in_flight.add(self._next_id)
            self._next_id += 1
        else:
            deq_front = self._ret_control[4]
            if deq_front is None:
                # Send as normal, don't have enough in fligh observations
                msg_bin = self.__build_msg(self._next_id)
                self._ret_control.appendleft(self._next_id)
                self._in_flight.add(self._next_id)
                self._next_id += 1
            else:
                # Check if we need to retransmit
                if min_in_flight <= deq_front:
                    # Retransmit
                    msg_bin = self.__build_msg(min_in_flight)
                    self._num_retrans += 1
                    self._ledbat.data_loss()
                else:
                    # Send as normal
                    msg_bin = self.__build_msg(self._next_id)
                    self._ret_control.appendleft(self._next_id)
                    self._in_flight.add(self._next_id)
                    self._next_id += 1

        msg_sz = len(msg_bin)
        delay = self._ledbat.get_delay(msg_sz)
        self._delays.appendleft(delay)

        self._transport.sendto(msg_bin, self._peer_addr)
        self._sent_data += msg_sz

        if delay == 0:
            self._send_handle = self._loop.call_soon(self.__send_next)
        else:
            self._send_handle = self._loop.call_later(delay, self.__send_next)

def main(args):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.info("LEDBAT TEST SOURCE starting. Target: {}".format(args.target_ip))

    loop = asyncio.get_event_loop()

    listen = loop.create_datagram_endpoint(lambda: PeerProtocol(args), local_addr=("0.0.0.0", 6778))
    transport, protocol = loop.run_until_complete(listen)
    
    if os.name == 'nt':
        def wakeup():
            # Call again later
            loop.call_later(0.5, wakeup)
        loop.call_later(0.5, wakeup)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    # Parse cmd line parameters and/or use defaults
    parser = argparse.ArgumentParser(description="Send UDP traffic using LEDBAT")
    parser.add_argument("target_ip", help='Traffic sink address', nargs='?', default='10.51.32.132')
    args = parser.parse_args()

    # Start the program
    main(args)