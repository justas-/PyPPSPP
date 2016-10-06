import logging
import asyncio
import pathlib
import sys
import os
import time
import collections
import struct

if __name__ == "__main__" and __package__ is None:
    top = pathlib.Path(__file__).resolve().parents[1]
    sys.path.append(str(top))
    from LEDBAT import LEDBAT

class PeerProtocol(asyncio.DatagramProtocol):
    def __init__(self, **kwargs):
        self._peer_addr = ("127.0.0.1", 6778)
        self._transport = None
        self._send_handle = None
        self._ledbat = LEDBAT()
        self._loop = asyncio.get_event_loop()
        self._next_id = 1

        self._in_flight = set()
        self._ret_control = collections.deque(5*[None], 5)

        self._start_time = None
        self._sent_data = 0

    def connection_made(self, transport):
        logging.info("Connection made callback")
        self._transport = transport
        self.start_sending()

    def datagram_received(self, data, addr):
        type, seq, ts = struct.unpack('>cIQ', data)
        assert type == 2
        self._in_flight.discard(seq)
        self._ledbat.FeedAck([ts], 1)

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
        self._send_handle = self._loop.call_soon(self.__send_next)

    def stop_sending(self):
        """Stop sending data"""
        assert self._send_handle is not None
        self._send_handle.cancel()
        self._send_handle = None

    def __build_msg(self, seq):
        """Build a fake message with the given seq number"""
        msg_bin = bytearray()
        msg_bin.extend(struct.pack_into('>cIQ',
                                        bytes([1]),                 # MSG TYPE
                                        seq,                        # SEQ
                                        ((time.time() * 1000000)))) # TIMESTAMP
        msg_bin.extend(1024 * bytes([127]))                         # DATA

        return msg_bin

    def __send_next(self):
        """Perform generating and sending of data"""
        msg_bin = bytearray()
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
                    self._ledbat.DataLoss()
                else:
                    # Send as normal
                    msg_bin = self.__build_msg(self._next_id)
                    self._ret_control.appendleft(self._next_id)
                    self._in_flight.add(self._next_id)
                    self._next_id += 1

        msg_sz = len(msg_bin)
        delay = self._ledbat.GetDelay(msg_sz)

        self._transport.sendto(msg_bin, self._peer_addr)
        self._sent_data += msg_sz

        if delay == 0:
            self._send_handle = self._loop.call_soon(self.__send_next)
        else:
            self._send_handle = self._loop.call_later(delay, self.__send_next)

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.info("LEDBAT TEST SOURCE starting")

    loop = asyncio.get_event_loop()

    listen = loop.create_datagram_endpoint(PeerProtocol, local_addr=("0.0.0.0", 6778))
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
    main()