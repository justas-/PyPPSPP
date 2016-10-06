import logging
import asyncio
import sys
import os
import time
import collections
import struct

class PeerProtocol(asyncio.DatagramProtocol):
    def __init__(self, **kwargs):
        self._transport = None
        self._loop = asyncio.get_event_loop()
        
        self._start_time = None
        self._received_data = 0
        self._sent_data = 0

        self._dlys = collections.deque(5 * [None], 5)
        self._num_rx = 0

    def connection_made(self, transport):
        logging.info("Connection made callback")
        self._transport = transport

    def datagram_received(self, data, addr):
        """Reply to the DATA message with the ACK message"""
        ts_in = time.time()
        type, seq, ts = struct.unpack('>cIQ', data[0:13])
        assert type == 1

        msg_len = len(data)
        self._received_data += msg_len

        dly = int((time.time() * 1000000) - ts_in)
        self._dlys.appendleft(dly)

        msg_ack = self.__build_ack_msg(seq, ts_in)
        self._sent_data += len(msg_ack)

        self._transport.sendto(msg_ack, addr)

        self._num_rx += 1
        if self._num_rx % 50 == 0:
            sum = 0
            count = 0
            for n in self._dlys:
                if n is not None:
                    sum += n
                    count += 1
            avg_dly = sum / count
            logging.info("Average delay: {}".format(avg_dly))

    def error_received(self, exc):
        logging.warning("Error received: {0}".format(exc))

    def connection_lost(self, exc):
        logging.critical("Socket closed: {0}".format(exc))

    def pause_writing(self):
        logging.warn("UDP SOCKET OVER HIGH-WATER MARK")

    def resume_writing(self):
        logging.warn("UDP SOCKET DRAINED")

    def __build_ack_msg(self, seq, dly):
        """Build a fake message with the given seq number"""
        msg_bin = bytearray()
        msg_bin.extend(struct.pack_into('>cIQ', bytes([2]), seq, dly))
        return msg_bin

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.info("LEDBAT TEST SINK starting")

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