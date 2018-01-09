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
import asyncio
import collections
import struct
import logging

from Messages import *
from AbstractSendRequestedChunks import AbstractSendRequestedChunks

import pyledbat
import pyledbat.ledbat
import pyledbat.testledbat

class LEDBATSendRequestedChunks(AbstractSendRequestedChunks):
    """Sending of requested chunks using LEDBAT"""

    def __init__(self, swarm, member):
        """Init sender"""
        self._num_outstanding = 0       # Number of chunks outstanding
        self._num_checks = 0            # Times the above number did not change

        return super().__init__(swarm, member)

    def _build_and_send(self, chunk_id):
        """Build DATA message with indicated chunk"""
        data = self._swarm.GetChunkData(chunk_id)
        
        md = MsgData.MsgData(self._member.chunk_size, self._member.chunk_addressing_method)
        md.start_chunk = chunk_id
        md.end_chunk = chunk_id
        md.data = data
        md.timestamp = int((time.time() * 1000000))

        mdata_bin = bytearray()
        mdata_bin[0:4] = struct.pack('>I', self._member.remote_channel)
        mdata_bin[4:] = md.BuildBinaryMessage()

        self._member.send_and_account_udp(mdata_bin)
        self._member.set_sent.add(chunk_id)

    def SendAndSchedule(self):
        """ """

        t_now = time.time()

        # Skip all this if nothing outstanding
        num_outstanding = len(self._member.set_requested)
        if num_outstanding > 0:

            # Are we stuck outstanding on the same number of chunks?
            if num_outstanding == self._num_outstanding:
                # Same number -> increase the counter
                self._num_checks += 1
            else:
                # Number of outstanding changed -> reset the counter
                self._num_outstanding = num_outstanding
                self._num_checks = 0

            # Do action after 10 checks stuck on the same number of outstanding chunks
            if self._num_checks > 10:
                # No ACKs received at all - first packets stuck - resend them
                if self._member._time_last_ack_rx is None:
                    logging.info("Resengin all in-flight packets (1)")
                    self._member.resend_all_inflight()
                    self._num_checks = 0
                # No acks for more than 10 estimated RTTs
                elif t_now - self._member._time_last_ack_rx > self._member.ledbat.rtt * 10:
                    logging.info("Resengin all in-flight packets (2)")
                    self._member.resend_all_inflight()
                    self._num_checks = 0

        # Chunks I have and member is interested
        set_to_send = (self._swarm.set_have & self._member.set_requested) - self._member.set_sent
        any_to_send = any(set_to_send)

        # Nothing to send
        if not any_to_send:
            self._member._sending_handle = asyncio.get_event_loop().call_later(
                0.1, self._member.SendRequestedChunks)
            return

        # We have data to send:
        # Can we send?
        (can_send, reason) = self._member.ledbat.try_sending(1024 + 24)
        if not can_send:
            # Delay 0.1 sec
            self._member._sending_handle = asyncio.get_event_loop().call_later(
                0.01, self._member.SendRequestedChunks)
            return

        # We can send - Send and rechedule immediate resending
        chunk_id = min(set_to_send)
        #print("Sending Chunk {}".format(chunk_id))
        self._build_and_send(chunk_id)
        self._member.in_flight.add(chunk_id, t_now, None)
        self._member._sending_handle = asyncio.get_event_loop().call_soon(
                self._member.SendRequestedChunks)

    def SendAndSchedule_old(self):
        """Send requested data using LEDBAT"""

        # Get lowest chunk in flight
        min_in_fligh = None
        if any(self._member.set_sent):
            min_in_fligh = min(self._member.set_sent)

        # Chunks I have and member is interested
        set_to_send = (self._swarm.set_have & self._member.set_requested) - self._member.set_sent
        any_to_send = any(set_to_send)

        if min_in_fligh is None:
            # All is acknowledged. Try to send next requested
            if any_to_send:
                # We have stuff to send
                next_id = min(set_to_send)
                self._build_and_send(next_id)
                self._ret_control.appendleft(next_id)
        else:
            # We have chunks in flight. Get earliest in-flight id
            deq_front = self._ret_control[LEDBATSendRequestedChunks.WINDOWLEN-1]

            if deq_front is None:
                # Send as normal, not enough in-flight chunks
                if any_to_send:
                    # We have stuff to send
                    next_id = min(set_to_send)
                    self._build_and_send(next_id)
                    self._ret_control.appendleft(next_id)
            else:
                # Check if we need to retransmit
                if min_in_fligh <= deq_front:
                    # Retransmit
                    self._build_and_send(min_in_fligh)
                    self._member._ledbat.data_loss()
                    #logging.info("Data loss. Min in flight: {}. Delay: {}"
                    #             .format(min_in_fligh, self._member._ledbat._cto / 1000000))
                else:
                    # Send as normal
                    if any_to_send:
                        # We have stuff to send
                        next_id = min(set_to_send)
                        self._build_and_send(next_id)
                        self._ret_control.appendleft(next_id)

        # Check if sending still needed?
        if len(self._member.set_sent) > 0 and len(self._member.set_requested) > 0:
            self._member._sending_handle = None

        # Get delay before next send
        #delay = max([self._member._ledbat.get_delay(self._member.chunk_size), 0.01])
        delay = self._member._ledbat.get_delay(self._member.chunk_size)
        #logging.info("Delay: {}".format(delay))
        if delay <= 0:
            self._member._sending_handle = asyncio.get_event_loop().call_soon(
                self._member.SendRequestedChunks)
        else:
            self._member._sending_handle = asyncio.get_event_loop().call_later(
                delay, self._member.SendRequestedChunks)
