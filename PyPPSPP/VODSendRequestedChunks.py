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
import time
import asyncio
import struct

from Messages import *
from AbstractSendRequestedChunks import AbstractSendRequestedChunks

class VODSendRequestedChunks(AbstractSendRequestedChunks):
    """VOD1 Sending of requested chunks"""

    def __init__(self, swarm, member):
        self._counter = 0
        return super().__init__(swarm, member)

    def SendAndSchedule(self):
        # Choose what to send
        set_to_send = (self._swarm.set_have & self._member.set_requested) - self._member.set_sent

        if any(set_to_send):
            # We have stuff to send - all is fine
            chunk_to_send = min(set_to_send)
       
            data = self._swarm.GetChunkData(chunk_to_send)

            # We might have discarded this chunk:
            if data is None:
                self._member.set_requested.discard(chunk_to_send)
            else:
                md = MsgData.MsgData(self._member.chunk_size, self._member.chunk_addressing_method)
                md.start_chunk = chunk_to_send
                md.end_chunk = chunk_to_send
                md.data = data
                md.timestamp = int((time.time() * 1000000))

                mdata_bin = bytearray()
                mdata_bin[0:4] = struct.pack('>I', self._member.remote_channel)
                mdata_bin[4:] = md.BuildBinaryMessage()

                self._member.SendAndAccount(mdata_bin)
                self._member.set_sent.add(chunk_to_send)

                self._counter += 1
                if self._counter % 100 == 0:
                    logging.info("(VODSend) Member: {} Can/All: {}/{}. Last: {}"
                                .format(
                                    self._member,
                                    len(set_to_send),
                                    len(self._swarm.set_have), 
                                    chunk_to_send
                                )
                    )

            # If we have stuff to send - do not throttle
            self._member._sending_handle = asyncio.get_event_loop().call_soon(
                self._member.SendRequestedChunks)

        else:
            # We have nothing to send - throttle sending
            self._member._sending_handle = asyncio.get_event_loop().call_later(
                0.01, self._member.SendRequestedChunks)
