import logging
import time
import struct
import asyncio

from Messages import *
from AbstractSendRequestedChunks import AbstractSendRequestedChunks
from LEDBAT import LEDBAT

class OfflineSendRequestedChunks(AbstractSendRequestedChunks):
    """Chunks sending algorithm for offline data sharing"""

    def __init__(self, swarm, member):
        self._outstanding_backoff = False
        return super().__init__(swarm, member)

    def SendAndSchedule(self):
        set_to_send = (self._swarm.set_have & self._member.set_requested) - self._member.set_sent
        outstanding_len = len(set_to_send)

        if outstanding_len > 0:
            # We have stuff to send - all is fine
            chunk_to_send = min(set_to_send)
       
            data = self._swarm.GetChunkData(chunk_to_send)
        
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

            logging.info("Can serve: {0}/{1} chunks. Sent {2} chunk"
                         .format(len(set_to_send), len(self._swarm.set_have), chunk_to_send))

            delay = self._member._ledbat.get_delay(len(mdata_bin))
            if delay == 0:
                self._member._sending_handle = asyncio.get_event_loop().call_soon(self._member.SendRequestedChunks)
            else:
                self._member._sending_handle = asyncio.get_event_loop().call_later(delay, self._member.SendRequestedChunks)
        else:
            # We have sent everything, now check if we need to resend
            if len(self._swarm.set_have - self._member.set_requested) != 0:
                # There are pieces in need of resending
                if self._outstanding_backoff == False:
                    # Give 1 sec to receive ACKs in flight
                    logging.info("In the end-of-sending backoff")
                    self._outstanding_backoff = True
                    self._member._sending_handle = asyncio.get_event_loop().call_later(
                        1, self.SendRequestedChunks)
                else:
                    # Remove un-ACKed pieces from sent set and keep sending
                    self._member.set_sent = self._member.set_sent - self._member.set_requested
                    self._member._sending_handle = asyncio.get_event_loop().call_soon(
                        self._member.SendRequestedChunks)
            else:
                # All I have == all peer has
                # Not even gonna reschedule the sender :)
                self._member._sending_handle = None


