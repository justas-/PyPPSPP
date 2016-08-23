import datetime
from struct import pack, pack_into, unpack

from Messages.MessageTypes import MsgTypes

class MsgData(object):
    """A class representing PPSPP handshake message"""
    # TODO: We need to know how big are start and end fields

    def __init__(self, chunk_size, chunk_addr_method):
        self.start_chunk = 0
        self.end_chunk = 0
        self.timestamp = 0
        self.data = None

        # Peer dependant params
        self._chunk_size = chunk_size
        self._chunk_addr_method = chunk_addr_method

    def BuildBinaryMessage(self):
        """Build bytearray of the message"""
        wb = bytearray()
        wb[0:] = pack('>cIIQ', 
                      bytes([MsgTypes.DATA]), 
                      self.start_chunk, 
                      self.end_chunk, 
                      self.one_way_delay_sample)
        wb[len(wb):] = data

        return wb

    def ParseReceivedData(self, data):
        """Parse binary data to an Object"""

        if self._chunk_addr_method == 2:
            details = unpack('>IIQ', data[0:16])
            self.start_chunk = details[0]
            self.end_chunk = details[1]
            self.timestamp = details[2]

            data_len = (self.end_chunk - self.start_chunk + 1) * self._chunk_size
            self.data = data[16:data_len+16]

            return 16 + len(self.data)
        else:
            raise NotImplementedError()

    def __str__(self):
        return str("[DATA] Start: {0}; End: {1}; TS: {2}; Data Len: {3}"
                   .format(
                       self.start_chunk,
                       self.end_chunk,
                       self.timestamp,
                       len(self.data)))

    def __repr__(self):
        return self.__str__()