import datetime

from struct import pack, pack_into, unpack

class MsgData(object):
    """A class representing PPSPP handshake message"""
    # TODO: We need to know how big are start and end fields

    def __init__(self):
        self.start_chunk = 0
        self.end_chunk = 0
        self.timestamp = 0
        self.data = None

    def BuildBinaryMessage(self):
        """Build message"""
        # TODO
        pass

    def ParseReceivedData(self, data):
        """Parse binary data to an Object"""

        details = unpack('>IIQ', data[0:16])
        self.start_chunk = details[0]
        self.end_chunk = details[1]
        self.timestamp = details[2]

        self.data = data[16:]

        return 16 + len(self.data)

    def __str__(self):
        return str("[DATA] Start: {0}; End: {1}; TS: {2}; Data: {3}"
                   .format(
                       self.start_chunk,
                       self.end_chunk,
                       #datetime.datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                       self.timestamp,
                       len(self.data)))

    def __repr__(self):
        return self.__str__()