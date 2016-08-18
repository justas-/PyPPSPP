from struct import pack_into, unpack

class MsgAck(object):
    """A class representing ACK message"""

    def __init__(self):
        self.start_chunk = 0
        self.end_chunk = 0
        self.one_way_delay_sample = 0

    def BuildBinaryMessage(self):
        """Build bytearray of the message"""
        wb = bytearray(128)
        pack_into('>IIQ', wb, 0, 
                  self.start_chunk, 
                  self.end_chunk, 
                  self.one_way_delay_sample)

        return wb

    def ParseReceivedData(self, data):
        """Parse given bytearray to usable data"""
        contents = unpack('>IIQ', data)
        self.start_chunk = contents[0]
        self.end_chunk = contents[1]
        self.one_way_delay_sample = contents[2]
