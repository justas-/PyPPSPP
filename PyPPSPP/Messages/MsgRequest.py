from struct import pack_into, unpack

class MsgRequest(object):
    """A class representing REQUEST message"""
    # TODO: support from 64 bit messages

    def __init__(self):
        self.start_chunk = 0
        self.end_chunk = 0

    def BuildBinaryMessage(self):
        """Build binary version of REQUEST message"""
        wb = bytearray(8)
        pack_into('>II', wb, 0, 
                  self.start_chunk, 
                  self.end_chunk)

        return wb

    def ParseReceivedData(self, data):
        """Parse received data back to the message"""
        # TODO: This method should be adapted to chunk addressing method
        contents = unpack('>II', data[0:8])
        self.start_chunk = contents[0]
        self.end_chunk = contents[1]
        
    def __str__(self):
        return str("[REQUEST] Start: {0}; End: {1}".format(self.start_chunk, self.end_chunk))

    def __repr__(self):
        return self.__str__()