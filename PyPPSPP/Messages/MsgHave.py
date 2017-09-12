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

from Messages.MessageTypes import MsgTypes
from struct import pack_into, unpack

class MsgHave(object):
    """A class representing HAVE message"""

    def __init__(self):
        self.start_chunk = 0
        self.end_chunk = 0

    def BuildBinaryMessage(self):
        """Build binary version of HAVE message"""
        wb = bytearray(9)
        pack_into('>cII', wb, 0,
                  bytes([MsgTypes.HAVE]),
                  self.start_chunk, 
                  self.end_chunk)

        return wb

    def ParseReceivedData(self, data):
        """Parse received data back to the message"""
        contents = unpack('>II', data)
        self.start_chunk = contents[0]
        self.end_chunk = contents[1]

    def __str__(self):
        return str("[HAVE] Start: {0}; End: {1}".format(self.start_chunk, self.end_chunk))

    def __repr__(self):
        return self.__str__()
        