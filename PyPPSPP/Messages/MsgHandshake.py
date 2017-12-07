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

from struct import pack, pack_into, unpack
from array import array
import uuid
import logging

from GlobalParams import GlobalParams as GB
from Messages.MessageTypes import MsgTypes

class MsgHandshake(object):
    """A class representing PPSPP handshake message"""

    def __init__(self):
        self.version = GB.version
        self.min_version = GB.min_version
        self.content_identity_protection = GB.content_protection_scheme
        self.swarm = 0
        self.merkle_tree_hash_func = 2
        self.live_signature_alg = 0
        self.chunk_addressing_method = GB.chunk_addressing_method
        self.live_discard_window = 0
        self.supported_messages_len = GB.supported_messages_len
        self.supported_messages = GB.supported_messages
        self.chunk_size = GB.chunk_size
        self.uuid = None

        # Used during handshake
        self.our_channel = 0
        self.their_channel = 0

        self._is_goodbye = False

    def BuildGoodbye(self):
        """Build HANDSHAKE indicating that we are leaving"""
        
        wb = bytearray()
        wb[0:] = pack('>cI', bytes([MsgTypes.HANDSHAKE]), 0)
        wb[len(wb):] = pack('cc', bytes([0]), bytes([self.version]))
        wb[len(wb):] = pack('cc', bytes([1]), bytes([self.min_version]))
        wb[len(wb):] = pack('c', bytes([255]))

        self._is_goodbye = True
        return wb

    def BuildBinaryMessage(self):
        """Build HANDSHAKE message. Ref [RFC7574] §7"""
        
        wb = bytearray(512)
        offset = 0

        # Packing is done in sorted way

        # [0] Version
        pack_into('cc', wb, offset, bytes([0]), bytes([self.version]))
        offset = offset + 2

		# [1] Minimum version
        pack_into('cc', wb, offset, bytes([1]), bytes([self.min_version]))
        offset = offset + 2

        # [2] Swarm identifier
        swid_len = len(self.swarm)
        pack_into('>cH', wb, offset, bytes([2]), swid_len)
        offset = offset + 1 + 2
        
        wb[offset:offset+swid_len] = self.swarm
        offset = offset + swid_len

        # [3] Content Integrity Protection Method
        pack_into('cc', wb, offset, bytes([3]), bytes([self.content_identity_protection]))
        offset = offset + 2

        # [4] Merkle Tree Hash Function
        if self.content_identity_protection == 1:
            pack_into('cc', wb, offset, bytes([4]), bytes([self.merkle_tree_hash_func]))
            offset = offset + 2

        # [5] Live signature Algorith
        if (self.content_identity_protection == 2 or self.content_identity_protection == 3):
            pack_into('cc', wb, offset, bytes([5]), bytes([self.live_signature_alg]))
            offset = offset + 2

        # [6] Chunk addressing method
        pack_into('cc', wb, offset, bytes([6]), bytes([self.chunk_addressing_method]))
        offset = offset + 2

        # [7] Live discard window
        # Not according to specs!!!
        if self.live_discard_window != 0:
            pack_into('>ci', wb, offset, bytes([7]), self.live_discard_window)
            offset = offset + 5

        # [8] Supported messages
        # Not according to specs!!!
        pack_into('ccs', wb, offset, bytes([8]), bytes([self.supported_messages_len]), bytes([255, 255]))
        offset = offset + 4

        # [9] Chunk size
        pack_into('>ci', wb, offset, bytes([9]), self.chunk_size)
        offset = offset + 5

        # [10] Peer UUID
        pack_into('>c', wb, offset, bytes([10]))
        offset = offset + 1

        wb[offset:offset+16] = self.uuid.bytes
        offset = offset + 16

        # [255] End option
        pack_into('c', wb, offset, bytes([255]))
        offset = offset + 1

        return wb[0:offset]

    def ParseReceivedData(self, data):
        """Parse received data until all HANDSHAKE message is parsed"""
        
        idx = 0
        finish_parsing = False

        while finish_parsing == False:
            next_tag = data[idx]

            if next_tag == 0:
                idx = idx + 1
                self.version = data[idx]
                logging.debug("Parsed version: {0}".format(self.version))
                idx = idx + 1
            elif next_tag == 1:
                idx = idx + 1
                self.min_version = data[idx]
                logging.debug("Parsed min version: {0}".format(self.min_version))
                idx = idx + 1
            elif next_tag == 2:
                idx = idx + 1
                swarm_len = unpack('>H', data[idx:idx+2])[0]
                idx = idx + 2
                self.swarm = data[idx:idx+swarm_len]
                logging.debug("Parsed swid_len: {0} Swarm id: {1}".format(swarm_len, self.swarm))
                idx = idx + swarm_len
            elif next_tag == 3:
                idx = idx + 1
                self.content_identity_protection = data[idx]
                logging.debug("Parsed cip: {0}".format(self.content_identity_protection))
                idx = idx + 1
            elif next_tag == 4:
                idx = idx + 1
                self.merkle_tree_hash_func = data[idx]
                logging.debug("Parsed mhf: {0}".format(self.merkle_tree_hash_func))
                idx = idx + 1
            elif next_tag == 5:
                idx = idx + 1
                self.live_signature_alg = data[idx]
                logging.debug("Parsed lsa: {0}".format(self.live_signature_alg))
                idx = idx + 1
            elif next_tag == 6:
                idx = idx + 1
                self.chunk_addressing_method = data[idx]
                logging.debug("Parsed cam: {0}".format(self.chunk_addressing_method))
                idx = idx + 1
            elif next_tag == 7:
                # TODO implement
                idx = idx + 1
                self.live_discard_window = unpack('>I', data[idx:idx+4])[0]
                logging.debug('Parsed ld: {}'.format(self.live_discard_window))
                idx = idx + 4
            elif next_tag == 8:
                idx = idx + 1
                self.supported_messages_len = data[idx]
                idx = idx + 1
                self.supported_messages = data[idx:idx+self.supported_messages_len]
                logging.debug("Parsed sm: {0} {1}".format(self.supported_messages_len, self.supported_messages))
                idx = idx + self.supported_messages_len
            elif next_tag == 9:
                idx = idx + 1
                self.chunk_size = unpack('>I', data[idx:idx+4])[0]
                logging.debug("Parsed cz: {0}".format(self.chunk_size))
                idx = idx + 4
            elif next_tag == 10:
                idx = idx + 1
                self.uuid = uuid.UUID(bytes=bytes(data[idx:idx+16]))
                logging.debug('Parsed peer UUID: %s', self.uuid)
                idx = idx + 16
            elif next_tag == 255:
                logging.debug("Parsed: EOM")
                return idx + 1
            
    def __str__(self):
        if self._is_goodbye == True:
            return str("[HANDSHAKE] Goodbye!")
        else:
            return str("[HANDSHAKE] LocCh: {0}; RemCh: {1}".format(self.our_channel, self.their_channel))

    def __repr__(self):
        return self.__str__()
