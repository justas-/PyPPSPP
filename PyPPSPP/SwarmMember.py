import logging
import random
import sys
import struct

from collections import deque

from Messages.MsgHandshake import MsgHandshake
from Messages.MsgRequest import MsgRequest
from Messages.MsgHave import MsgHave
from Messages.MsgData import MsgData
from Messages.MessageTypes import MsgTypes as MT

class SwarmMember(object):
    """A class used to represent member in the swarm"""

    def __init__(self, swarm, ip_address, udp_port = 6778):
        """Init object representing the remote peer"""
        self._swarm = swarm
        self.ip_address = ip_address
        self.udp_port = udp_port

        # Channels for multiplexing
        self.local_channel = 0
        self.remote_channel = 0

        # Chunk parameters and hash parameters
        self.chunk_size = 4 # 4 Bytes / 32 bits
        self.hash_type = None

        # Did we choke or are we choked
        self.local_choked = False
        self.remote_choked = False

        # Is session fully init & if handshake is sent
        self.is_init = False
        self.is_hs_sent = False

        # Amount of bytes sent and received from this peer in this session
        self._total_data_tx = 0
        self._total_data_rx = 0

        # Make have map
        self.set_have = set()

    def SendHandshake(self):
        """Send initial packet to the potential remote peer"""

        # Create a handshake message
        hs = MsgHandshake()
        hs.swarm = self._swarm.swarm_id
        bm = hs.BuildBinaryMessage()

        # Assign a local channel ID
        self.remote_channel = 0
        self.local_channel = random.randint(1, 65535)

        # Create a full HANDSHAKE message
        hs = bytearray()
        hs[0:4] = struct.pack('>I', self.remote_channel)
        hs[4:] = bytes([MT.HANDSHAKE])
        hs[5:7] = struct.pack('>I', self.local_channel)
        hs[9:] = bm

        logging.info("Sending handshake to {0}:{1}. RC={2};LC={3}"
                     .format(self.ip_address, self.udp_port, self.remote_channel, self.local_channel))
        self.SendAndAccount(hs)
        self.is_hs_sent = True

    def SendAndAccount(self, binary_data):
        self._swarm.SendData(self.ip_address, self.udp_port, binary_data)
        self._total_data_tx = self._total_data_tx + len(binary_data)
        
    def GotKeepalive(self):
        """Sometimes remote peer might send us keepalive only"""
        pass

    def HandleMessages(self, messages):
        """ Handle messages received from the remote peer"""

        logging.info("Handling messages:")
        
        for msg in messages:
            if isinstance(msg, MsgHandshake):
                self.HandleHandshake(msg)
                continue
            if isinstance(msg, MsgHave):
                self.HandleHave(msg)
                continue
            if isinstance(msg, MsgData):
                self.HandleData(msg)
                continue

    def HandleHandshake(self, handshake):
        """Handle the handshake received from remote peer"""
        
        logging.info("Handled handshake")

        if self.is_init == True:
            # Do not allow redefinition of parameters
            return

        if self.is_hs_sent == True:
            # This is reply to our HS
            # TODO: for now just care about channel numbers
            self.remote_channel = handshake.their_channel
            self.hash_type = handshake.merkle_tree_hash_func
            self.is_init = True

        elif self.is_hs_sent == False:
            # This is remote peer initiating connection to us
            # TODO: Make this work by replying with HS
            return

    def HandleHave(self, have):
        """Update the local have map"""

        logging.info("Handling Have")

        for i in range(have.start_chunk, have.end_chunk+1):
            self.set_have.add(i)

        # Inform swarm to consider rerunning chunk selection alg
        self._swarm.MemberHaveMapUpdated()

    def HandleData(self, data):
        """ """

    def RequestChunks(self, chunks_set):
        """Request chunks from this member"""
        first_chunk = min(chunks_set)
        
        # Get first range of continuous chunks
        x = first_chunk + 1
        while x in chunks_set:
            x = x + 1
        last_chunk = x - 1

        # Build a set
        request = set()
        for x in range(first_chunk, last_chunk+1):
            request.add(x)

        # Build a message
        req = MsgRequest()
        req.start_chunk = first_chunk
        req.end_chunk = last_chunk

        data = bytearray()
        data[0:4] = struct.pack('>I', self.remote_channel)
        data[4:] = bytes([MT.REQUEST])
        data[5:] = req.BuildBinaryMessage()

        logging.info("Sending: {0}".format(req))

        self.SendAndAccount(data)
        self._swarm.set_requested.union(request)

    def Disconnect(self):
        """Close association with the remote member"""
        pass