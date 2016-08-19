import logging
import random
import sys
import struct
import hashlib

from collections import deque

from Messages.MsgHandshake import MsgHandshake
from Messages.MsgRequest import MsgRequest
from Messages.MsgHave import MsgHave
from Messages.MsgData import MsgData
from Messages.MsgIntegrity import MsgIntegrity
from Messages.MsgAck import MsgAck
from Messages.MessageTypes import MsgTypes as MT

from MessagesParser import MessagesParser

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

        # Outbox to stuff all reply messages into one datagram
        self._outbox = deque()

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
        return

    def ParseData(self, data):
        """Handle data received from the peer"""

        # Parse all messages into queue
        messages = MessagesParser.ParseData(self, data)
        
        # Handle all messages in the same way as they arrived
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
            if isinstance(msg, MsgIntegrity):
                self.HandleIntegrity(msg)
                continue
            if isinstance(msg, MsgAck):
                self.HandleAck(msg)
                continue

        # Account all received data
        self._total_data_rx = self._total_data_rx + len(data)

        # We might have generated replies while processing
        self.ProcessOutbox()

#region Messages Handling
    def HandleHandshake(self, handshake):
        """Handle the handshake received from remote peer"""
        
        logging.info("Handled handshake")

        if self.is_init == True:
            # Do not allow redefinition of parameters
            return

        if self.is_hs_sent == True:
            # This is reply to our HS
            # TODO: for now just care about channel numbers
            # TODO: Verify that our Python knows how to validate given hash type
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

    def HandleData(self, msg_data):
        """Handle the received data"""
        # Do we have integrity for given range?
        # We should keep integritys in scope of the peer as hash funtion can be different between the peers
        integrity = None
        if (msg_data.start_chunk, msg_data.end_chunk) in self._swarm.integrity:
            integrity = self._swarm.integrity[(msg_data.start_chunk, msg_data.end_chunk)]

        # Calculate the integirty of received message
        calc_integirty = self.GetIntegrity(msg_data.data)
        logging.info("Calculated integrity. From: {0} To: {1} Value: {2}"
                     .format(msg_data.start_chunk, msg_data.end_chunk, calc_integirty))

        # Compare agains value we have
        if integrity != None and integrity == calc_integirty:
            # Save data to file
            self._swarm.SaveVerifiedData(msg_data.start_chunk, msg_data.end_chunk, msg_data.data)

            # Send ack to peer
            msg_ack = MsgAck()
            msg_ack.start_chunk = msg_data.start_chunk
            msg_ack.end_chunk = msg_data.end_chunk
            msg_ack.one_way_delay_sample = 1000
            self._outbox.append(msg_ack)

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

    def HandleIntegrity(self, msg_integrity):
        """Handle the incomming integorty message"""
        self._swarm.integrity[(msg_integrity.start_chunk, msg_integrity.end_chunk)] = msg_integrity.hash_data

    def HandleAck(self, msg_ack):
        """Handle incomming ck message"""
        return
#endregion

    def ProcessOutbox(self):
        """Binarify and send all messages in the outbox"""
        # This method should do any re-arrangement if required

        data = bytearray()
        data[0:] = struct.pack('>I', self.remote_channel)

        for msg in self._outbox:
            msg_bin = msg.BuildBinaryMessage()
            data[len(data):] = msg_bin
            
        self.SendAndAccount(data)

    def Disconnect(self):
        """Close association with the remote member"""

        # Build goodbye handshake
        hs = MsgHandshake()
        hs_bin = hs.BuildGoodbye()

        # Serialize
        data = bytearray()
        data[0:] = struct.pack('>I', self.remote_channel)
        data[4:] = hs_bin
        
        self.SendAndAccount(data)

    def GetIntegrity(self, data):
        """Calculate the integirty value of given data using remote peers hash"""
        if self.hash_type == None:
            return None
        elif self.hash_type == 0:
            # SHA-1
            return hashlib.sha1(data).digest()
        elif self.hash_type == 1:
            # SHA-224
            return hashlib.sha224(data).digest()
        elif self.hash_type == 2:
            # SHA-256
            return hashlib.sha256(data).digest()
        elif self.hash_type == 3:
            # SHA-384
            return hashlib.sha384(data).digest()
        elif self.hash_type == 4:
            # SHA-512
            return hashlib.sha512(data).digest()
        else:
            return None