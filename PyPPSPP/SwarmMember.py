import logging
import random
import sys
import struct
import hashlib
import binascii
import time
import asyncio

from collections import deque

from Messages import *
from Messages.MessageTypes import MsgTypes as MT
from MessagesParser import MessagesParser
from GlobalParams import GlobalParams
from OfflineSendRequestedChunks import OfflineSendRequestedChunks
from VODSendRequestedChunks import VODSendRequestedChunks

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
        # Will be overwritten by Handshake msg
        self.chunk_addressing_method = None
        self.chunk_size = None
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

        # Chunk-maps
        self.set_have = set()       # What peer has
        self.set_requested = set()  # What peer requested
        self.set_sent = set()       # What chunks are sent but not ACK. After ACK they are removed

        self.unverified_data = []   # Keep all unverified messages

        # Outbox to stuff all reply messages into one datagram
        self._outbox = deque()

        # Chunk sending
        self._chunk_sending_alg =  None
        if self._swarm.live:
            self._chunk_sending_alg = VODSendRequestedChunks(
                self._swarm, self)
        else:
            self._chunk_sending_alg = OfflineSendRequestedChunks(
                self._swarm, self)
        self._sending_handle = None

    def SendHandshake(self):
        """Send initial packet to the potential remote peer"""

        # Create a handshake message
        hs = MsgHandshake.MsgHandshake()
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

        # Add information about pieces we have
        for range_data in self._swarm._have_ranges:
            have = MsgHave.MsgHave()
            have.start_chunk = range_data[0]
            have.end_chunk = range_data[1]
            hs[len(hs):] = have.BuildBinaryMessage()

        logging.info("Sending handshake to {0}:{1}. RC={2};LC={3}"
                     .format(self.ip_address, self.udp_port, self.remote_channel, self.local_channel))
        self.SendAndAccount(hs)
        self.is_hs_sent = True

    def SendReplyHandshake(self):
        """Reply with a handshake when remote peer is connecting to us"""

        hs = MsgHandshake.MsgHandshake()
        hs.swarm = self._swarm.swarm_id
        bm = hs.BuildBinaryMessage()

        self.local_channel = random.randint(1, 65535)
        
        # Create a full HANDSHAKE message
        hs = bytearray()
        hs[0:4] = struct.pack('>I', self.remote_channel)
        hs[4:] = bytes([MT.HANDSHAKE])
        hs[5:7] = struct.pack('>I', self.local_channel)
        hs[9:] = bm

        # Add information about pieces we have
        for range_data in self._swarm._have_ranges:
            have = MsgHave.MsgHave()
            have.start_chunk = range_data[0]
            have.end_chunk = range_data[1]
            hs.extend(have.BuildBinaryMessage())
            logging.info("Pigybacking on HANDSHAKE: {0}".format(have))

        logging.info("Replying with HANDSHAKE to {0}:{1}. RC={2};LC={3}"
                     .format(self.ip_address, self.udp_port, self.remote_channel, self.local_channel))

        self.SendAndAccount(hs)

    def SendAndAccount(self, binary_data):
        logging.debug("!! Sending BIN data: {0}".format(binascii.hexlify(binary_data)))
        self._swarm.SendData(self.ip_address, self.udp_port, binary_data)
        self._total_data_tx = self._total_data_tx + len(binary_data)
        
    def GotKeepalive(self):
        """Sometimes remote peer might send us keepalive only"""
        logging.info("Got KEEPALIVE from {0}:{1}".format(self.ip_address, self.udp_port))
        return

    def ParseData(self, data):
        """Handle data received from the peer"""

        # Parse all messages into queue
        messages = MessagesParser.ParseData(self, data)
        
        # Handle all messages in the same way as they arrived
        for msg in messages:
            if isinstance(msg, MsgHandshake.MsgHandshake):
                self.HandleHandshake(msg)
                continue
            if isinstance(msg, MsgHave.MsgHave):
                self.HandleHave(msg)
                continue
            if isinstance(msg, MsgData.MsgData):
                self.HandleData(msg)
                continue
            if isinstance(msg, MsgIntegrity.MsgIntegrity):
                self.HandleIntegrity(msg)
                continue
            if isinstance(msg, MsgAck.MsgAck):
                self.HandleAck(msg)
                continue
            if isinstance(msg, MsgRequest.MsgRequest):
                self.HandleRequest(msg)
                continue

        # Account all received data
        self._total_data_rx = self._total_data_rx + len(data)

        # We might have generated replies while processing
        self.ProcessOutbox()

#region Messages Handling
    def HandleHandshake(self, msg_handshake):
        """Handle the handshake received from remote peer"""
        
        if self.is_init == True:
            if msg_handshake._is_goodbye == True:
                logging.info("Received goodbye HANDSHAKE")
                if self._sending_handle != None:
                    self._sending_handle.cancel()
                    self._sending_handle = None
                self._swarm.RemoveMember(self)
            else:
                logging.info("Received non-goodbye HANDSHAKE in initialized the channel")
            return
        else:
            # Remote member is not init!
            if msg_handshake._is_goodbye ==  True:
                # Got Goodbye from non-init member...
                logging.info("Received goodbye HANDSHAKE from non-init member")
                if self._sending_handle != None:
                    self._sending_handle.cancel()
                    self._sending_handle = None
                self._swarm.RemoveMember(self)

            elif self.is_hs_sent == True:
                # This is reply to our HS
                # TODO: Catch the exception here and let the peer object die...
                self.SetPeerParameters(msg_handshake)
                self.is_init = True
                logging.info("Received reply HANDSHAKE and initialized the channel")

            elif self.is_hs_sent == False:
                # This is remote peer initiating connection to us
                # TODO: Catch the exception here and let the peer object die...
                self.SetPeerParameters(msg_handshake)
                self.SendReplyHandshake()
                self.is_init = True
                logging.info("Received init HANDSHAKE. Replied and initialzed the channel")

    def HandleHave(self, msg_have):
        """Update the local have map"""
        #logging.info("Handling Have: {0}".format(msg_have))
        for i in range(msg_have.start_chunk, msg_have.end_chunk+1):
            self.set_have.add(i)

            # Special handling for live swarms
            if self._swarm.live:
                if i in self._swarm.set_have:
                    # Do nothing if I have the advertised chunk
                    pass
                else:
                    # There is a chunk somebody have and I don't -> add to missing set
                    self._swarm.set_missing.add(i)

    def HandleData(self, msg_data):
        """Handle the received data"""
        # Place integrity checking here once ready
        
        # Save data to file
        # TODO: Hack. now taking one chunk only
        self._swarm.SaveVerifiedData(msg_data.start_chunk, msg_data.data)

        # Send ack to peer
        msg_ack = MsgAck.MsgAck()
        (min_ch, max_ch) = self._swarm.GetAckRange(msg_data.start_chunk, msg_data.end_chunk)
        msg_ack.start_chunk = min_ch
        msg_ack.end_chunk = max_ch
        
        # As described in [RFC7574] 8.7 && [RFC6817]
        delay = int((time.time() * 1000000) - msg_data.timestamp)
        msg_ack.one_way_delay_sample = delay

        self._outbox.append(msg_ack)

    def RequestChunks(self, chunks_set):
        """Request chunks from this member"""
        first_chunk = min(chunks_set)
        
        # Get first range of continuous chunks
        x = first_chunk + 1
        while (x in chunks_set) and (first_chunk + 1000 > x):
            x = x + 1
        last_chunk = x - 1

        # Build a set
        request = set()
        for x in range(first_chunk, last_chunk+1):
            request.add(x)

        # Build a message
        req = MsgRequest.MsgRequest()
        req.start_chunk = first_chunk
        req.end_chunk = last_chunk

        data = bytearray()
        data[0:4] = struct.pack('>I', self.remote_channel)
        data[4:] = bytes([MT.REQUEST])
        data[5:] = req.BuildBinaryMessage()

        #logging.info("TX > To: {0}; Msg: {1}".format(self, req))

        self.SendAndAccount(data)
        self._swarm.set_requested = self._swarm.set_requested.union(request)

    def HandleIntegrity(self, msg_integrity):
        """Handle the incomming integorty message"""
        self._swarm.integrity[(msg_integrity.start_chunk, msg_integrity.end_chunk)] = msg_integrity.hash_data

    def HandleAck(self, msg_ack):
        """Handle incomming ACK message"""
        for x in range(msg_ack.start_chunk, msg_ack.end_chunk + 1):
            self.set_requested.discard(x)

    def HandleRequest(self, msg_request):
        """Handle incomming REQUEST message"""
        for x in range(msg_request.start_chunk, msg_request.end_chunk + 1):
            self.set_requested.add(x)

        # Try to send some data
        if self._sending_handle == None:
           self._sending_handle = asyncio.get_event_loop().call_soon(self.SendRequestedChunks) 
        
#endregion

    def SetPeerParameters(self, msg_handshake):
        """Set Peer parameters as received in the HS message"""

        self.remote_channel = msg_handshake.their_channel
        self.hash_type = msg_handshake.merkle_tree_hash_func
                
        # Verify that we can understand each other
        if msg_handshake.chunk_addressing_method != 2:
            raise NotImplementedError("Not supported chunk addressing method!")
        else:
            self.chunk_addressing_method = msg_handshake.chunk_addressing_method
                
        if msg_handshake.chunk_size != 1024:
            raise NotImplementedError("Not standard chunk size!")
        else:
            self.chunk_size = msg_handshake.chunk_size

    def SendRequestedChunks(self):
        """Send the requested chunks to the peer"""
        self._chunk_sending_alg.SendAndSchedule()
                
    def ProcessOutbox(self):
        """Binarify and send all messages in the outbox"""
        # This method should do any re-arrangement if required

        # If outbox is empty not much to do
        if len(self._outbox) == 0:
            return

        data = bytearray()
        data[0:] = struct.pack('>I', self.remote_channel)

        for msg in self._outbox:
            msg_bin = msg.BuildBinaryMessage()
            #logging.info("Outbox: {0}".format(msg))
            data[len(data):] = msg_bin
            
        self._outbox.clear()

        self.SendAndAccount(data)

    def Disconnect(self):
        """Close association with the remote member"""

        # Build goodbye handshake
        hs = MsgHandshake.MsgHandshake()
        hs_bin = hs.BuildGoodbye()

        # Serialize
        data = bytearray()
        data[0:] = struct.pack('>I', self.remote_channel)
        data[4:] = hs_bin
        
        logging.info("Sending: {0}".format(hs))
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

    def __str__(self):
        return str("Peer {0}:{1} LC: {2}; RC: {3}"
                   .format(self.ip_address, self.udp_port, self.local_channel, self.remote_channel))

    def __rept(self):
        return self__str__()