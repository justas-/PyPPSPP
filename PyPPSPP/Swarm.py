import logging
import math
import asyncio
import datetime
import os
import hashlib
import struct

from SwarmMember import SwarmMember
from GlobalParams import GlobalParams
from MerkleHashTree import MerkleHashTree
from Messages import *

from AbstractChunkStorage import AbstractChunkStorage
from MemoryChunkStorage import MemoryChunkStorage
from FileChunkStorage import FileChunkStorage
from ContentConsumer import ContentConsumer
from ContentGenerator import ContentGenerator

class Swarm(object):
    """A class used to represent a swarm in PPSPP"""

    def __init__(self, socket, swarm_id, filename, filesize, live, live_src):
        """Initialize the object representing a swarm"""
        self.swarm_id = swarm_id
        self.live = live
        self.live_src = live_src

        self._socket = socket
        self._members = []

        # data
        # TODO: Live discard window!
        self._selection_rps = 1         # Frequency of selection alg run (runs per second)
        self._chunk_storage = None
        self._chunk_selction_handle = None
        self._chunk_offer_handle = None

        self.set_have = set()           # This is all I have
        self.set_missing = set()        # This is what I am missing. Always empty in the source of the live stream
        self.set_requested = set()      # This is what I have requested. Always empty in the source of the live stream

        self._have_ranges = []          # List of ranges of chunks we have verified
        self._last_num_missing = 0

        self._data_chunks_rx = 0        # Number of data chunks received overall

        self._next_peer_num = 1

        self._cont_consumer = None
        self._cont_generator = None

        if self.live:
            # Initialize in memory chunk storage
            self._chunk_storage = MemoryChunkStorage(self)
            self._chunk_storage.Initialize(self.live_src)

            # Initialize live content generator
            self._cont_generator = ContentGenerator()
            self._cont_generator.add_on_generated_callback(self._chunk_storage.ContentGenerated)

            if live_src:
                # Start generating if source
                self._cont_generator.start_generating()
            else:
                # Start requesting if not source
                self._cont_consumer = ContentConsumer(self)
                self.StartChunkRequesting()
                self._cont_consumer.StartConsuming()
        else:
            self._chunk_storage = FileChunkStorage(self)
            self._chunk_storage.Initialize(
                filename = filename, 
                filesize = filesize)

        logging.info("Created Swarm with ID: {0}".format(self.swarm_id))

    def SendData(self, ip_address, port, data):
        """Send data over a socket used by this swarm"""
        self._socket.sendto(data, (ip_address, port))

    def AddMember(self, ip_address, port = 6778):
        """Add a member to a swarm and try to initialize connection"""
        logging.info("Swarm: Adding member at {0}:{1}".format(ip_address, port))

        for member in self._members:
            if member.ip_address == ip_address and member.udp_port == port:
                logging.info("Member {0}:{1} is already present and will be ignorred"
                             .format(ip_address, port))
                return None

        m = SwarmMember(self, ip_address, port)
        self._members.append(m)

        m._peer_num = self._next_peer_num
        self._next_peer_num += 1

        return m

    def GetMemberByChannel(self, channel):
        """Get a member in a swarm with the given channel ID"""
        for m in self._members:
            if m.local_channel == channel:
                return m

        # No member found
        return None

    def GetAckRange(self, start_chunk, end_chunk):
        """Ref [RFC7574] §4.3.2 ACK message containing
           the chunk specification of its biggest interval
           covering the chunk"""

        min_chunk = 0
        max_chunk = 0
                
        while min_chunk >= 0 and min_chunk in self.set_have:
            min_chunk = min_chunk - 1
        min_chunk = min_chunk + 1

        max_chunk = min_chunk
        while max_chunk in self.set_have:
            max_chunk = max_chunk + 1
        max_chunk = max_chunk - 1

        return (min_chunk, max_chunk)

    def StartChunkRequesting(self):
        """Start running chunk selection algorithm"""
        if self._chunk_selction_handle != None:
            # Error in program logic somewhere
            raise Exception

        # Schedule the execution of selection alg
        self._chunk_selction_handle = asyncio.get_event_loop().call_later(
            1 / self._selection_rps, 
            self.ChunkRequest)

    def StopChunkRequesting(self):
        """Stop running chunk selection algorithm"""
        if self._chunk_selction_handle == None:
            # Error in program logic somewhere
            raise Exception

        self._chunk_selction_handle.cancel()
        self._chunk_selction_handle = None

    def ChunkRequest(self):
        """Implements Chunks selection/request algorith"""
        
        num_missing = len(self.set_missing)
        #logging.info("Running chunks selection algorithm. Live: {0}; Num missing: {1}"
        #             .format(self.live, num_missing))

        if num_missing == 0 and self.live == False:
            logging.info("All chunks onboard. Not rescheduling selection alg")
            return

        # If we are twice at the same fulfillment level - start re-requesting chunks
        #if num_missing == self._last_num_missing:
        #    for member in self._members:
        #        member.set_requested.clear()
        #    self.set_requested.clear()

        # TODO: Implement smart algorithm here
        for member in self._members:
            #set_i_need = member.set_have - self.set_have - self.set_requested
            set_i_need = member.set_have - self.set_have
            len_i_need = len(set_i_need)
            #logging.info("Need {0} chunks from member {1}"
            #             .format(len_i_need, member))
            if len_i_need > 0:
                member.RequestChunks(set_i_need)

        # Number of chunks missing at the end of chunk selection alg run
        #self._last_num_missing = len(self.set_requested)

        # Schedule a call to select chunks again
        self._chunk_selction_handle = asyncio.get_event_loop().call_later(
            1 / self._selection_rps,
            self.ChunkRequest)

    def SaveVerifiedData(self, chunk_id, data):
        """Called when we receive data from a peer and validate the integrity"""
        self._data_chunks_rx += 1
        self._chunk_storage.SaveChunkData(chunk_id, data)
        if self.live and not self.live_src:
            self._cont_consumer.DataReceived(chunk_id, data)

    def SendHaveToMembers(self):
        """Send to members all information about chunks we have"""
        
        # Build representation of our data using HAVE messages
        msg = bytearray()
        for range_data in self._have_ranges:
            have = MsgHave.MsgHave()
            have.start_chunk = range_data[0]
            have.end_chunk = range_data[1]
            msg.extend(have.BuildBinaryMessage())

        # Send our information to all members
        for member in [m for m in self._members if m._has_complete_data is False]:
            hs = bytearray()
            hs.extend(struct.pack('>I', member.remote_channel))
            hs.extend(msg)
            member.SendAndAccount(hs)

    def GetChunkData(self, chunk):
        """Get Data of indicated chunk"""
        return self._chunk_storage.GetChunkData(chunk)

    def RemoveMember(self, member):
        """Remove indicated member from a swarm"""
        logging.info("Removing member {0} from a swarm".format(member))
        self._members.remove(member)

    def ReportData(self):
        """Report amount of data sent and received from each peer"""
        logging.info("Data transfer stats:")
        for member in self._members:
            logging.info("   Member: {0};\tRX: {1} Bytes; TX: {2} Bytes"
                         .format(member, member._total_data_rx, member._total_data_tx))

    def CloseSwarm(self):
        """Close swarm nicely"""
        logging.info("Request to close swarm nicely!")
        self.ReportData()
        # Send departure handshakes
        for member in self._members:
            member.Disconnect()

        # Close chunk storage
        self._chunk_storage.CloseStorage()

        # If live - print data
        if self.live and not self.live_src:
            self._cont_consumer.StopConsuming()