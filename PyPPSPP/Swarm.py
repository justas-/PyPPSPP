import logging
import math
import asyncio
import datetime
import os
import hashlib
import struct
import sys
import socket
import time
import json
import binascii

from SwarmMember import SwarmMember
from GlobalParams import GlobalParams
from MerkleHashTree import MerkleHashTree
from Messages import *
from PeerProtocolTCP import PeerProtocolTCP

from AbstractChunkStorage import AbstractChunkStorage
from MemoryChunkStorage import MemoryChunkStorage
from FileChunkStorage import FileChunkStorage
from ContentConsumer import ContentConsumer
from ContentGenerator import ContentGenerator

class Swarm(object):
    """A class used to represent a swarm in PPSPP"""

    def __init__(self, socket, args):
        """Initialize the object representing a swarm"""
        self._args = args

        self.swarm_id = binascii.unhexlify(args.swarmid)
        self.live = args.live
        self.live_src = args.livesrc
        if args.discardwnd is not None:
            self.discard_wnd = int(args.discardwnd)

        self._socket = socket
        self._members = []

        # Logger for fast access
        self._logger = logging.getLogger()

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

        self._data_chunks_rx = 0        # Number of data chunks received overall
        self._last_discarded_id = 0     # Last discarded chunk id when used with live discard window

        self._next_peer_num = 1
        self._max_peers = args.numpeers

        self._cont_consumer = None
        self._cont_generator = None

        self._all_data_rx = 0
        self._all_data_tx = 0
        self._start_time = time.time()
        self._int_time = 0
        self._int_chunks = 0
        self._int_data_rx = 0
        self._int_data_tx = 0

        self._periodic_stats_handle = None
        self._periodic_stats_freq = 3

        self._member_stats = {}

        if self.live:
            # Initialize in memory chunk storage
            self._chunk_storage = MemoryChunkStorage(self)
            self._chunk_storage.Initialize(self.live_src)

            # Initialize live content generator
            self._cont_generator = ContentGenerator()
            #self._cont_generator.add_on_generated_callback(self._chunk_storage.ContentGenerated)
            self._cont_generator.add_on_generated_callback(self._chunk_storage.pack_data_with_de)

            if self.live_src:
                # Start generating if source
                self._cont_generator.start_generating()
            else:
                # Start requesting if not source
                self._cont_consumer = ContentConsumer(self)
                self._cont_consumer.allow_tune_in()
                self.StartChunkRequesting()
                self._cont_consumer.start_consuming()
        else:
            self._chunk_storage = FileChunkStorage(self)
            self._chunk_storage.Initialize(
                filename = args.filename, 
                filesize = args.filesize)

        logging.info("Created Swarm with ID: {0}".format(self.swarm_id))

        # Start printing stats
        self._periodic_stats_handle = asyncio.get_event_loop().call_later(
            self._periodic_stats_freq,
            self._print_periodic_stats)

    def SendData(self, ip_address, port, data):
        """Send data over a socket used by this swarm"""
        self._socket.sendto(data, (ip_address, port))
        self._all_data_tx += len(data)

    def AddMember(self, ip_address, port = 6778, proto = None):
        """Add a member to a swarm and try to initialize connection"""
        
        if self._max_peers is not None and len(self._members) >= self._max_peers:
            logging.info("Swarm: Max number of peers reached (Skipping: {0}:{1})".format(ip_address, port))
            return

        logging.info("Swarm: Adding member at {0}:{1}".format(ip_address, port))

        for member in self._members:
            if member.ip_address == ip_address and member.udp_port == port:
                logging.info("Member {0}:{1} is already present and will be ignorred"
                             .format(ip_address, port))
                return None

        m = SwarmMember(self, ip_address, port, proto)
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
        assert start_chunk <= end_chunk
        assert start_chunk >= 0

        min_chunk = start_chunk
        max_chunk = end_chunk
                
        while min_chunk >= 0 and min_chunk-1 in self.set_have:
            min_chunk -= 1

        while max_chunk+1 in self.set_have:
            max_chunk += 1

        return (min_chunk, max_chunk)

    def StartChunkRequesting(self):
        """Start running chunk selection algorithm"""
        if self._chunk_selction_handle != None:
            # Error in program logic somewhere
            raise Exception

        # Schedule the execution of selection alg
        if self.live and not self.live_src:
            self._chunk_selction_handle = asyncio.get_event_loop().call_later(
                1 / self._selection_rps, 
                self.greedy_chunk_request)
        else:
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

    def greedy_chunk_request(self):
        """Implements a greedy chunk request algorithm for live streaming"""
        REQMAX = 1000           # Max number of outstanding requests from one peer
        REQTHRESH = 250         # Threshhold to request more pieces
        
        any_missing = any(self.set_missing)
        if self.live_src and any_missing:
            raise AssertionError("Live Source and missing chunks!")

        if not any_missing and self.live == False:
            logging.info("All chunks onboard. Not rescheduling request algorithm")
            return

        # Check if there's anything I need
        all_empty = True

        # Have local copy to save recomputing each time
        all_req_local = self._get_all_requested()

        # Request up to REQMAX from each member
        for member in self._members:
            set_i_need = member.set_have - self.set_have - all_req_local
            len_i_need = len(set_i_need)
            len_member_outstanding = len(member.set_i_requested)

            logging.info('Member: {}. I need {}; Outstanding: {}'
                         .format(member, len_i_need, len_member_outstanding))

            # At least one member has something I need
            if len_i_need > 0:
                all_empty = False

            # Do not bother asking for more until we are below threshold
            #if len_member_outstanding > REQTHRESH:
            #    continue

            member.RequestChunks(set_i_need)
            all_req_local = all_req_local | set_i_need

        # If I can't download anything from anyone - reset requested
        if all_empty == True:
            if self._logger.isEnabledFor(logging.DEBUG):
                logging.debug("Cleared rquested chunks set. Num missing: {}".format(len(self.set_missing)))
            self.set_requested.clear()

        # Schedule a call to select chunks again
        self._chunk_selction_handle = asyncio.get_event_loop().call_later(
            1 / self._selection_rps,
            self.greedy_chunk_request)

    def ChunkRequest(self):
        """Implements Chunks selection/request algorith"""

        REQMAX = 1000           # Max number of outstanding requests from one peer
        REQTHRESH = 250         # Threshhold to request more pieces
        
        any_missing = any(self.set_missing)
        if self.live_src and any_missing:
            raise AssertionError("Live Source and missing chunks!")

        if not any_missing and self.live == False:
            logging.info("All chunks onboard. Not rescheduling request algorithm")
            return

        # Check if there's anything I need
        all_empty = True

        # Have local copy to save recomputing each time
        all_req_local = self._get_all_requested()

        # Request up to REQMAX from each member
        for member in self._members:
            set_i_need = member.set_have - self.set_have - all_req_local
            len_i_need = len(set_i_need)
            len_member_outstanding = len(member.set_i_requested)

            logging.info('Member: {}. I need {}; Outstanding: {}'
                         .format(member, len_i_need, len_member_outstanding))

            # At least one member has something I need
            if len_i_need > 0:
                all_empty = False

            # Do not bother asking for more until we are below threshold
            if len_member_outstanding > REQTHRESH:
                continue

            if len_i_need >= REQMAX:
                # I need more than REQMAX, so request up to REQMAX chunks
                member_request = set(list(set_i_need)[0:REQMAX])
                member.RequestChunks(member_request)
                all_req_local = all_req_local | member_request
            else:
                # I need less than REQMAX, so request all
                member.RequestChunks(set_i_need)
                all_req_local = all_req_local | set_i_need

        # If I can't download anything from anyone - reset requested
        if all_empty == True:
            if self._logger.isEnabledFor(logging.DEBUG):
                logging.debug("Cleared rquested chunks set. Num missing: {}".format(len(self.set_missing)))
            self.set_requested.clear()

        # Schedule a call to select chunks again
        self._chunk_selction_handle = asyncio.get_event_loop().call_later(
            1 / self._selection_rps,
            self.ChunkRequest)

    def _get_all_requested(self):
        """Return a set of all chunks that I have
           requested from all known members
        """
        requested_set = set()
        for member in self._members:
            requested_set = requested_set | member.set_i_requested

        return requested_set

    def SaveVerifiedData(self, chunk_id, data):
        """Called when we receive data from a peer and validate the integrity"""
        # When using UDP we might get data after completion
        if not any(self.set_missing):
            return

        # Update stats
        self._data_chunks_rx += 1

        # Save chunk in our storage
        self._chunk_storage.SaveChunkData(chunk_id, data)
        
        # Update chunk maps
        self.set_have.add(chunk_id)
        self.set_missing.discard(chunk_id)
        
        # Feed data to live video consumer if required
        if self.live and not self.live_src:
            #self._cont_consumer.data_received(chunk_id, data)
            self._cont_consumer.data_received_with_de(chunk_id, data)

        # Run post complete actions (not any() is faster than len() == 0)
        if not any(self.set_missing):
            self._chunk_storage.PostComplete()

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

    def disconnect_and_remove_member(self, member):
        """Try to disconnect and remove member from the swarm"""
        if member not in self._members:
            logging.warn("Trying to remove a member that is not in the swarm! Member: {}".format(member))
            return

    def RemoveMember(self, member):
        """Remove indicated member from a swarm"""
        logging.info("Removing member {0} from a swarm".format(member))
        if member in self._members:
            self._members.remove(member)
        else:
            logging.info("Member {} not found in a swarm member's list"
                         .format(member))

    def ReportData(self):
        """Report amount of data sent and received from each peer"""
        logging.info("Data transfer stats:")
        for member in self._members:
            logging.info("   Member: {0};\tRX: {1} Bytes; TX: {2} Bytes"
                         .format(member, member._total_data_rx, member._total_data_tx))

    def _print_periodic_stats(self):
        # Get stats
        num_missing = len(self.set_missing)
        num_have = len(self.set_have)

        # Calculate time interval length
        t_int = 0
        if self._int_time == 0:
            t_int = time.time() - self._start_time
        else:
            t_int = time.time() - self._int_time

        # Calculate data in interval
        chunks_in_int = self._data_chunks_rx - self._int_chunks
        data_tx_in_int = self._all_data_tx - self._int_data_tx
        data_rx_in_int = self._all_data_rx - self._int_data_rx

        # Set the values
        self._int_chunks = self._data_chunks_rx
        self._int_data_rx = self._all_data_rx
        self._int_data_tx = self._all_data_tx
        self._int_time = time.time()

        # Print results
        logging.info("# Have/Missing {}/{}; Down ch/s: {}; Speed up/down: {}/{} Bps"
                     .format(
                         num_have, 
                         num_missing, 
                         int(chunks_in_int/t_int),
                         int(data_tx_in_int/t_int),
                         int(data_rx_in_int/t_int)))

        self._periodic_stats_handle = asyncio.get_event_loop().call_later(
            self._periodic_stats_freq,
            self._print_periodic_stats)

    def _save_member_stats(self, name, stats):
        """Save given member stats"""
        # Do not overwrite
        if name in self._member_stats:
            return
        self._member_stats[name] = stats

    def _log_data(self, data):
        """Log all data from the data dict to unique file"""
        
        result_file = self._args.output_dir+'results_'+self._args.result_id+".dat"
        with open(result_file,"w") as fp:
            fp.write(json.dumps(data))
        logging.info("Wrote logs to file: {}".format(result_file))

    def close_swarm(self):
        """Close the swarm and send disconnect to all members"""
        logging.info("Request to close swarm nicely!")

        # Send departure handshakes
        for member in self._members:
            member.destroy()
        self._members.clear()

        self.ReportData()
                
        ############################
        report = {}
        report['data_tx'] = self._all_data_tx
        report['data_rx'] = self._all_data_rx

        report['live'] = self.live
        report['live_src'] = self.live_src
        
        if isinstance(self._chunk_storage, FileChunkStorage):
            report['file_ts_start'] = self._chunk_storage._ts_start
            report['file_ts_end'] = self._chunk_storage._ts_end
            report['start_source'] = self._chunk_storage._start_source
        elif self._chunk_storage is MemoryChunkStorage:
            pass

        report['start_time'] = self._start_time
        report['close_time'] = time.time()

        report['run_args'] = vars(self._args)
        report['member_stats'] = self._member_stats

        if self.live and not self.live_src:
            self._cont_consumer.stop_consuming()
            report['content_consumer'] = self._cont_consumer.get_stats()

        self._log_data(report)
        ############################

        # Close chunk storage
        self._chunk_storage.CloseStorage()
