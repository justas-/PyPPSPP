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
import operator
import uuid
import random

from SwarmMember import SwarmMember
from GlobalParams import GlobalParams
from Messages import *
from PeerProtocolTCP import PeerProtocolTCP
from ALTOInterface import ALTOInterface

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
        self.vod = args.vod
        if args.discardwnd is not None:
            self.discard_wnd = int(args.discardwnd)
        else:
            self.discard_wnd = None

        if args.dlfwd is not None:
            self.dlfwd = args.dlfwd
        else:
            self.dlfwd = 0

        self._uuid = uuid.uuid4()

        # setup for ALTO
        self._use_alto = False
        self._alto_cost_type = None
        self._alto = None
        self._alto_period = 0
        self._alto_event = None
        self._alto_members = None
        self._alto_addr = None

        if args.alto:
            self._use_alto = True
            self._alto_cost_type = args.altocosttype
            self._alto_period = 15
            self._alto_addr = args.altoserver

        self._socket = socket
        self._members = []
        self._known_peers = set()       # Set of (IP, Port) tuples of other known peers

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
        self._last_discarded_id = -1     # Last discarded chunk id when used with live discard window

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
        self._discarded_rx = 0

        self._periodic_stats_handle = None
        self._periodic_stats_freq = 3

        self._member_stats = {}

        if self.vod:
            # Setup client for VOD role
            self._chunk_storage = MemoryChunkStorage(self)
            self._chunk_storage.Initialize(False)

            self._cont_consumer = ContentConsumer(self, args)
            self._cont_consumer._content_len = 3330 # TODO: Get from tracker!
            self.StartChunkRequesting()
            self._cont_consumer.start_consuming()

        elif self.live:
            # Initialize in memory chunk storage
            self._chunk_storage = MemoryChunkStorage(self)
            self._chunk_storage.Initialize(self.live_src)

            if self.live_src:
                # Initialize live content generator
                self._cont_generator = ContentGenerator()
                
                #self._cont_generator.add_on_generated_callback(self._chunk_storage.ContentGenerated)
                self._cont_generator.add_on_generated_callback(self._chunk_storage.pack_data_with_de)
                
                # Start generating if source
                self._cont_generator.start_generating()
            else:
                # Start requesting if not source
                self._cont_consumer = ContentConsumer(self, args)
                self._cont_consumer.allow_tune_in()
                self.StartChunkRequesting()
                self._cont_consumer.start_consuming()
        else:
            self._chunk_storage = FileChunkStorage(self)
            self._chunk_storage.Initialize(
                filename = args.filename, 
                filesize = args.filesize)

        logging.info('Created Swarm with ID: %s; Our UUID: %s', args.swarmid, self._uuid)

        # Start periodic ALTO requests
        if self._use_alto:
            logging.info('Initializing ALTO interface to server %s', self._alto_addr)
            self._alto = ALTOInterface(self._alto_addr)
            self._alto_event = asyncio.get_event_loop().call_later(
                self._alto_period, self.alto_lookup)

        # Start printing stats
        self._periodic_stats_handle = asyncio.get_event_loop().call_later(
            self._periodic_stats_freq,
            self._print_periodic_stats)

    def alto_lookup(self):
        """Request ranking using given cost provider"""
        member_ips = [member.ip_address for member in self._members]
        self._alto.rank_sources(
            member_ips, self._alto_cost_type, self.alto_callback)
        self._alto_event = asyncio.get_event_loop().call_later(
            self._alto_period, self.alto_lookup)

    def alto_callback(self, cost_map):
        """Callback for ALTO lookup"""

        # Print debug information
        logging.info('Got cost-map from ALTO: %s', cost_map)

        # Do not use stale ALTO data
        if cost_map is None:
            self._alto_members = None
            return

        # Build a list using ALTO returned keys and the rest at the end
        if self._alto_cost_type == 'residual-pathbandwidth':
            ips_min_to_max = sorted(cost_map.items(), key=operator.itemgetter(1), reverse=True)
        else:
            ips_min_to_max = sorted(cost_map.items(), key=operator.itemgetter(1))

        sorted_members = []
        members_copy = self._members.copy()

        # Add members by the cost
        for (ip, cost) in ips_min_to_max:
            member = next((m for m in members_copy if m.ip_address == ip), None)
            if member is None:
                continue
            else:
                sorted_members.append(member)
                members_copy.remove(member)

        # Add the remaining members
        sorted_members.extend(members_copy)

        # Update the parameter
        self._alto_members = sorted_members

    def SendData(self, ip_address, port, data):
        """Send data over a socket used by this swarm"""
        self._socket.sendto(data, (ip_address, port))
        self._all_data_tx += len(data)

    def any_valid_members_at(self, ip_address):
        """Return True if swarm has any init members at given IP"""
        
        for member in self._members:
            if member.is_init and member.ip_address == ip_address:
                return True
        
        return False

    def any_free_peer_slots(self):
        """Check if the swarm can accept any peers"""
        if self._max_peers is None or len(self._members) < self._max_peers:
            return True

    def AddMember(self, ip_address, port = 6778, proto = None):
        """Add a member to a swarm and try to initialize connection"""
        
        logging.info('Swarm::AddMember Request: IP: %s; Port: %s; Proto present: %s',
                     ip_address, port, bool(proto))

        # Check if the swarm is full
        if self._max_peers is not None and len(self._members) >= self._max_peers:
            logging.info("Swarm: Max number of peers reached (Skipping: {0}:{1})".format(ip_address, port))
            return 'E_FULL'

        # Check for duplicate members
        #if proto is None:
        #    # UDP
        #    if any([m for m in self._members if m.ip_address == ip_address and m.udp_port == port]):
        #        logging.info('Member {0}:{1} is already present and will be ignorred'
        #                     .format(ip_address, port))
        #        return 'E_DUP_UDP'
        #else:
        #    #TCP
        #    if any([m for m in self._members if m.ip_address == ip_address]):
        #        logging.info('Member at {0} is already present and will be ignorred'.format(ip_address))
        #        return 'E_DUP_TCP'

        new_member = SwarmMember(self, ip_address, port, proto, self._next_peer_num)
        self._next_peer_num += 1
        self._members.append(new_member)

        logging.info('Swarm::AddMember: Created member: %s', new_member)

        return new_member

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
        if self.vod:
            self._chunk_selction_handle = asyncio.get_event_loop().call_later(
                1 / self._selection_rps, 
                self.greedy_chunk_request)
            return

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
        """Implements a greedy chunk request algorithm for live streaming
           This algorith neves stops running (i.e. there is no stop threshold).
        """
        
        any_missing = any(self.set_missing)
        if self.live_src and any_missing:
            raise AssertionError("Live Source and missing chunks!")

        if any(self.set_have) and not any_missing and not self.live:
            logging.info("All chunks onboard. Not rescheduling request algorithm")
            return

        # Have local copy to save recomputing each time
        all_req_local = self._get_all_requested()

        # Take note what is the last chunkid fed into Content consumer

        playback_started = False
        last_showed = None
       
        if self._cont_consumer is None:
            logging.error('Forward window set, but missing content consumer! Dlfwd will be turned off!')
            self.dlfwd = 0
        else:
            playback_started = self._cont_consumer.playback_started()
            if playback_started:
                last_showed = self._cont_consumer.last_showed_chunk()
                if last_showed is None:
                    last_showed = 0
                max_permitted = last_showed + self.dlfwd
            else:
                # TODO: Adjust this! The number should be small enough to encompass all chunk within starting window
                max_permitted = self.dlfwd + 1000

        if playback_started:
            OUTSTANDING_LIMIT = 150
            REQUEST_LIMIT = 250
        else:
            OUTSTANDING_LIMIT = 100
            REQUEST_LIMIT = 150
            
        # Check all members for any missing pieces
        if self._use_alto and self._alto_members is not None:
            # During startup ALTO member list might be smaller, so
            # use normal members list
            if len(self._alto_members) >= len(self._members) - 3:
                logging.info('Using ALTO sorted members list')
                members_list = self._alto_members
            else:
                members_list = self._members
        else:
            members_list = self._members

        logging.info('Have ranges: {}; LastSh: {}; Max permitted: {}; Playing: {};'.format(self._have_ranges, last_showed, max_permitted, playback_started))

        # Poor man's load balancing
        random.shuffle(members_list)

        for member in members_list:
            # Build missing chunks
            req_chunks_no_filter = member.set_have - self.set_have - all_req_local
            
            # Filter for Discard and Forward Windows
            if self.discard_wnd is not None:
                if self.dlfwd != 0:
                    # DL & Discard windows 
                    required_chunks = [x for x in req_chunks_no_filter if x > self._last_discarded_id and x < max_permitted]
                else:
                    # Discard window only
                    required_chunks = [x for x in req_chunks_no_filter if x > self._last_discarded_id]
            else:
                if self.dlfwd != 0:
                    # Only DL Window filtering
                    required_chunks = [x for x in req_chunks_no_filter if x < max_permitted]
                else:
                    # No filtering
                    required_chunks = list(req_chunks_no_filter)

            b_any_required = any(required_chunks)
            b_any_outstanding = any(member.set_i_requested)

            # Not needing anything and not waiting for anything
            #if not b_any_outstanding and not b_any_required:
            #    continue

            num_required = len(required_chunks)
            num_outstanding = len(member.set_i_requested)
            num_member_has = len(member.set_have)

            logging.info('(Greedy) Member: {}. Has: {}; I need {}; Outstanding: {}'
                         .format(member, num_member_has, num_required, num_outstanding))
            
            # Continue if there's nothing to request
            if not b_any_required:
                continue

            # Continue if backlog is large
            if num_outstanding > OUTSTANDING_LIMIT:
                continue

            # Request up to REQ_LIMIT chunks
            if num_required > REQUEST_LIMIT:
                required_chunks.sort()
                required_chunks = required_chunks[0:REQUEST_LIMIT]
            
            # Request the data and keep track of requests
            set_to_request = set(required_chunks)
            member.RequestChunks(set_to_request)
            all_req_local = all_req_local | set_to_request

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
            set_i_need = set(filter(lambda x: x > self._last_discarded_id, set_i_need))
            
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

        if chunk_id <= self._last_discarded_id:
            self._discarded_rx += 1
            logging.info('Received chunk ({}) in discarded range'.format(chunk_id))
            return

        # Update stats
        self._data_chunks_rx += 1

        # Save chunk in our storage
        self._chunk_storage.SaveChunkData(chunk_id, data)
        
        # Update chunk maps
        self.set_have.add(chunk_id)
        self.set_missing.discard(chunk_id)
        
        # Feed data to live video consumer if required
        if self.vod:
            self._cont_consumer.data_received_with_de(chunk_id, data)

        elif self.live and not self.live_src:
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
        
        num_sent = 0
        for member in [m for m in self._members if m.is_init]:
            hs = bytearray()
            hs.extend(struct.pack('>I', member.remote_channel))
            hs.extend(msg)
            member.SendAndAccount(hs)
            logging.info("Sent HAVE(%s) to peer: %s", self._have_ranges, member)

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

        # Number of members in the swarm
        known_members = len(self._members)
        valid_members = sum(m.is_init for m in self._members)

        # Print results
        logging.info("# Have/Missing {}/{}; Down ch/s: {}; Speed up/down: {}/{} Bps; Members Known/Valid: {}/{}"
                     .format(
                         num_have, 
                         num_missing, 
                         int(chunks_in_int/t_int),
                         int(data_tx_in_int/t_int),
                         int(data_rx_in_int/t_int),
                         known_members,
                         valid_members))

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
        
        if not os.path.exists(self._args.output_dir):
            os.makedirs(self._args.output_dir)

        result_file = self._args.output_dir+'results_'+self._args.result_id+".dat"
        with open(result_file,"w") as fp:
            fp.write(json.dumps(data))

        logging.info("Wrote logs to file: {}".format(result_file))

    def get_member_by_uuid(self, calling_member, member_uuid):
        """Get member having indicated UUID"""

        if member_uuid is None:
            return None

        for member in self._members:
            if calling_member != member and member.uuid == member_uuid:
                return member

        return None

    def add_other_peers(self, other_peers):
        """Add other known peers in [(IP, Port)] form"""

        for peer in other_peers:
            if peer not in self._known_peers:
                self._known_peers.add(peer)

    def remove_other_peers(self, other_peers):
        """Remove other known peers"""

        for peer in other_peers:
            self._known_peers.discard(peer)

    def close_swarm(self):
        """Close the swarm and send disconnect to all members"""
        logging.info("Request to close swarm nicely!")

        # Cancel events
        if self._alto_event is not None:
            self._alto_event.cancel()
            self._alto_event = None

        if self._periodic_stats_handle is not None:
            self._periodic_stats_handle.cancel()
            self._periodic_stats_handle = None

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
        report['rx_discarded'] = self._discarded_rx

        if self.vod:
            self._cont_consumer.stop_consuming()
            report['content_consumer'] = self._cont_consumer.get_stats()

        if self.live and not self.live_src:
            self._cont_consumer.stop_consuming()
            report['content_consumer'] = self._cont_consumer.get_stats()

        self._log_data(report)
        ############################

        # Close chunk storage
        self._chunk_storage.CloseStorage()
