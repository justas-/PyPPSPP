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
import datetime
import asyncio
import math
import pickle
import struct

from ContentGenerator import ContentGenerator
from AbstractChunkStorage import AbstractChunkStorage
from GlobalParams import GlobalParams
from Framer import Framer

class MemoryChunkStorage(AbstractChunkStorage):
    """Memory backed chunk storage"""

    def __init__(self, swarm):
        super().__init__(swarm)
        
        self._chunks = {}
        self._cg = None
        self._is_source = False
        self._next_inject_id = 0

        self._num_chunks_received = 0   # Number of all chunks received
        self._num_unique_received = 0   # Number of unique chunks received

        self._have_outstanding = 0

    def Initialize(self, is_source):
        """Initialize In Memory storage"""

        if is_source == True:
            self._is_source = True

    def CloseStorage(self):
        self._chunks.clear()
        self._chunks = None

    def GetChunkData(self, chunk_id, ignore_missing = False):
        if chunk_id in self._chunks:
            return self._chunks[chunk_id]
        else:
            if not ignore_missing:
                logging.info("Received request for missing chunk: {0}".format(chunk_id))
            return None

    def SaveChunkData(self, chunk_id, data):
        """Save given data to the memory backed storage"""
        if self._is_source:
            # We are not saving in source mode
            raise AssertionError("Saving received data in live source mode!")

        # Count all received
        self._num_chunks_received += 1
        
        # We are relay - we can save this data
        if chunk_id in self._chunks.keys():
            logging.info("Received duplicate data. Chunk {0} is already known".format(chunk_id))
            return
        else:
            # Count unique received
            self._num_unique_received += 1

            # Save and account
            self._chunks[chunk_id] = data
            self._swarm.set_missing.discard(chunk_id)
            self._swarm.set_have.add(chunk_id)

            # If live discarding is used - do the discard
            if self._swarm.discard_wnd is not None:
                self.discard_old_chunks()

            # Send have ranges to other members every 100th chunk
            if self._num_unique_received % 100 == 0:
                self.BuildHaveRanges()
                self._swarm.SendHaveToMembers()

            # Print stats every 100'th chunk
            if self._num_chunks_received % 100 == 0:
                last_known = 0
                len_missing = len(self._swarm.set_missing)
                if len_missing == 0:
                    last_known = max(self._swarm.set_have)
                else:
                    last_known = max(self._swarm.set_missing)

                num_have_ranges = len(self._swarm._have_ranges)
                logging.info("Saved chunk {0}; Num missing: {1}; Last known: {2}; Num have ranges: {3}"
                             .format(chunk_id, len_missing, last_known, num_have_ranges))
                if num_have_ranges < 10:
                    logging.info('Have ranges: {}'.format(self._swarm._have_ranges))

    def ContentGenerated(self, data):
        # Pickle audio and video data
        ser_data = pickle.dumps(data)
        
        # Build one big bytes array
        msg_bytes = bytearray()
        msg_bytes.extend(struct.pack('>I', len(ser_data)))
        msg_bytes.extend(ser_data)

        # Chop into chunk_size pieces
        packs = []
        
        data_packed = 0
        all_data = len(msg_bytes)
        while data_packed < all_data:
            if all_data - data_packed > GlobalParams.chunk_size:
                # We have enough data for a full packet
                pack = bytearray()
                pack.extend(msg_bytes[data_packed:data_packed+GlobalParams.chunk_size])
                packs.append(pack)
                data_packed += GlobalParams.chunk_size
            else:
                # Make last pack by extendig it with zeros
                last_pack = bytearray()
                last_pack.extend(msg_bytes[data_packed:])
                last_pack.extend((GlobalParams.chunk_size - len(last_pack)) * bytes([0]))
                packs.append(last_pack)
                data_packed = all_data

        # Inject into system
        self.inject_chunks(packs)

        # Reduce the number of have messages
        self._have_outstanding += len(packs)
        if self._have_outstanding >= 100:
            self.build_distribute_have_live_src()
            self._have_outstanding = 0

    def pack_data_with_de(self, data):
        """Pickle and pack data using DiscardEligible format"""
        # Every first packet from pickled data will be marked as
        # not eligible for discarding. This way we can lock to
        # meaningful data in the live-receiver. Eligibility for
        # discard is indicated by [0/1] as the first byte.

        # Pickle the data
        binary_data = pickle.dumps(data)
        data_size = len(binary_data)

        # Prevent overflow
        assert data_size < 4294967295 # 2^32 - 1

        # Append length indicator in front
        msg_bytes = bytearray()
        msg_bytes.extend(struct.pack('>I', data_size))
        msg_bytes.extend(binary_data)

        # Pack data into ChunkSize chunks
        chunks = []

        first_packed = False
        data_packed = 0
        all_data = 4 + data_size # bytes for len + data
        
        while data_packed < all_data:
            chunk = bytearray()

            if all_data - data_packed > GlobalParams.chunk_size - 1:
                # We have enough data to fill one full packet
                
                # Check if this chunk is eligible for discard
                if first_packed:
                    chunk.extend(bytes([1])) # Eligible for discard
                else:
                    chunk.extend(bytes([0])) # Not eligible for discard
                    first_packed = True

                # Packe the data
                chunk.extend(msg_bytes[data_packed:data_packed+GlobalParams.chunk_size - 1])
                chunks.append(chunk)

                # Update number of bytes packed
                data_packed += GlobalParams.chunk_size - 1
            else:
                # Not enough data to fill all packet - will pad with 0

                # Check if this chunk is eligible for discard
                if first_packed:
                    chunk.extend(bytes([1])) # Eligible for discard
                else:
                    chunk.extend(bytes([0])) # Not eligible for discard
                    first_packed = True

                # Add data
                chunk.extend(msg_bytes[data_packed:])

                # Add padding
                chunk.extend((GlobalParams.chunk_size - len(chunk)) * bytes([0]))
                chunks.append(chunk)
                data_packed = all_data

        # Inject into system
        self.inject_chunks(chunks)

        # Discard old chunks if needed
        if self._swarm.discard_wnd is not None:
            self.discard_old_chunks()

        # Reduce the number of have messages
        self._have_outstanding += len(chunks)
        if self._have_outstanding >= 100:
            self.build_distribute_have_live_src()
            self._have_outstanding = 0

    def build_distribute_have_live_src(self):
        """Update have ranges and send them to the connected peers.
           This alg is optimized for live source use case!
        """
        self.BuildHaveRangesLiveSrc()
        self._swarm.SendHaveToMembers()

    def inject_chunks(self, chunks):
        """Inject [chunks] into the system"""

        for chunk in chunks:
            # Ensure the correct size of data before sending it into the system
            assert len(chunk) == GlobalParams.chunk_size

            self._chunks[self._next_inject_id] = chunk
            self._swarm.set_have.add(self._next_inject_id)
            self._next_inject_id += 1

    def BuildHaveRangesLiveSrc(self):
        # Build have ranges in Live Source
        assert self._swarm.live and self._swarm.live_src
        self._swarm._have_ranges.clear()
        self._swarm._have_ranges.append((self._swarm._last_discarded_id + 1, self._next_inject_id - 1))
    
    def BuildHaveRanges(self):
        """Build HAVE ranges"""
        
        # In data len: 18177 (Shuffled)
        # 8758d5ffb54d9fe6f4cec3ead8fb6d7c19454f01 
        # 30 runs: time: 161.36131001623446. Time per run: 5.378710333874482
        # Latest:
        # 30 runs: time: 7.854214742891969. Time per run: 0.261807158096399
        # For live streaming with 20K continuous items speed for new is about twice faster

        present_chunks = list(self._chunks)
        present_chunks.sort()

        ranges = []

        in_range = False
        x_min = 0
        num_chunks = len(present_chunks)

        for key, chunk_num in enumerate(present_chunks):
            if not in_range:
                x_min = chunk_num
                in_range = True

            if in_range:
                # Check for end
                if num_chunks == key + 1:
                    ranges.append((x_min, chunk_num))
                    break
                # Not the end -> check next
                elif present_chunks[key+1] == chunk_num + 1:
                    continue
                # Range ends here - add as a resulting range
                else:
                    ranges.append((x_min, chunk_num))
                    in_range = False

        self._swarm._have_ranges = ranges

    def discard_old_chunks(self):
        """Discard chunks below the discard threshold"""
        min_have = min(self._swarm.set_have)
        max_have = max(self._swarm.set_have)

        # Check if we have anything to discard?
        # Chunks start at 0!
        if max_have - min_have + 1 > self._swarm.discard_wnd:

            # Discard all items below discard window
            for chunk_id in range(min_have, max_have - self._swarm.discard_wnd + 2):
                if chunk_id in self._swarm.set_have:
                    self._swarm.set_have.discard(chunk_id)
                    self._swarm.set_missing.discard(chunk_id)
                if chunk_id in self._chunks:
                    del self._chunks[chunk_id]

            # Set last discarded ID
            self._swarm._last_discarded_id = max_have - self._swarm.discard_wnd + 1
