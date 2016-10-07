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
        self._last_inject_id = 0
        self._last_discard_id = 0

        self._num_chunks_received = 0   # Number of all chunks received
        self._num_unique_received = 0   # Number of unique chunks received

        self._have_built = False
        self._have_keys = {}

    def Initialize(self, is_source):
        """Initialize In Memory storage"""

        if is_source == True:
            self._is_source = True

    def CloseStorage(self):
        self._chunks.clear()
        self._chunks = None

    def GetChunkData(self, chunk, ignore_missing = False):
        if chunk in self._chunks.keys():
            return self._chunks[chunk]
        else:
            if not ignore_missing:
                logging.info("Received request for missing chunk: {0}".format(chunk))
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
            self._swarm.set_missing.remove(chunk_id)
            self._swarm.set_have.add(chunk_id)

            # Send have ranges to other members every 20th chunk
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

                logging.info("Saved chunk {0}; Num missing: {1}; Last known: {2}; Num have ranges: {3}"
                             .format(chunk_id, len_missing, last_known, len(self._swarm._have_ranges)))

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
        first_ch = self._last_inject_id + 1
        for pack in packs:
            self._last_inject_id += 1
            self._chunks[self._last_inject_id] = pack
            self._swarm.set_have.add(self._last_inject_id)
        last_ch = self._last_inject_id

        self.BuildHaveRangesLiveSrc()
        self._swarm.SendHaveToMembers()

    def BuildHaveRangesLiveSrc(self):
        # Build have ranges in Live Source
        assert self._swarm.live and self._swarm.live_src
        self._swarm._have_ranges.clear()
        self._swarm._have_ranges.append((self._last_discard_id + 1, self._last_inject_id))
    
    def BuildHaveRanges(self):
        # Build live ranges in all other nodes
        # TODO: Optimize this
        present_chunks = list(self._chunks)
        present_chunks.sort()
        
        self._swarm._have_ranges.clear()

        in_range = False
        x_min = 0
        for x in present_chunks:
            if in_range == False:
                x_min = x
                in_range = True
        
            if in_range:
                if x + 1 in present_chunks:
                    continue
                else:
                    self._swarm._have_ranges.append((x_min, x))
                    in_range = False