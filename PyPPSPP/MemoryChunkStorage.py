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

# TODO: Generating and injecting should be carved away from this one...

class MemoryChunkStorage(AbstractChunkStorage):
    """Memory backed chunk storage"""

    def __init__(self, swarm):
        super().__init__(swarm)
        
        self._chunks = {}
        self._cg = None
        self._is_source = False
        self._last_inject_id = 0
        self._last_discard_id = 0
        
        self._framer = None   # Frame from network data to A/V frames
        self._next_frame = 1  # Id of the next chunk that should be fed to framer

        self._num_chunks_received = 0   # Number of all chunks received
        self._num_unique_received = 0   # Number of unique chunks received
        self._av_log_remover = 0

        self._have_built = False
        self._have_keys = {}

    def Initialize(self, is_source):
        """Initialize In Memory storage"""

        if is_source == True:
            self._is_source = True
            self._cg = ContentGenerator(
                asyncio.get_event_loop(),
                self.ContentGenerated,
                0)
            self._cg.StartGenerating()
        else:
            self._framer = Framer(self.DataFramed, av_framer=True)

    def CloseStorage(self):
        self._chunks.clear()
        self._chunks = None

    def GetChunkData(self, chunk):
        if chunk in self._chunks.keys():
            return self._chunks[chunk]
        else:
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
            self.ExtendBuild(chunk_id)

            # Send have ranges to other members every 20th chunk
            if self._num_unique_received % 20 == 0:
                self._swarm.SendHaveToMembers()

            # If we are not source - we need to rebuild AV packets
            # First assume that we have no holes in sequence:
            if chunk_id == self._next_frame:
                # Inject data into framer
                self._framer.DataReceived(self._chunks[chunk_id])

                self._next_frame += 1
            else:
                # Handle hole in the sequence
                for x in self._chunks.keys():
                    if x < self._next_frame:
                        # This chunk is already framed
                        continue
                    if x == self._next_frame:
                        # Feed the framer if this is what we need
                        self._framer.DataReceived(self._chunks[x])
                        self._next_frame += 1

            # Print stats every 100'th chunk
            if self._num_chunks_received % 100 == 0:
                last_known = 0
                len_missing = len(self._swarm.set_missing)
                if len_missing == 0:
                    last_known = max(self._swarm.set_have)
                else:
                    last_known = max(self._swarm.set_missing)

                logging.info("Saved chunk {0}; Num missing: {1}; Last known: {2}; Next to framer: {3}; Nr: {4}"
                             .format(chunk_id, len_missing, last_known, self._next_frame, self._swarm._have_ranges))


    def ExtendBuild(self, chunk_id):
        """Special case for live consumer"""

        if self._have_built == False:
            self._swarm._have_ranges.append((chunk_id, chunk_id))
            self._have_built = True
            return

        k = 0
        num_ranges = len(self._swarm._have_ranges)

        for range in self._swarm._have_ranges:
            if range[1] == chunk_id - 1:
                # Extend current range if we have next
                self._swarm._have_ranges[k] = (range[0], chunk_id)
                return
            else:
                if k + 1 == num_ranges:
                    # Start a new range
                    self._swarm._have_ranges.append((chunk_id, chunk_id))
                    return
                else:
                    # Continue
                    k += 1

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

        self.BuildHaveRanges()

        #logging.info("Injected into system {0} chunks ({1}-{2})"
        #             .format(len(packs), first_ch, last_ch))
        
        self._swarm.SendHaveToMembers()

    def BuildHaveRanges(self):
        """Update have ranges based on the content in Memory storage"""
        # Small optimization for source of live streaming
        if self._swarm.live and self._swarm.live_src:
            self._swarm._have_ranges.clear()
            self._swarm._have_ranges.append(
                (self._last_discard_id + 1, self._last_inject_id))
            return


        #present_chunks = list(self._chunks.keys())
        #in_range = False
        #x_min = 0

        #for x in present_chunks:
        #    if in_range == False:
        #        x_min = x
        #        in_range = True
        #
        #    if in_range:
        #        if x + 1 in present_chunks:
        #            continue
        #        else:
        #            self._swarm._have_ranges.append((x_min, x))
        #            in_range = False

    def DataFramed(self, data):
        """Called by framer once data arrives"""
        av_data = pickle.loads(data)
        if self._av_log_remover % 50 == 0:
            logging.info("Got AV data! Seq: {0}; Video size: {1}; Audio size: {2}"
                         .format(av_data['id'], len(av_data['vd']), len(av_data['ad'])))
        self._av_log_remover += 1
