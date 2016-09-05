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
        
        self._framer = None   # Frame from network data to A/V frames
        self._next_frame = 1  # Id of the next chunk that should be fed to framer

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
            self._framer = Framer(self.DataFramed)

    def CloseStorage(self):
        self._chunks.clear()
        self._chunks = None

    def GetChunkData(self, chunk):
        if chunk in self._chunks.keys():
            return self._chunks[chunk]
        else:
            logging.info("Received request for missing chunk: {0}"
                         .format(chunk))
            return None

    def SaveChunkData(self, chunk_id, data):
        if self._is_source:
            # We are not saving in source mode
            raise Exception
        else:
            if chunk_id in self._chunks.keys():
                # TODO: Log duplicate data
                return
            else:
                self._chunks[chunk_id] = data
                self.BuildHaveRanges()
                self._swarm.SendHaveToMembers() # TODO: Every time?

        if self._is_source == False:
            # If we are not source - we need to unframe data as well
            for x in self._chunks.keys():
                if x < self._last_framed:
                    # This chunk is already framed
                    continue
                if x == self._next_frame:
                    # Feed the framer if this is what we need
                    self._framer.DataReceived(self._chunks[x])
                    self._next_frame += 1

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

        logging.info("Injected into system {0} chunks ({1}-{2})"
                     .format(len(packs), first_ch, last_ch))
        
        self._swarm.SendHaveToMembers()

    def BuildHaveRanges(self):
        """Update have ranges based on the content in Memory storage"""
        self._swarm._have_ranges.clear()

        present_chunks = list(self._chunks.keys())
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

    def DataFramed(self, data):
        """Called by framer once data arrives"""
        av_data = pickle.loads(data)
        logging.info("Got AV data!")