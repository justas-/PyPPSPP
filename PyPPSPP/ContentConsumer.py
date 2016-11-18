import asyncio
import logging
import pickle
import queue
import time

from GlobalParams import GlobalParams
from Framer import Framer

class ContentConsumer(object):
    """A class to consume the content"""

    def __init__(self, swarm):
        self._swarm = swarm
        self._loop = asyncio.get_event_loop()
        self._fps = 10
        self._handle = None
        self._framer = Framer(self.__data_framed, av_framer = True)
        self._q = queue.Queue()
        self._biggest_seen_chunk = 0    # Biggest chunk ID ever seen
        self._next_frame = 1            # Next chunk that should go to framer

        # Stats
        self._frames_consumed = 0       # Number of A/V frames shown
        self._frames_missed = 0         # Number of frames that was not there when needed
        self._start_time = 0
        self._first_frame_time = 0
        

    def StartConsuming(self):
        # Here we do not wait for q to fill, but we could if we wanted...
        if self._handle != None:
            raise Exception

        self._start_time = time.time()

        self._handle = self._loop.call_later(1 / self._fps, self.__consume)

    def StopConsuming(self):
        if self._handle == None:
            raise Exception

        self._handle.cancel()
        self._handle = None

        if self._frames_consumed == 0 and self._frames_missed == 0:
            pct_missed = 100
        else:
            pct_missed = (self._frames_missed / (self._frames_consumed + self._frames_missed)) * 100

        pct_showed = 100 - pct_missed
        logging.info("Frames showed {} ({:.2f}%) / Frames missed {} ({:.2f}%)"
                     .format(self._frames_consumed, pct_showed, self._frames_missed, pct_missed))

    def DataReceived(self, chunk_id, data):
        # Track the biggest seen id
        if chunk_id > self._biggest_seen_chunk:
            self._biggest_seen_chunk = chunk_id
        
        # Feed the framer as required
        if chunk_id == self._next_frame:
            self._framer.DataReceived(data)
            self._next_frame += 1

        # If we have a gap - try to fill it
        if self._biggest_seen_chunk > self._next_frame:
            while True:
                chunk = self._swarm._chunk_storage.GetChunkData(self._next_frame, ignore_missing = True)
                if chunk == None:
                    # We do not have next chunk yet
                    break
                else:
                    self._framer.DataReceived(chunk)
                    self._next_frame += 1

    def data_received_with_de(self, chunk_id, chunk_data):
        """Extrack data from chunks with DiscardEligible marks"""

        # Ensure that we got the exact required amount of data
        assert len(chunk_data) == GlobalParams.chunk_size

        # Track the biggest seen id
        if chunk_id > self._biggest_seen_chunk:
            self._biggest_seen_chunk = chunk_id
        
        # Feed the framer as required
        if chunk_id == self._next_frame:
            self._framer.DataReceived(chunk_data[1:])
            self._next_frame += 1

        # If we have a gap - try to fill it
        if self._biggest_seen_chunk > self._next_frame:
            while True:
                chunk = self._swarm._chunk_storage.GetChunkData(self._next_frame, ignore_missing = True)
                if chunk == None:
                    # We do not have next chunk yet
                    break
                else:
                    self._framer.DataReceived(chunk[1:])
                    self._next_frame += 1
        
    def __data_framed(self, data):
        # Called by framer when full A/V frame is ready
        av_data = pickle.loads(data)
        self._q.put(av_data)

    def __consume(self):
        """Consume the next frame as given by callback"""

        try:
            av_data = self._q.get(block = False)

            # Make first frame timestamp if required
            if self._first_frame_time == 0:
                self._first_frame_time = time.time()

            self._frames_consumed += 1
            if self._frames_consumed % 25 == 0:
                logging.info("Got AV data! Seq: {0}; Video size: {1}; Audio size: {2}"
                         .format(av_data['id'], len(av_data['vd']), len(av_data['ad'])))

        except queue.Empty:
            # Do not count missed frames until first is shown
            if self._first_frame_time == 0:
                pass
            else:
                self._frames_missed += 1
            
        # Reschedule the call
        self._handle = self._loop.call_later(1 / self._fps, self.__consume)

    def get_stats(self):
        """Create statistics object"""
        stat = {}
        stat['frames_consumed'] = self._frames_consumed
        stat['frames_missed'] = self._frames_missed
        stat['content_first_frame'] = self._first_frame_time
        stat['content_start_time'] = self._start_time

        return stat