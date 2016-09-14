import asyncio
import logging
import pickle
import queue

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
        self._frames_consumed = 0       # Number of A/V frames shown
        self._next_frame = 1            # Next chunk that should go to framer
        self._biggest_seen_chunk = 0    # Biggest chunk ID ever seen

    def StartConsuming(self):
        # Here we do not wait for q to fill, but we could if we wanted...
        if self._handle != None:
            raise Exception

        self._handle = self._loop.call_later(1 / self._fps, self.__consume)

    def StopConsuming(self):
        if self._handle == None:
            raise Exception

        self._handle.cancel()
        self._handle = None

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
                chunk = self._swarm._chunk_storage.GetChunkData(self._next_frame)
                if chunk == None:
                    # We do not have next chunk yet
                    break
                else:
                    self._framer.DataReceived(chunk)
                    self._next_frame += 1
        
    def __data_framed(self, data):
        # Called by framer when full A/V frame is ready
        av_data = pickle.loads(data)
        self._q.put(av_data)

    def __consume(self):
        """Consume the next frame as given by callback"""
        av_data = None
        try:
            av_data = self._q.get(block = False)
            self._frames_consumed += 1
            if self._frames_consumed % 25 == 0:
                logging.info("Got AV data! Seq: {0}; Video size: {1}; Audio size: {2}"
                         .format(av_data['id'], len(av_data['vd']), len(av_data['ad'])))

        except queue.Empty:
            # Some day log missing read...
            pass
            
        # Reschedule the call
        self._handle = self._loop.call_later(1 / self._fps, self.__consume)