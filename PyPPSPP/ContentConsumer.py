import asyncio
import logging
import pickle
import queue
import time
import threading

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
        self._consume_thread = threading.Thread(target=self.thread_entry, name='cont_consume')
        self._stop_thread = False
        self._consumer_locked = False   # Is the consumer locked to the right position in the datastream
        self._allow_tune_in = False     # Allow the client to tune-in (see method for explanation)

        self._frames_consumed = 0       # Number of A/V frames shown
        self._frames_missed = 0         # Number of frames that was not there when needed
        self._start_time = 0            # When consumer started
        self._first_frame_time = 0      # When the first frame was consumed
        self._stop_time = 0             # When consumer stopped
        self._consume_run = 0           # Number of time consume event ran
        self._buffer_start = 0        # Time when buffering stops and consuming start

        
    def thread_entry(self):
        """Entry point for consumption happening in the thread"""

        try:
            # Keep beffering for some time
            self._buffer_start = time.time()
            while self._q.qsize() < 50:
                time.sleep(0.25) 

            # Set the start time
            self._start_time = time.time()
            
            # Run until requested to stop
            while not self._stop_thread:
                self.__consume()
                time.sleep(1 / self._fps)
            
            # When stopped - indicate stop time
            self._stop_time = time.time()
            return

        except KeyboardInterrupt:
            logging.warn('Except in consume thread!')
            pass

    def allow_tune_in(self):
        """Allow the client to tune-in into the live broadcast.
           This way the client will start recreating frames from the
           first place it can identify start of a valid frame, instead
           of waiting for the first frame in the live stream.
        """
        if self._consume_thread.is_alive():
            logging.error('Canot enable tune in in the already running content consumer!')
            return
        
        self._allow_tune_in = True 

    def start_consuming(self):
        # Here we do not wait for q to fill, but we could if we wanted...
        if self._consume_thread.is_alive():
            logging.error('Content consuming thread is already alive!')
            return

        self._consume_thread.start()

    def stop_consuming(self):
        if not self._consume_thread.is_alive():
            logging.error('Content consuming thread is already stopped!')
            return

        self._stop_thread = True
        self._consume_thread.join()

        self.print_statistics()

    def print_statistics(self):
        if self._frames_consumed == 0 and self._frames_missed == 0:
            pct_missed = 100
        else:
            pct_missed = (self._frames_missed / (self._frames_consumed + self._frames_missed)) * 100

        pct_showed = 100 - pct_missed
        logging.info("Frames showed {} ({:.2f}%) / Frames missed {} ({:.2f}%); Runtime: {}; Consume runs: {}; First frame in: {:.2f}s"
                     .format(
                        self._frames_consumed, 
                        pct_showed, 
                        self._frames_missed, 
                        pct_missed, 
                        int(self._stop_time - self._first_frame_time),
                        self._consume_run,
                        self._first_frame_time - self._start_time))

    def data_received(self, chunk_id, data):
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

        # Extra handling of tune-in:
        if self._allow_tune_in and not self._consumer_locked:
            
            # Determine the DE status and position in the data stream
            if chunk_data[0] == 0 and chunk_id >= self._next_frame:

                # Skip all frames before this frame if we are in the future
                if chunk_id != self._next_frame:
                    self._next_frame = chunk_id

                self._consumer_locked = True
                logging.info('Locked content consumer on chunk id: {}'.format(chunk_id))
            else:
                # Discard the frame
                self._next_frame = chunk_id + 1
                return

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

        # Update stats
        self._consume_run += 1

        # Try to get data from the Queue
        try:
            av_data = self._q.get(block = False)

            # Mark first frame time
            if self._first_frame_time == 0:
                self._first_frame_time = time.time()

            self._frames_consumed += 1
            if self._frames_consumed % 50 == 0:
                logging.info("Got AV data! Seq: {}; Video size: {}; Audio size: {}; Valid: {}"
                         .format(av_data['id'], len(av_data['vd']), len(av_data['ad']), av_data['in']))

        except queue.Empty:
            # Do not count missed frames until the first frame is shown
            if self._first_frame_time != 0:
                self._frames_missed += 1
                if self._frames_missed % 10 == 0:
                    logging.warn('Framer stuck on: {}'.format(self._next_frame))

    def get_stats(self):
        """Create statistics object"""

        stat = {}
        stat['frames_consumed'] = self._frames_consumed
        stat['frames_missed'] = self._frames_missed
        stat['content_first_frame'] = self._first_frame_time
        stat['content_start_time'] = self._start_time
        stat['content_stop_time'] = self._stop_time
        stat['consume_runs'] = self._consume_run
        stat['buffer_start'] = self._buffer_start

        return stat
