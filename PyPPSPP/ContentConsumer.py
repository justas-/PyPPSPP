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

    def __init__(self, swarm, args):
        self._swarm = swarm
        self._loop = asyncio.get_event_loop()
        self._fps = 10
        self._handle = None
        self._framer = Framer(self.__data_framed, av_framer = True)
        self._q = queue.Queue()
        self._biggest_seen_chunk = 0    # Biggest chunk ID ever seen
        self._next_frame = 0            # Next chunk that should go to framer
        self._consume_thread = threading.Thread(target=self.thread_entry, name='cont_consume')
        self._stop_thread = False
        self._consumer_locked = False   # Is the consumer locked to the right position in the datastream
        self._allow_tune_in = False     # Allow the client to tune-in (see method for explanation)
        self._data_lock = threading.Lock()
        self._video_buffer_sz = args.buffsz     # How many chunks to download before starting playback

        self._last_showed = None        # Last chunk ID that was used to recreate showed frame
        self._last_showed_lock = threading.Lock()   # Lock to access the last showed id
        self._content_len = None

        self._frames_consumed = 0       # Number of A/V frames shown
        self._frames_missed = 0         # Number of frames that was not there when needed
        self._start_time = 0            # When consumer started
        self._first_frame_time = 0      # When the first frame was consumed
        self._stop_time = 0             # When consumer stopped
        self._consume_run = 0           # Number of time consume event ran
        self._buffer_start = 0          # Time when buffering stops and consuming start
        self._num_skipped = 0           # Number of data chunks skipped
        self._chunk_at_start = 0        # Chunk number when playback starts


    def last_showed_chunk(self):
        """Return ID of the last consumed chunk ID"""

        with self._last_showed_lock:
            return self._last_showed

    def playback_started(self):
        """Return True if buffering is over and playback has started"""
        return bool(self._start_time != 0)

    def thread_entry(self):
        """Entry point for consumption happening in the thread"""

        try:
            # Keep beffering for some time
            self._buffer_start = time.time()
            while self._q.qsize() < self._video_buffer_sz and not self._stop_thread:
                logging.info('Buffering. Q: %s', self._q.qsize())
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
            return

        self._consume_thread.start()

    def stop_consuming(self):
        if not self._consume_thread.is_alive():
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
                        self._first_frame_time - self._buffer_start))

    def data_received(self, chunk_id, data):
        # Track the biggest seen id
        if chunk_id >= self._biggest_seen_chunk:
            self._biggest_seen_chunk = chunk_id
        
        # Feed the framer as required
        if chunk_id == self._next_frame:
            self._framer.DataReceived(data, chunk_id)
            self._next_frame += 1

        # If we have a gap - try to fill it
        if self._biggest_seen_chunk > self._next_frame:
            while True:
                chunk = self._swarm._chunk_storage.GetChunkData(self._next_frame, ignore_missing = True)
                if chunk == None:
                    # We do not have next chunk yet
                    break
                else:
                    self._framer.DataReceived(chunk, chunk_id)
                    self._next_frame += 1

    def data_received_with_de(self, chunk_id, chunk_data):
        """Extrack data from chunks with DiscardEligible marks"""

        # Ensure that we got the exact required amount of data
        assert len(chunk_data) == GlobalParams.chunk_size

        # Get the lock
        with self._data_lock:

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
            if chunk_id >= self._biggest_seen_chunk:
                self._biggest_seen_chunk = chunk_id

            # Feed the framer as required
            if chunk_id == self._next_frame:
                self._framer.DataReceived(chunk_data[1:], chunk_id)
                self._next_frame += 1

            # If we have a gap - try to fill it
            if self._biggest_seen_chunk > self._next_frame:
                self.feed_q_until_max()

    def __data_framed(self, data):
        """Callback from Framer/Deframer"""

        # Get the chunks rnage that was used to create given data
        chunks_range = self._framer.get_deframed_chunks_range()

        if chunks_range is None:
            logging.error('Got None when requesting chunks range from the framer!')

        # Called by framer when full A/V frame is ready
        av_data = pickle.loads(data)
        self._q.put((av_data, chunks_range))

    def __consume(self):
        """Consume the next frame as given by callback"""

        # Update stats
        self._consume_run += 1

        # Try to get data from the Queue
        try:
            (av_data, chunks_range) = self._q.get(block = False)

            # Take note of the chunks range used to build this frame
            if chunks_range is None:
                chunk_start = None
                chunk_end = None
            else:
                (chunk_start, chunk_end) = chunks_range

            # Update the value accessible for the other threads
            with self._last_showed_lock:
                self._last_showed = chunk_end

            # Mark first frame time
            if self._first_frame_time == 0:
                self._first_frame_time = time.time()

            self._frames_consumed += 1
            if self._frames_consumed % 50 == 0:
                logging.info("Got AV data! Seq: {}; Video size: {}; Audio size: {}; Valid: {}; Chunks: {}:{};"
                             .format(av_data['id'], len(av_data['vd']), len(av_data['ad']), av_data['in'], 
                                chunk_start, chunk_end))

            if self._frames_consumed == self._video_buffer_sz:
                self._chunk_at_start = chunk_end

            if self._content_len is not None and self._content_len == self._frames_consumed:
                logging.info('End of content reached!')
                self._stop_thread = True
                

        except queue.Empty:
            # Do not count missed frames until the first frame is shown
            if self._first_frame_time != 0:
                self._frames_missed += 1
                if self._frames_missed % 10 == 0:
                    logging.warn('Framer stuck on: {}'.format(self._next_frame))
                    if self._swarm._args.skip is True:
                        self._skip_frames()

    def feed_q_until_max(self):
        """Try feeding the frames Q until the last known chunk"""
        while True:
            chunk = self._swarm._chunk_storage.GetChunkData(self._next_frame, ignore_missing = True)
            if chunk == None:
                # We do not have next chunk yet
                break
            else:
                self._framer.DataReceived(chunk[1:], self._next_frame)
                self._next_frame += 1

    def _skip_frames(self):
        """Skip number of frames forward to prevent content consumer being stuck."""

        # Get the lock so we are not fed anything
        self._data_lock.acquire()

        skipped = False


        # Skip initial chunks
        nf_old = self._next_frame
        nf = self._next_frame + 250

        # Keep running until the end
        while nf <= self._biggest_seen_chunk:

            # Get the chunk
            chunk = self._swarm._chunk_storage.GetChunkData(nf, ignore_missing = True)

            # If Chunk is missing continue with next
            if chunk is None:
                nf += 1
                continue
            else:
                # we have chunk, check DE
                if chunk[0] == 0:
                    # non DE frame found
                    skipped = True
                    break
                else:
                    # This chunk is Discard eligible - continue
                    nf += 1
                    continue

        # If skip was sucessful:
        if skipped:
            # Print results:
            logging.info('Chunk skip successful. Cur nf: {}; New nf: {}, Biggest seen: {};'
                         .format(self._next_frame, nf, self._biggest_seen_chunk))

            # Log stats
            self._num_skipped += (nf - nf_old)

            # Clear the q and framer
            self._q = queue.Queue()
            self._framer = Framer(self.__data_framed, av_framer = True)

            # Fast forward to new next chunk
            self._next_frame = nf
            # Feed the queue as much as possible
            self.feed_q_until_max()
        else:
            logging.info('Chunk skip failed! Nf: {}, Biggest seen: {}'
                         .format(self._next_frame, self._biggest_seen_chunk))

        # Release the lock
        self._data_lock.release()

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
        stat['chunks_skipped'] = self._num_skipped
        stat['chunk_at_start'] = self._chunk_at_start

        return stat
