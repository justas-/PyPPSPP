import logging
import os
import math
import time

from MerkleHashTree import MerkleHashTree
from AbstractChunkStorage import AbstractChunkStorage
from GlobalParams import GlobalParams

class FileChunkStorage(AbstractChunkStorage):
    """File based chunk storage"""

    def __init__(self, swarm):
        super().__init__(swarm)

        self._mht = None
        self._file = None
        self._file_completed = False
        self._file_size = 0
        self._file_name = None

        self._ts_start = 0
        self._ts_end = 0
        self._start_source = False

        self._num_chunks = 0

    def Initialize(self, filename = None, filesize = 0):
        self._num_chunks = math.ceil(filesize / GlobalParams.chunk_size)
        
        self._file_name = filename
        self._file_size = filesize

        if (os.path.isfile(filename)):
            logging.info("File found. Checking integrity")
            self._mht = MerkleHashTree('sha1', filename, GlobalParams.chunk_size)
            if self._swarm.swarm_id == self._mht.root_hash:
                logging.info("File integrity checking passed. Starting to share the file")
                self.InitValidFile()
            else:
                logging.info("File integrity checking failed. Calculated hash: {}. Recreating file!".format(self._mht.root_hash))
                self.InitNewFile()
        else:
            logging.info("No file found. Creating an empty file")
            self.InitNewFile()

        # Save timestamp when we start operating for stats
        self._ts_start = time.time()
        self._ts_end = None

    def CloseStorage(self):
        """Close file handle"""
        self._file.close()

    def GetChunkData(self, chunk):
        """Get required chunk from file"""
        self._file.seek(chunk * GlobalParams.chunk_size)
        return self._file.read(GlobalParams.chunk_size)

    def PostComplete(self):
        """Post complete actions for file storage"""
        self._ts_end = time.time()
        elapsed_time = self._ts_end - self._ts_start
        elapsed_seconds = int(elapsed_time)
        logging.info("Downloaded in {0}s. Speed: {1}Bps".format(elapsed_seconds, self._file_size / elapsed_seconds))

        # Once all downlaoded - stop running the selection alg
        self._swarm.StopChunkRequesting()

        # Remove peers limit
        self._swarm._max_peers = None

        # Reopen in read-only
        self._file.close()
        self._file = open(self._file_name, 'br')
        self._file_completed = True
            
        logging.info("No more missing chunks. Reopening file read-only!")
        self.BuildHaveRanges()
        self._swarm.SendHaveToMembers()
        self._swarm.ReportData()

    def SaveChunkData(self, chunk_id, data):
        """Save indicated chunk to file"""
        if self._file_completed == True:
            return

        self._file.seek(chunk_id * GlobalParams.chunk_size)
        self._file.write(data)
        #logging.info("Wrote chunk {0} to file".format(chunk_id))        

    def InitValidFile(self):
        """We have the file and it passes validation"""
        self._file = open(self._file_name, 'br')
        self._file_completed = True
        self._start_source = True

        # Create set of pieces we have
        for x in range(self._num_chunks):
            self._swarm.set_have.add(x)

        # Build have ranges
        self.BuildHaveRanges()

        logging.info("File integrity valid. Seeding the file!")

    def InitNewFile(self):
        """There is no file, or file is not full"""
        self._file = open(self._file_name, 'bw')
        self._file_completed = False

        for x in range(self._num_chunks):
            self._swarm.set_missing.add(x)

        # Schedule a call to chunk selection algorithm
        self._swarm.StartChunkRequesting()

        logging.info("Created empty file and started chunk selection")

    def BuildHaveRanges(self):
        """Populate have ranges list"""
        self._swarm._have_ranges.clear()

        x_min = 0
        x_max = 0
        in_range = False

        for x in range(0, self._num_chunks+1):
            if x in self._swarm.set_have:
                if in_range == False:
                    # Start of new range of chunks we have
                    x_min = x
                    in_range = True
                else:
                    # We are in range and we have this chunk
                    x_max = x
            else:
                if in_range == False:
                    # We are not in range and don't have this chunk
                    continue
                else:
                    # We are in range and don't have this chunk
                    # We are no longer in range of chunks we have
                    in_range = False

                    # Add to a range of chunks we have
                    self._swarm._have_ranges.append((x_min, x_max))