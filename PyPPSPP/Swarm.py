import logging
import math
import asyncio
import datetime
import os
import hashlib

from SwarmMember import SwarmMember
from GlobalParams import GlobalParams
from MerkleHashTree import MerkleHashTree

class Swarm(object):
    """A class used to represent a swarm in PPSPP"""

    def __init__(self, socket, swarm_id, filename, filesize):
        """Initialize the object representing a swarm"""
        self.swarm_id = swarm_id
        self.filename = filename
        self.filesize = filesize
        
        self._socket = socket
        self._members = []

        # data
        self._chunk_selction_handle = None
        self._chunk_offer_handle = None

        self.integrity = {} # Not used for now
        self.num_chunks = math.ceil(filesize / GlobalParams.chunk_size)
        self._mht = None
        self._file = None

        self.set_have = set()
        self.set_missing = set()
        self.set_requested = set()

        self._have_ranges = [] # List of ranges of chunks we have verified
        self._last_num_missing = 0
        
        if (os.path.isfile(self.filename)):
            self._mht = MerkleHashTree('sha1', self.filename, GlobalParams.chunk_size)
            if swarm_id == self._mht.root_hash:
                self.InitValidFile()
            else:
                self.InitNewFile()
        else:
            self.InitNewFile()
        
        # Save timestamp when we start operating for stats
        self._ts_start = datetime.datetime.now()
        self._ts_end = None

        # Start operating Chunk offerrer
        self._chunk_offer_handle = asyncio.get_event_loop().call_later(0.5, self.ChunkOffer)

        logging.info("Created Swarm with ID= {0}. Num chunks: {1}"
                     .format(self.swarm_id, self.num_chunks))

    def SendData(self, ip_address, port, data):
        """Send data over a socket used by this swarm"""
        return self._socket.sendto(data, (ip_address, port))

    def AddMember(self, ip_address, port = 6778):
        """Add a member to a swarm and try to initialize connection"""

        logging.info("Swarm: Adding member at {0}:{1}".format(ip_address, port))

        # TODO - Check if already present
        m = SwarmMember(self, ip_address, port)
        self._members.append(m)
        return m

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

        min_chunk = start_chunk - 1
        max_chunk = end_chunk + 1

        while min_chunk >= 0 and min_chunk in self.set_have:
            min_chunk = min_chunk - 1
        min_chunk = min_chunk + 1

        while max_chunk <= self.num_chunks and max_chunk in self.set_have:
            max_chunk = max_chunk + 1
        max_chunk = max_chunk - 1

        return (min_chunk, max_chunk)

    def ChunkOffer(self):
        """Implements chunks sending alg"""

        for member in self._members:
            # Check if member needs anything
            num_requested = len(member.set_requested)
            if num_requested == 0:
                continue
            else:
                member.SendChunks()

        self._chunk_offer_handle = asyncio.get_event_loop().call_later(0.5, self.ChunkOffer)

    def ChunkRequest(self):
        """Implements Chunks selection/request algorith"""
        
        num_missing = len(self.set_missing)
        logging.info("Running chunks selection algorithm. Num missing: {0}".format(num_missing))

        if num_missing == 0:
            logging.info("All chunks onboard. Not rescheduling selection alg")
            return

        # If we are twice at the same fulfillment level - start re-requesting chunks
        if num_missing == self._last_num_missing:
            for member in self._members:
                member.set_requested.clear()
            self.set_requested.clear()

        # TODO: Implement smart algorithm here
        for member in self._members:
            set_i_need = member.set_have - self.set_have - self.set_requested
            if len(set_i_need) > 0:
                member.RequestChunks(set_i_need)
                break

        # Number of chunks missing at the end of chunk selection alg run
        self._last_num_missing = len(self.set_requested)

        # Schedule a call to select chunks again
        asyncio.get_event_loop().call_later(0.5, self.ChunkRequest)

    def BuildHaveRanges(self):
        """Populate have ranges list"""
        self._have_ranges.clear()

        x_min = 0
        x_max = 0
        in_range = False

        while x in range(0, self.num_chunks):
            if x in self.set_have:
                if in_range == False:
                    # Start of new range of chunks we have
                    x_min = x
                    in_range = True
                    continue
                else:
                    # We are in range and we have this chunk
                    x_max = x
                    continue
            else:
                if in_range == False:
                    # We are not in range and don't have this chunk
                    continue
                else:
                    # We are in range and don't have this chunk
                    # We are no longer in range of chunks we have
                    in_range = False

                    # Add to a range of chunks we have
                    self._have_ranges.append((x_min, x_max)) 
                    continue

    def SaveVerifiedData(self, start_chunk, end_chunk, data):
        """Called when we receive data from a peer and validate the integrity"""
        # For now we assume 1024 Byte chunks. This is not always the case
        # as remote peer might be operating using other size chunks

        # Do not overwrite completed files. Ignore duplicate chunks
        if self._file_completed == True:
            return

        self._file.seek(start_chunk * GlobalParams.chunk_size)
        self._file.write(data)
        logging.info("Wrote to file from chunk {0} to chunk {1}".format(start_chunk, end_chunk))

        # Update present / requested / missing chunks
        for x in range(start_chunk, end_chunk+1): 
            self.set_have.add(x)
            self.set_requested.discard(x)
            self.set_missing.discard(x)

        # Close the file once we are done and reopen read-only
        if len(self.set_missing) == 0:
            self._ts_end = datetime.datetime.now()
            elapsed_time = self._ts_end - self._ts_start
            elapsed_seconds = elapsed_time.total_seconds()
            logging.info("Downloaded in {0}s. Speed: {1}Bps".format(elapsed_seconds, self.filesize / elapsed_seconds))

            # Once all downlaoded - stop running the selection alg
            self._chunk_selction_handle.cancel()

            # Reopen in read-only
            self._file.close()
            self._file = open(self.filename, 'br')
            self._file_completed = True
            
            logging.info("No more missing chunks. Reopening file read-only!")

    def InitNewFile(self):
        """There is no file, or file is not full"""
        self._file = open(self.filename, 'bw')
        self._file_completed = False

        for x in range(self.num_chunks):
            self.set_missing.add(x)

        # Schedule a call to chunk selection algorithm
        self._chunk_selction_handle = asyncio.get_event_loop().call_later(0.5, self.ChunkRequest)

        logging.info("Created empty file and started chunk selection")

    def InitValidFile(self):
        """We have the file and it passes validation"""
        self._file = open(self.filename, 'br')
        self._file_completed = True

        # Create set of pieces we have
        for x in range(self.num_chunks):
            self.set_have.add(x)

        # Inform that we have all pieces
        self._have_ranges.append((0, self.num_chunks))

        logging.info("File integrity valid. Seeding the file!")

    def CloseSwarm(self):
        """Close swarm nicely"""
        logging.info("Request to close swarm nicely!")
        # Send departure handshakes
        for member in self._members:
            member.Disconnect()

        # Close FD
        self._file.close()