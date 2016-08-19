import logging
import math
import asyncio

from SwarmMember import SwarmMember
from GlobalParams import GlobalParams

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
        self.integrity = {}

        self._file = open(filename, 'bw')
        self._file.seek(0)
        self._file_completed = False

        # Calculate num chunks and make chunk sets
        self.num_chunks = math.ceil(filesize / GlobalParams.chunk_size)
        
        self.set_have = set()
        self.set_missing = set()
        self.set_requested = set()

        for x in range(self.num_chunks):
            self.set_missing.add(x)

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

    def MemberHaveMapUpdated(self):
        """Called when members have map updated to download chunks"""
        loop = asyncio.get_event_loop()
        loop.call_soon(self.RequestChunks)
        logging.info("Scheduled Request Chunks running soon")

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
            self._file.close()
            self._file = open(self.filename, 'br')
            self._file_completed = True
            logging.info("No more missing chunks. Reopening file read-only!")
            
    def RequestChunks(self):
        """Request missing chunks from remote peers"""
        logging.info("Running RequestChunks")

        chunks = set()
        selected_member = None

        for member in self._members:
            # Get chunks missing and not requested in this node
            chunks = member.set_have - self.set_have - self.set_requested

            # Anything?
            if len(chunks) == 0:
                continue
            else:
                selected_member = member

        if selected_member == None:
            # Nothing they have we need
            return
        else:
            selected_member.RequestChunks(chunks)

    def CloseSwarm(self):
        """Close swarm nicely"""
        logging.info("Request to close swarm nicely!")
        # Send departure handshakes
        for member in self._members:
            member.Disconnect()

        # Close FD
        self._file.close()