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

class AbstractChunkStorage():
    """Abstract class for for concrete chunk storage implementors"""

    def __init__(self, swarm):
        self._swarm = swarm

    def Initialize(self):
        """Initialize the storage engine"""
        pass

    def CloseStorage(self):
        """Close the storage and release any held resources"""
        pass

    def GetChunkData(self, chunk):
        """Get indicated chunk"""
        pass

    def SaveChunkData(self, chunk_id, data):
        """Save given chunk in storage"""
        pass

    def PostComplete(self):
        """Called when all chunks are onboard"""
        pass