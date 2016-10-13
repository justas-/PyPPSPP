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