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

"""
Helper program to build VOD file
"""

import logging
import asyncio
import binascii
import os

from MerkleHashTree import MerkleHashTree
from GlobalParams import GlobalParams
from MemoryChunkStorage import MemoryChunkStorage
from ContentGenerator import ContentGenerator


logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s %(message)s')

class FakeSwarm(object):
    """Fake Swarm object to be inserted instead of real swarm"""
    
    def __init__(self):
        """Build required parameters"""
        self.discard_wnd = None
        self.set_have = set()
        self.live = True
        self.live_src = True
        self._have_ranges = []
        self._last_discarded_id = -1

    def SendHaveToMembers(self):
        """Do nothing"""
        pass

def main(length, filename):
    """Generate file having length number of seconds and save to filename"""

    logging.info('Building VOD file. Length: %s s. Filename: %s', length, filename)

    swarm = FakeSwarm()
    storage = MemoryChunkStorage(swarm)
    generator = ContentGenerator()
    mekle_hasher = MerkleHashTree('sha1', GlobalParams.chunk_size)

    fps = 10
    key = 0
    total_frames = length * fps
    for _ in range(total_frames):
        # Generate AV data
        if key == min([
            len(generator._audio_samples),
            len(generator._video_samples)
        ]):
            key = 0

        avdata = generator._get_next_avdata(key)
        key += 1

        # Feed it into storage
        storage.pack_data_with_de(avdata)

    storage_len = len(storage._chunks)
    logging.info('Total frames: %s Total chunks: %s',
                 total_frames, storage_len
    )

    m = bytearray()
    for cid in range(storage_len):
        m.extend(storage.GetChunkData(cid))
    mmh = mekle_hasher.get_data_hash(m)
    logging.info('In Memory Merkle Root hash: %s', binascii.hexlify(mmh))

    # Storage now has all data. Store it into file
    num_writes = 0
    with open(filename, 'wb') as file_hdl:
        for chunk_id in range(storage_len):
            data = storage.GetChunkData(chunk_id)
            file_hdl.write(data)
            num_writes += 1
            if num_writes % 1000 == 0:
                logging.info('Wrote chunk %s of %s', num_writes, storage_len)

    # Calculate Merkle Root Hash:
    logging.info('Calculating Merkle root hash')

    mrh = mekle_hasher.get_file_hash(filename)

    logging.info('Merkle Root hash: %s', binascii.hexlify(mrh))
    logging.info('Min %s, Max %s', min(swarm.set_have), max(swarm.set_have))

    with open('{}.log'.format(filename), 'w') as log_hdl:
        log_hdl.write('Filename: {}\n'.format(filename))
        log_hdl.write('Total frames: {}\n'.format(total_frames))
        log_hdl.write('Total chunks: {}\n'.format(storage_len))
        log_hdl.write('Merkle hash: {}\n'.format(binascii.hexlify(mrh)))

if __name__ == '__main__':
    main(333, 'vod333.dat')