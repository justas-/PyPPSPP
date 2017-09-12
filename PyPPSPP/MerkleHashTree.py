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
Implement Merkle Hash Tree calculation functions.
"""
import os
import hashlib
import math
import io
import logging

class MerkleHashTree(object):
    """Helper class for dealing with Merkle Hash Tree"""
    # TODO: Peak Hashes

    def __init__(self, hash_funct, chunk_len):
        """Initialize Merkle Hash Tree using given hash function and file"""

        self._hash_func = hash_funct
        self._chunk_len = chunk_len

    def get_file_hash(self, filename):
        """Get Merkle hash of a given file"""

        try:
            file_len = os.stat(filename).st_size
        except OSError as exc:
            logging.error('Opening file: %s raised exception: %s', filename, exc)
            return None

        if file_len == 0:
            logging.warning('Given file %s is empty!', filename)
            return None

        # Calculate tree parameters
        tree_populated_width = math.ceil(file_len / self._chunk_len)
        tree_height = math.ceil(math.log2(tree_populated_width))
        tree_width = int(math.pow(2, tree_height))

        # Fill bottom layer with file's hashes
        tree_bottom_layer = ['\x00'] * tree_width
        with open(filename, 'rb') as file_hdl:
            self._initial_hasher(
                file_hdl,
                tree_populated_width,
                tree_bottom_layer
            )

        # Get Merkle's root hash
        mrh = self._calculate_root_hash(tree_bottom_layer)
        return mrh

    def get_data_hash(self, data_bytes):
        """Calculate Merkle's root hash of the given data bytes"""

        # Calculate tree parameters
        data_len = len(data_bytes)
        tree_populated_width = math.ceil(data_len / self._chunk_len)
        tree_height = math.ceil(math.log2(tree_populated_width))
        tree_width = int(math.pow(2, tree_height))

        tree_bottom_layer = ['\x00'] * tree_width
        with io.BytesIO(data_bytes) as b_data:
            self._initial_hasher(
                b_data,
                tree_populated_width,
                tree_bottom_layer
            )

        # Get Merkle's root hash
        mrh = self._calculate_root_hash(tree_bottom_layer)
        return mrh

    def _initial_hasher(self, stream, num_chunks, buffer):
        """Fill initial hash values"""
        for chunkid in range(num_chunks):
            data = stream.read(self._chunk_len)
            hasher = hashlib.new(self._hash_func)
            hasher.update(data)
            buffer[chunkid] = hasher.digest()

    def _calculate_root_hash(self, data):
        """Calculate Merkle's root hash of the given data"""
        working_list = []
        data_len = len(data) # Length of tree's bottom layer

        for x in range(0, data_len, 2):
            if data[x] != '\x00' and data[x+1] != '\x00':
                # Both children are normal hashes
                hasher = hashlib.new(self._hash_func)
                hasher.update(data[x] + data[x+1])
                working_list.append(hasher.digest())
            elif data[x] != '\x00' and data[x+1] == '\x00':
                # Second child is null hash
                hasher = hashlib.new(self._hash_func)
                hasher.update(data[x] + hasher.digest_size * bytes([0]))
                working_list.append(hasher.digest())
            else:
                # Both children are null hashes
                working_list.append('\x00')

        while len(working_list) != 1:
            # Eache iteration reduces list in half, until only one hash is left
            inner_list = working_list[:]
            working_list.clear()
            inner_len = len(inner_list)

            for x in range(0, inner_len, 2):
                if inner_list[x] != '\x00' and inner_list[x+1] != '\x00':
                    # Both children are normal hashes
                    hasher = hashlib.new(self._hash_func)
                    hasher.update(inner_list[x] + inner_list[x+1])
                    working_list.append(hasher.digest())
                elif inner_list[x] != '\x00' and inner_list[x+1] == '\x00':
                    # Second child is null hash
                    hasher = hashlib.new(self._hash_func)
                    hasher.update(inner_list[x] + hasher.digest_size * bytes([0]))
                    working_list.append(hasher.digest())
                else:
                    # Both children are null hashes
                    working_list.append('\x00')

        return working_list[0]
