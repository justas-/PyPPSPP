import os
import hashlib
import math

class MerkleHashTree(object):
    """Helper class for dealing with Merkle Hash Tree"""

    def __init__(self, hash_funct, file_name, chunk_len):
        """Initialize Merkle Hash Tree using given hash function and file"""

        self._hash_func = hash_funct
        self._chunk_len = chunk_len

        self._file_handle = open(file_name, 'rb')
        self._file_len = os.stat(file_name).st_size
        
        # Number of hashes from the file
        self._tree_populated_width = math.ceil(self._file_len / chunk_len)
        # Height of the tree
        self._tree_height = math.ceil(math.log2(self._tree_populated_width))
        # Total number of leaf nodes for balanced tree
        self._tree_all_width = int(math.pow(2,self._tree_height))

        # Fill bottom layer
        self._bottom_layer = ['\x00'] * self._tree_all_width
        self.FillBotomLayer()

        # Get Root Hash
        self.root_hash = None
        self.CalculateRootHash()

        # Close the file handle
        self._file_handle.close()
        
    def FillBotomLayer(self):
        for x in range(0, self._tree_populated_width):
            # Read file and calculate hash
            data = self._file_handle.read(self._chunk_len)
            hasher = hashlib.new(self._hash_func)
            hasher.update(data)
            self._bottom_layer[x] = hasher.digest()


    def CalculateRootHash(self):
        working_list = []

        for x in [z for z in range(0, self._tree_all_width) if z % 2 == 0]:
            if self._bottom_layer[x] != '\x00' and self._bottom_layer[x+1] != '\x00':
                # Both children are normal hashes
                hasher = hashlib.new(self._hash_func)
                hasher.update(self._bottom_layer[x] + self._bottom_layer[x+1])
                working_list.append(hasher.digest())
            elif self._bottom_layer[x] != '\x00' and self._bottom_layer[x+1] == '\x00':
                # Second children is null hash
                hasher = hashlib.new(self._hash_func)
                hasher.update(self._bottom_layer[x] + hasher.digest_size * bytes([0]))
                working_list.append(hasher.digest())
            else:
                # Both children are null hashes
                working_list.append('\x00')

        while len(working_list) != 1:
            inner_list = working_list[:]
            working_list.clear()
            
            for x in [z for z in range(0, len(inner_list)) if z % 2 == 0]:
                if inner_list[x] != '\x00' and inner_list[x+1] != '\x00':
                    # Both children are normal hashes
                    hasher = hashlib.new(self._hash_func)
                    hasher.update(inner_list[x] + inner_list[x+1])
                    working_list.append(hasher.digest())
                elif inner_list[x] != '\x00' and inner_list[x+1] == '\x00':
                    # Second children is null hash
                    hasher = hashlib.new(self._hash_func)
                    hasher.update(inner_list[x] + hasher.digest_size * bytes([0]))
                    working_list.append(hasher.digest())
                else:
                    # Both children are null hashes
                    working_list.append('\x00')
            
        self.root_hash = working_list[0]
