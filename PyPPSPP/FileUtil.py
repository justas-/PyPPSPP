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

import binascii
import argparse
import time
import sys
import os

from MerkleHashTree import MerkleHashTree
from GlobalParams import GlobalParams

DATA_BLOCK = 1024

def create_file(path, size):
    """Create a file with random contents and a given size"""

    print('Creating file: {} having a size of {} Bytes'.format(path, size))

    t_start = time.time()

    with open(path, mode='wb') as fp:
        
        # Write integer number of blocks
        data_left = size
        for _ in range(size // DATA_BLOCK):
            fp.write(os.urandom(DATA_BLOCK))
            data_left -= DATA_BLOCK

        # Write the remainder (if any)
        if data_left > 0:
            fp.write(os.urandom(data_left))

    print('File created in {} sec.'.format(time.time()-t_start))

def calculate_hash(path):
    """Calculate a hash of a given file"""

    print('Calculating Merkle Tree Hash of file {}'.format(path))
    t_start = time.time()


    mht = MerkleHashTree('sha1', GlobalParams.chunk_size)
    hash = mht.get_file_hash(path)

    if hash is None:
        print('Error calculating file hash!')
        return

    print('Given file hash is: {}. Calculated in {:.2f} s.'.format(
        str(binascii.hexlify(hash)), time.time() - t_start))

def main(args):
    # Basic arguments corectness check
    if args.create and not args.filename and not args.filesize:
        print('Size and Filename are mandatory when creating a file')
        return

    if args.hash and not args.filename:
        print('Filename is mandatory when calculating a hash')
        return

    # Create a file if required
    if args.create:
        create_file(args.filename, args.size)

    # Calculate a hash if required
    if args.hash:
        calculate_hash(args.filename)

def parse_args():
    if len(sys.argv) == 1:
        print('Run program with -h for a list of supported options')
        return

    parser = argparse.ArgumentParser(description='PyPPSPP File utility')
    parser.add_argument('--filename', help='Path to file')
    parser.add_argument('--size', help='File size in Bytes', type=int)
    parser.add_argument('--create', help='Create a file with random data having indicated size', action='store_true')
    parser.add_argument('--hash', help='Calculate hash of a given or created file', action='store_true')

    args = parser.parse_args()
    print(vars(args))
    main(args)

if __name__ == '__main__':
    parse_args()
