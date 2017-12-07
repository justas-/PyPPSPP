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

import logging

from struct import unpack

class Framer(object):
    """Framer to convert stream of data to packets.
       AV Framer is special in a way that after a frame is returned, 
       all remaining data is discarded. This is done because AV data
       is allways padded by zeros until the end of the last packet.
    """

    def __init__(self, callback, av_framer = False):
        self._data_len = 0
        self._data_buf = bytearray()
        self._data_callback = callback
        self._av_framer = av_framer

        self._reset_on_data = False
        self._range_start = None
        self._range_end = None

    def DataReceived(self, data, chunk_id = None):
        """Called upon reception of new data from the socket"""
        # Save data to the buffer
        self._data_buf.extend(data)

        # Special parts for AV framers
        if self._av_framer:
            # Reset if this is new range
            if self._reset_on_data:
                self._range_start = None
                self._range_end = None
                self._reset_on_data = False

            # Track chunks
            if self._range_start is None:
                self._range_start = chunk_id
                self._range_end = chunk_id
            else:
                self._range_end = chunk_id

        while True:
            # If we don't have packet length:
            if self._data_len == 0:
                if len(self._data_buf) >= 4:
                    # But have enough data - extract it
                    self._data_len = unpack('>I', self._data_buf[0:4])[0]
                    self._data_buf = self._data_buf[4:]
                else:
                    # Don't have nough data - return
                    break

            # If we have enough data
            if self._data_len > 0 and len(self._data_buf) >= self._data_len:
                # Build the package and continue
                self._data_callback(self._data_buf[0:self._data_len])

                # Reset chunks counter
                self._reset_on_data = True

                # Clear the buffer if this is AV framer
                if self._av_framer:
                    self._data_buf.clear()
                else:
                    self._data_buf = self._data_buf[self._data_len:]

                self._data_len = 0
            else:
                # Return waiting for more data
                break

    def get_deframed_chunks_range(self):
        """Get a range of chunks that were used to deframe latest data"""

        # If anything is None - return fail
        if self._range_start is None or self._range_end is None:
            return None
        else:
            return (self._range_start, self._range_end)
