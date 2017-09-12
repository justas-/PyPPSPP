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

from struct import unpack

class Framer(object):
    """description of class"""

    def __init__(self, callback):
        self._data_len = 0
        self._data_buf = bytearray()
        self._data_callback = callback

    def DataReceived(self, data):
        """Called upon reception of new data from the socket"""
        # Save data to the buffer
        self._data_buf.extend(data)

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
                self._data_buf = self._data_buf[self._data_len:]
                self._data_len = 0
            else:
                # Return waiting for more data
                break
