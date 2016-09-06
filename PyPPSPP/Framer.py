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

                # Clear the buffer if this is AV framer
                if self._av_framer:
                    self._data_buf.clear()
                else:
                    self._data_buf = self._data_buf[self._data_len:]

                self._data_len = 0
            else:
                # Return waiting for more data
                break
