import asyncio
import logging

class ContentConsumer(object):
    """A class to consume the content"""

    def __init__(self, loop, get_next):
        self._loop = loop
        self._get_next = get_next
        self._fps = 10
        self._handle = None

    def StartConsuming(self):
        if self._handle != None:
            raise Exception

        self._handle = self._loop.call_later(1 / self._fps, self._Consume)

    def StopConsuming(self):
        if self._handle == None:
            raise Exception

        self._handle.cancel()
        self._handle = None

    def _Consume(self):
        """Consume next frame as given by callback"""

        # Count up to 2 frames per go
        c = 0
        while c < 2:
            f = self._get_next()

            # No frames available - Warn
            if f == None:
                logging.warn("Failed to get next frame")
                break

            # If frame is available - print what kind of frame
            if f['t'] == 'v':
                logging.info("Consumed video frame")
            elif f['t'] == 'a':
                logging.info("Consumed audio frame")

            # Advance the counter
            c += 1
        
        # Reschedule the call
        self._handle = self._loop.call_later(1 / self._fps, self._Consume)