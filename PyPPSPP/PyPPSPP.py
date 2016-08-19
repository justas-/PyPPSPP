import logging
import asyncio
import os

from PeerProtocol import PeerProtocol

# Configure logger
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
logging.info ("PyPPSPP starting")

# Start minimalistic event loop
loop = asyncio.get_event_loop()
loop.set_debug(True)

# Create an UDP server
listen = loop.create_datagram_endpoint(
    PeerProtocol, local_addr=("0.0.0.0", 6778))

# Run until completed
transport, protocol = loop.run_until_complete(listen)

# Schedule wakeups to catch Ctrl+C in Win32
# This should be fixed in Python 3.5 
# Ref: http://stackoverflow.com/questions/24774980/why-cant-i-catch-sigint-when-asyncio-event-loop-is-running
if os.name == 'nt':
    def wakeup():
        # Call again later
        loop.call_later(0.5, wakeup)
    loop.call_later(0.5, wakeup)

try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

# Clean-up
transport.close()
loop.close()