import logging
import asyncio

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

try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

# Clean-up
transport.close()
loop.close()