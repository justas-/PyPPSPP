import logging
import asyncio
import os
import socket
import binascii

from PeerProtocol import PeerProtocol
from TrackerClientProtocol import TrackerClientProtocol
from SimpleTracker import SimpleTracker

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logging.info ("PyPPSPP starting")

# Start minimalistic event loop
loop = asyncio.get_event_loop()
loop.set_debug(True)

tracker = SimpleTracker()

# Create connection to the tracker node
# All this code should be hidden in the Tracekr and Swarm manager!
tracker_listen = loop.create_connection(lambda: TrackerClientProtocol(tracker), '127.0.0.1', 6777)
tracker_transport, traceker_server = loop.run_until_complete(tracker_listen)

# TODO - check if connected to the Tracekr!
tracker.SetTrackerProtocol(traceker_server)

# Create an UDP server
# Since we are using Tracker, we don't have to run on 6778 port!
listen = loop.create_datagram_endpoint(PeerProtocol, local_addr=("0.0.0.0", 6778))
transport, protocol = loop.run_until_complete(listen)

# At this point we have connection to the tracker and listening UDP socket

# Start the swarm
# TODO: This is not the right place to do it...

#swarm_id = binascii.unhexlify("82d3614b17dcac7624e58b2bee9bca1580a87b75")
#swarm_filename = "C:\PyPPSPP\HelloWorld.txt"
#swarm_file_size = 33

swarm_id = binascii.unhexlify("87a5e6618b2af6f92854eb83e2664d09af7db138")
swarm_filename = r"C:\PyPPSPP\test10MB.bin"
swarm_file_size = 10485788

#swarm_id = binascii.unhexlify("87323eba55d370afcbfdf40ee4fd2b248bb98afb")
#swarm_filename = r"C:\PyPPSPP\test1MB.bin"
#swarm_file_size = 1048536

# Create the swarm
protocol.init_swarm(swarm_id, swarm_filename, swarm_file_size)

# Inform tracker about swarm ready to receive connections
tracker.SetSwarm(protocol.swarm)

# Register with the tracker
tracker.RegisterWithTracker(swarm_id)

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

# Inform protocol to close
protocol.CloseProtocol()

tracker.UnregisterWithTracker()

# Clean-up
transport.close()
loop.close()