import logging
import asyncio
import os
import socket
import binascii
import sys
import getopt

from PeerProtocol import PeerProtocol
from TrackerClientProtocol import TrackerClientProtocol
from SimpleTracker import SimpleTracker

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logging.info ("PyPPSPP starting")

def main(argv):
    # TODO: Move whatever possible to tracker
    # Leave here just download directory
    #trackerip = '127.0.0.1'      
    trackerip = '10.51.32.121'
    filename = 'test10MB-.bin'
    swarmid = '87a5e6618b2af6f92854eb83e2664d09af7db138'
    filesize = 10485788
    live = True
    live_src = False

    try:
        opts, args = getopt.getopt(argv, "t:f:s:z:l", ["tracker=", "filename=", "swarmid=", "filesize=", "live", "live-src"])
    except getopt.GetoptError:
        print("Error parsing command line arguments")
        sys.exit(1)

    for opt, arg in opts:
        if opt in ("-t", "--tracker"):
            trackerip = arg
        elif opt in ("-f", "--filename"):
            filename = arg
        elif opt in ("-s", "--swarmid"):
            swarmid = arg
        elif opt in ("-z", "--filesize"):
            filesize = int(arg)
        elif opt in ("-l", "--live"):
            live = True
        elif opt in ("--live-src"):
            live_src = True

    logging.info("PPSPP Parameters:\n\tTracker: {0};\n\tFilename: {1};\n\tFilesize: {2}B;\n\tSwarm: {3};\n\tLive: {4};\n\tLive Source: {5};"
                 .format(trackerip, filename, filesize, swarmid, live, live_src))

    # Start minimalistic event loop
    loop = asyncio.get_event_loop()
    loop.set_debug(True)

    tracker = SimpleTracker()

    # Create connection to the tracker node
    # All this code should be hidden in the Tracekr and Swarm manager!
    tracker_listen = loop.create_connection(lambda: TrackerClientProtocol(tracker), trackerip, 6777)
    tracker_transport, traceker_server = loop.run_until_complete(tracker_listen)

    # TODO - check if connected to the Tracekr!
    tracker.SetTrackerProtocol(traceker_server)

    # Create an UDP server
    # Since we are using Tracker, we don't have to run n 6778 port!
    listen = loop.create_datagram_endpoint(PeerProtocol, local_addr=("0.0.0.0", 6778))
    transport, protocol = loop.run_until_complete(listen)

    # At this point we have connection to the tracker and listening UDP socket

    # Start the swarm
    # TODO: This is not the right place to do it...

    # Create the swarm
    protocol.init_swarm(binascii.unhexlify(swarmid), filename, filesize, live, live_src)

    # Inform tracker about swarm ready to receive connections
    tracker.SetSwarm(protocol.swarm)

    # Register with the tracker
    tracker.RegisterWithTracker(swarmid)

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

if __name__ == "__main__":
    main(sys.argv[1:])