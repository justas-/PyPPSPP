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
import asyncio
import os
import socket
import argparse
import time
import sys


from PeerProtocolUDP import PeerProtocolUDP
from PeerProtocolTCP import PeerProtocolTCP
from PeerProtocolLedbatUDP import PeerProtocolLedbatUDP
from TrackerClientProtocol import TrackerClientProtocol
from SimpleTracker import SimpleTracker
from Hive import Hive

# Configure logger


def main(args):
    logging.info("""PPSPP Parameters:
        Tracker: {};
        Filename: {};
        Filesize: {}B;
        Swarm: {};
        Live: {};
        Live Source: {};
        Alto: {};
        Num peers: {};
        Identifier: {};
        Skip: {};
        DiscardWnd: {};
        Buffer Sz: {};
        Dl Fwd: {};
        VOD: {};
    """.format(
            args.tracker, 
            args.filename, 
            args.filesize, 
            args.swarmid, 
            args.live, 
            args.livesrc, 
            args.alto, 
            args.numpeers, 
            args.identifier, 
            args.skip,
            args.discardwnd,
            args.buffsz,
            args.dlfwd,
            args.vod
    ))

    #if args.tcp and args.ledbat:
    #    logging.error('Select TCP or LEDBAT for data connections')
    #    return

    if args.vod and args.live:
        logging.error('Client cannot be VOD and LIVE at the same time!')
        return

    if args.alto and 'altocosttype' not in args:
        logging.error('ALTO cost type must be provided if using ALTO')
        return

    if args.alto and 'altoserver' not in args:
        logging.error('ALTO server IP not provided')
        return

    if args.alto:
        logging.info('ALTO Server: %s ALTO cost type: %s',
                     args.altoserver, args.altocosttype)

    if args.workdir is not None:
        logging.info('Changing work directory to: {}'.format(args.workdir))
        os.chdir(args.workdir)

    # Create hive for storing swarms
    hive = Hive()

    # Start minimalistic event loop
    loop = asyncio.get_event_loop()
    loop.set_debug(False)

    tracker = SimpleTracker()
    tracker.set_hive(hive)

    # Create connection to the tracker node
    # All this code should be hidden in the Tracekr and Swarm manager!
    num_retries = 3
    while num_retries > 0:
        try:
            tracker_listen = loop.create_connection(lambda: TrackerClientProtocol(tracker), args.tracker, 6777)
            tracker_transport, traceker_server = loop.run_until_complete(tracker_listen)
            break
        except Exception as exp:
            logging.warn('Exception connecting to tracker: {}'.format(exp))
            time.sleep(1)
            num_retries -= 1

    if num_retries == 0:
        logging.error('Failed to connect to the tracker!')
        return

    # TODO - check if connected to the Tracekr!
    tracker.set_tracker_protocol(traceker_server)

    ip_port = 6778

    if args.tcp:
        # Create a TCP server
        # TODO: This is not great
        listen = loop.create_server(lambda: PeerProtocolTCP(hive), host = "0.0.0.0", port=ip_port)
        protocol = loop.run_until_complete(listen)

    if args.ledbat:
        # Create listening UDP socket for data
        listen = loop.create_datagram_endpoint(lambda: PeerProtocolLedbatUDP(hive), local_addr=("0.0.0.0", ip_port))
        ledbat_transport, ledbat_protocol = loop.run_until_complete(listen)
        logging.info('LEDBAT UDP socket created')

    # At this point we have a connection to the tracker and have a listening (TCP/UDP) socket
    if args.tcp:
        sw = hive.create_swarm(protocol, args)

    if args.ledbat:
        hive.set_ledbat_proto(ledbat_protocol)

    # Register with the tracker
    tracker.register_in_tracker(args.swarmid, ip_port)
    tracker.get_peers(args.swarmid)

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
    if args.tcp:
        hive.close_all_swarms()
        protocol.close()
    else:
        protocol.CloseProtocol()
        transport.close()

    tracker.unregister_from_tracker(args.swarmid)
    loop.close()
    logging.shutdown()

if __name__ == "__main__":

    # LOG TO FILE

    LOG_TO_FILE = False
    output_dir = ""
    if sys.platform == "linux" or sys.platform == "linux2":
        output_dir += "/tmp/"
    elif sys.platform == "win32":
        output_dir += "C:\\test\\"
    else:
        raise BaseException("Unknown platform")

    result_id = socket.gethostname()+'_'+str(int(time.time()))
    
    if LOG_TO_FILE:
        idstr = 'runlog_'+result_id+'.log'
        logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s %(message)s', filename=output_dir+idstr)
    else:
        logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(asctime)s %(message)s')

    logging.info ("PyPPSPP starting")

    # Set the default parameters
    defaults = {}
    defaults['trackerip'] = "10.51.32.121"
    defaults['filename'] = r"C:\PyPPSPP\vod333.dat"
    defaults['swarmid'] = "5600021e740f772f1323ee84a22c222dea6e15ba"
    defaults['filesize'] = 28077056
    defaults['live'] = False
    defaults['live_src'] = False
    defaults['tcp'] = True
    defaults['ledbat'] = False
    defaults['discard_window'] = 1000
    defaults['alto'] = False
    defaults['skip'] = False
    defaults['buffsz'] = 500
    defaults['dlfwd'] = 0
    defaults['vod'] = False

    # Parse command line parameters
    parser = argparse.ArgumentParser(description="Python implementation of PPSPP protocol")
    parser.add_argument("--tracker", help="Tracker IP address", nargs='?', default=defaults['trackerip'])
    parser.add_argument("--filename", help="Filename of the shared file", nargs='?', default=defaults['filename'])
    parser.add_argument("--swarmid", help="Hash value of the swarm", nargs='?', default=defaults['swarmid'])
    parser.add_argument("--filesize", help="Size of the file", nargs='?', type=int, default=defaults['filesize'])
    
    parser.add_argument("--live", help="Is this a live stream", action='store_true', default=defaults['live'])
    parser.add_argument("--livesrc", help="Is this a live stream source", action='store_true', default=defaults['live_src'])
    
    parser.add_argument("--numpeers", help="Limit the number of peers", nargs=1, type=int)
    parser.add_argument("--identifier", help="Free text that will be added to the results file", nargs='?')
    parser.add_argument("--tcp", help="Use TCP between the peers", action='store_true', default=defaults['tcp'])
    parser.add_argument("--ledbat", help="Use LEDBAT for data transmissions", action='store_true', default=defaults['ledbat'])
    parser.add_argument('--discardwnd', help="Live discard window size", nargs='?', default=defaults['discard_window'])
    parser.add_argument('--alto', help="Use ALTO server to rank peers", nargs='?', type=bool, default=defaults['alto'])
    parser.add_argument('--altocosttype', help='ALTO cost type', nargs='?')
    parser.add_argument('--altoserver', help='ALTO server IP', nargs='?')
    parser.add_argument('--workdir', help='Change the working direcotry', nargs='?')
    parser.add_argument('--skip', help='Allow skipping chunks when framer is stuck', action='store_true', default=defaults['skip'])
    parser.add_argument('--buffsz', help='Buffer size (chunks) in Content Consumer', nargs='?', type=int, default=defaults['buffsz'])
    # In VOD scenarios and when data is available, a client might want to request only some 
    # chunks in 'front' of its' playback position. This parameter sets moving window
    # size, where reference point is last chunk fed into the video framer. 0 - No limit
    parser.add_argument('--dlfwd', help='Number of chunks to request after last played', nargs='?', type=int, default=defaults['dlfwd'])
    # Indicate that this is VOD
    parser.add_argument('--vod', help='This is Video-On-Demand CLIENT', action='store_true', default=defaults['vod'])

    # Start the program
    args = parser.parse_args()

    args.result_id = result_id
    args.output_dir = output_dir
    if args.discardwnd == 'None':
        args.discardwnd = None
    
    main(args)
