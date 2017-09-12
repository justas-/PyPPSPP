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

class MsgTypes:
    PPSPPMsgTypes = {
        0 : "HANDSHAKE",
        1 : "DATA",
        2 : "ACK",
        3 : "HAVE",
        4 : "INTEGRITY",
        5 : "PEX_RESv4",
        6 : "PEX_REQ",
        7 : "SIGNED_INTEGRITY",
        8 : "REQUEST",
        9 : "CANCEL",
        10 : "CHOKE",
        11 : "UNCHOKE",
        12 : "PEX_RESv6",
        13 : "PEX_REScert"
    }
    HANDSHAKE = 0
    DATA = 1
    ACK = 2
    HAVE = 3
    INTEGRITY = 4
    PEX_RESv4 = 5
    PEX_REQ = 6
    SIGNED_INTEGRITY = 7
    REQUEST = 8
    CANCEL = 9
    CHOKE = 10
    UNCHOKE = 11
    PEX_RESv6 = 12
    PEX_REScert = 13