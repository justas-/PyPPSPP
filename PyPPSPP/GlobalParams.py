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

class GlobalParams(object):
    """Global Parameters for use all over the client"""
    version = 1
    min_version = 1
    content_protection_scheme = 0
    chunk_addressing_method = 2
    chunk_size = 1024
    supported_messages_len = 2
    supported_messages = b'11110110 11110000'
    #                      +|-|-|-| |-|-|-|- HANDSHAKE
    #                       +-|-|-| |-|-|-|- DATA
    #                        +|-|-| |-|-|-|- ACK
    #                         +-|-| |-|-|-|- HAVE
    #                          +|-| |-|-|-|- INTEGRITY
    #                           +-| |-|-|-|- PEX_RESv4
    #                            +| |-|-|-|- PEX_REQ
    #                             + |-|-|-|- SIGNED_INTEGRITY
    #                               | | | |
    #                               +-|-|-|- REQUEST
    #                                +|-|-|- CANCEL
    #                                 +-|-|- CHOKE
    #                                  +|-|- UNCHOKE
    #                                   +-|- PEX_RESv6
    #                                    +|- PEX_REScert
    #                                     +- UNUSED
    #                                      + UNUSED

