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

