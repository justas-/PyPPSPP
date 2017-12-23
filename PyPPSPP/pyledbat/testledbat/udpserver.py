"""
Copyright 2017, J. Poderys, Technical University of Denmark

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
"""
Asyncio implementation of UDP server.
"""
import asyncio
import logging

class UdpServer(asyncio.DatagramProtocol):
    """Extension of asyncio DatagramProtocol"""

    def __init__(self, **kwargs):
        self._transport = None
        self._receiver = None

    def send_data(self, data, addr):
        self._transport.sendto(data, addr)

    def register_receiver(self, receiver):
        self._receiver = receiver

    def connection_made(self, transport):
        self._transport = transport

    def datagram_received(self, data, addr):
        self._receiver.datagram_received(data, addr)

    def error_received(self, exc):
        logging.warning('Error received: %s', exc)

    def connection_lost(self, exc):
        logging.error('Connection lost: %s', exc)

    def pause_writing(self):
        logging.info('Socket is above high-water mark')

    def resume_writing(self):
        logging.info('Socket is below high-water mark')
