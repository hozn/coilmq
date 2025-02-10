"""
The default/recommended SocketServer-based server implementation. 
"""
import asyncio
import logging
import socket
from typing import Type

from coilmq.auth import Authenticator
from coilmq.util.frames import FrameBuffer
from coilmq.asyncio.server import StompConnection
from coilmq.asyncio.engine import StompEngine
from coilmq.asyncio.protocol import STOMP
from coilmq.asyncio.queue import QueueManager
from coilmq.asyncio.topic import TopicManager
from coilmq.exception import ClientDisconnected

__authors__ = ['"Hans Lellelid" <hans@xmpl.org>']
__copyright__ = "Copyright 2009 Hans Lellelid"
__license__ = """Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
  https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""


class StompHandler(StompConnection):

    def __init__(self,
                 timeout: float = 3.0,
                 authenticator: Authenticator = None,
                 queue_manager: QueueManager = None,
                 topic_manager: TopicManager = None,
                 protocol: Type[STOMP] = None):
        self.debug = True
        self.log = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))
        self.timeout = timeout
        self.authenticator = authenticator
        self.queue_manager = queue_manager
        self.topic_manager = topic_manager
        self.protocol = protocol

    async def on_connected(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.buffer = FrameBuffer()
        self.engine = StompEngine(
            connection=self,
            authenticator=self.authenticator,
            queue_manager=self.queue_manager,
            topic_manager=self.topic_manager,
            protocol=self.protocol,
        )
        self.reader = reader
        self.writer = writer
        try:
            while True:
                try:
                    async for data in self.reader:
                        if not data:
                            break
                        if self.debug:
                            self.log.debug("RECV: %r" % data)
                        self.buffer.append(data)

                        for frame in self.buffer:
                            self.log.debug("Processing frame: %s" % frame)
                            await self.engine.process_frame(frame)
                            if not self.engine.connected:
                                raise ClientDisconnected()
                except socket.timeout:  # pragma: no cover
                    pass
        except ClientDisconnected:
            self.log.debug("Client disconnected, discontinuing read loop.")
        except Exception as e:  # pragma: no cover
            self.log.error("Error receiving data (unbinding): %s" % e)
            await self.engine.unbind()
            raise
        finally:
            await self.engine.unbind()
            await self.queue_manager.close()
            await self.topic_manager.close()
            if hasattr(self.authenticator, 'close'):
                self.authenticator.close()
            self.writer.close()  # close the socket


    async def send_frame(self, frame):
        """ Sends a frame to connected socket client.

        @param frame: The frame to send.
        @type frame: C{coilmq.util.frames.Frame}
        """
        packed = frame.pack()
        if self.debug:  # pragma: no cover
            self.log.debug("SEND: %r" % packed)
        self.writer.write(packed)
        await self.writer.drain()
