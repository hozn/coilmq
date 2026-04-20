"""Functional tests to test full stack (but not actual socket layer)."""

import logging
import select
import socket
import threading
import unittest
from queue import Queue

from coilmq.protocol import STOMP10
from coilmq.queue import QueueManager
from coilmq.scheduler import FavorReliableSubscriberScheduler, RandomQueueScheduler
from coilmq.server.socket_server import ThreadedStompServer
from coilmq.store.memory import MemoryQueue
from coilmq.topic import TopicManager
from coilmq.util import frames
from coilmq.util.frames import Frame, FrameBuffer

__authors__ = ['"Hans Lellelid" <hans@xmpl.org>']
__copyright__ = "Copyright 2009 Hans Lellelid"
__license__ = """Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""


class BaseFunctionalTestCase(unittest.TestCase):
    """Base class for test cases provides the fixtures for setting up the multi-threaded
    unit test infrastructure.

    We use a combination of :py:class:`threading.Event` and :py:class:`Queue.Queue` objects to facilitate
    inter-thread communication and lock-stepping the assertions.
    """

    def setUp(self):

        self.clients = []
        self.server = ThreadedStompServer(
            ("127.0.0.1", 0),
            authenticator=None,
            queue_manager=self._queuemanager(),
            topic_manager=self._topicmanager(),
            protocol=STOMP10,
        )
        self.server_address = self.server.socket.getsockname()
        self.server_thread = threading.Thread(
            target=self.server.serve_forever, name="server"
        )
        self.server_thread.start()

    def _queuemanager(self):
        """Returns the configured :class:`QueueManager` instance to use.

        Can be overridden by subclasses that wish to change out any queue mgr parameters.

        :rtype: QueueManager
        """
        return QueueManager(
            store=MemoryQueue(),
            subscriber_scheduler=FavorReliableSubscriberScheduler(),
            queue_scheduler=RandomQueueScheduler(),
        )

    def _topicmanager(self):
        """Returns the configured :class:`TopicManager` instance to use.

        Can be overridden by subclasses that wish to change out any topic mgr parameters.

        :rtype: TopicManager
        """
        return TopicManager()

    def tearDown(self):
        for c in self.clients:
            c.close()
        self.server.shutdown()
        self.server_thread.join()
        del self.server_thread

    def _new_client(self, connect=True):
        """Get a new :class:`TestStompClient` connected to our test server.

        The client will also be registered for close in the tearDown method.

        :param connect: Whether to issue the CONNECT command.
        :type connect: bool

        :rtype: TestStompClient
        """
        client = StompClient(self.server_address)
        self.clients.append(client)
        if connect:
            client.connect()
            res = client.received_frames.get(timeout=1)
            assert res.cmd == frames.CONNECTED
        return client


class StompClient:
    """A stomp client for use in testing.

    This client spawns a listener thread and pushes anything that comes in onto the
    read_frames queue.

    :var received_frames: A queue of Frame instances that have been received.
    :vartype received_frames: Queue.Queue[coilmq.util.frames.Frame]
    """

    def __init__(self, addr, connect=True):
        """:param addr: The (host,port) tuple for connection.
        :type addr: tuple
        :param connect: Whether to connect socket to specified addr.
        :type connect: bool

        """
        self.log = logging.getLogger(f"{self.__module__}.{self.__class__.__name__}")
        self.sock = None
        self.addr = addr
        self.received_frames = Queue()
        self.read_stopped = threading.Event()
        self.buffer = FrameBuffer()
        if connect:
            self._connect()

    def connect(self, headers=None):
        self.send_frame(Frame(frames.CONNECT, headers=headers))

    def send(self, destination, message, set_content_length=True, extra_headers=None):
        headers = extra_headers or {}
        headers["destination"] = destination
        if set_content_length:
            headers["content-length"] = len(message)
        self.send_frame(Frame(frames.SEND, headers=headers, body=message))

    def subscribe(self, destination):
        self.send_frame(Frame(frames.SUBSCRIBE, headers={"destination": destination}))

    def send_frame(self, frame):
        """Sends a stomp frame.
        :param frame: The stomp frame to send.
        :type frame: coilmq.util.frames.Frame.
        """
        if not self.connected:
            raise RuntimeError("Not connected")
        self.sock.send(frame.pack())

    def _connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(self.addr)
        self.connected = True
        self.read_stopped.clear()
        t = threading.Thread(
            target=self._read_loop, name=f"client-receiver-{hex(id(self))}"
        )
        t.start()

    def _read_loop(self):
        while self.connected:
            r, *_ = select.select([self.sock], [], [], 0.1)
            if r:
                data = self.sock.recv(1024)
                self.buffer.append(data)
                for frame in self.buffer:
                    self.log.debug("Processing frame: %s", frame)
                    self.received_frames.put(frame)
        self.read_stopped.set()

    def disconnect(self):
        self.send_frame(Frame(frames.DISCONNECT))

    def close(self):
        if not self.connected:
            raise RuntimeError("Not connected")
        self.connected = False
        self.read_stopped.wait(timeout=0.5)
        self.sock.close()
