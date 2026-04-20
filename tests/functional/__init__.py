"""Functional tests to test full stack (but not actual socket layer)."""

from __future__ import annotations

import logging
import socket
from queue import Empty, Queue
from selectors import EVENT_READ, DefaultSelector
from socket import create_connection
from threading import Event, Thread
from typing import Final

from typing_extensions import Self

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


class Client:
    """A STOMP client for use in testing.

    This client creates a thread that sends frames to, and receives frames from,
    the server.  Frames received from the server are pushed into
    :attr:`~Client.received_frames`.

    :param addr: The ``(host, port)`` tuple for connection.
    """

    received_frames: Final[Queue[Frame]]
    """Frames received by this client."""

    def __init__(self, addr: tuple[str, int]) -> None:
        self.log = logging.getLogger(f"{self.__module__}.{self.__class__.__name__}")

        self.received_frames: Queue[Frame] = Queue()
        self.addr = addr

        self._rsock, self._ssock = socket.socketpair()
        self._stopping = Event()
        self._sending: Queue[Frame] = Queue()
        self._thread = Thread(
            target=self._thread_func,
            name=f"client-receiver-{hex(id(self))}",
        )

        self._thread.start()

    def connect(self, headers: dict[str, str] | None = None):
        """Send a ``CONNECT`` frame to the server."""
        self._sending.put(Frame(frames.CONNECT, headers=headers))
        self._rsock.sendall(b"\0")

    def send(
        self,
        destination: str,
        body: str,
        set_content_length: bool = True,
        extra_headers: dict[str, str] | None = None,
    ):
        """Send a ``SEND`` frame to the server."""
        headers: dict[str, str] = {
            "destination": destination,
        }

        if set_content_length:
            headers["content-length"] = str(len(body))

        if extra_headers:
            for key, value in extra_headers.items():
                headers.setdefault(key, value)

        self._sending.put(Frame(frames.SEND, headers, body))
        self._rsock.sendall(b"\0")

    def subscribe(self, destination: str):
        """Send a ``SUBSCRIBE`` frame to the server."""
        self._sending.put(Frame(frames.SUBSCRIBE, {"destination": destination}))
        self._rsock.sendall(b"\0")

    def disconnect(self):
        """Send a ``DISCONNECT`` frame to the server."""
        self._sending.put(Frame(frames.DISCONNECT))
        self._rsock.sendall(b"\0")

    def _thread_func(self) -> None:
        frame_buffer = FrameBuffer()

        with DefaultSelector() as sel, create_connection(self.addr) as server:
            sel.register(server, EVENT_READ)
            sel.register(self._ssock, EVENT_READ)

            while not self._stopping.is_set():
                for event, mask in sel.select():
                    if event.fileobj is server:
                        data = server.recv(1024)
                        frame_buffer.append(data)
                        for frame in frame_buffer:
                            self.log.debug("Processing frame: %s", frame)
                            self.received_frames.put_nowait(frame)

                    if event.fileobj is self._ssock:
                        try:
                            frame = self._sending.get_nowait()
                        except Empty:
                            pass
                        else:
                            server.sendall(frame.pack())
                        finally:
                            self._ssock.recv(1)

    def close(self):
        """Close the client's connection to the server."""
        self._stopping.set()
        self._rsock.sendall(b"\0")
        self._thread.join()
        self._rsock.close()
        self._ssock.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
