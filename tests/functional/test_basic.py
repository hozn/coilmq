"""Functional tests that use the default memory-based storage backends and default
scheduler implementations.
"""

from __future__ import annotations

import zlib
from queue import Empty

import pytest

from coilmq.auth.simple import SimpleAuthenticator
from coilmq.server.socket_server import ThreadedStompServer
from coilmq.store.memory import MemoryQueue
from coilmq.util import frames
from tests.functional import Client

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


@pytest.fixture
def store() -> MemoryQueue:
    """Returns the :func:`server` fixture's ``store`` argument."""
    return MemoryQueue()


class TestServerWithDefaultClasses:
    """Functional tests using default storage engine, etc."""

    def test_connect(self, c1: Client) -> None:
        """Test a basic (non-auth) connection."""
        c1.connect()
        frame = c1.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.CONNECTED

    @pytest.mark.parametrize(
        "server",
        [(SimpleAuthenticator(store={"user": "pass"}))],
        indirect=["server"],
    )
    def test_connect_auth(
        self,
        server: ThreadedStompServer,
        c1: Client,
        c2: Client,
        c3: Client,
    ) -> None:
        """Test connecting when auth is required."""
        c1.connect()
        frame = c1.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.ERROR
        assert b"Auth" in frame.body

        c2.connect(headers={"login": "user", "passcode": "pass"})
        frame = c2.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.CONNECTED

        c3.connect(headers={"login": "user", "passcode": "pass-invalid"})
        frame = c3.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.ERROR

    def test_send_receipt(self, c1: Client) -> None:
        c1.connect()
        frame = c1.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.CONNECTED

        c1.send("/topic/foo", "A message", extra_headers={"receipt": "FOOBAR"})
        frame = c1.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.RECEIPT

    def test_subscribe(self, c1: Client, c2: Client) -> None:
        c1.connect()
        frame = c1.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.CONNECTED

        c2.connect()
        frame = c2.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.CONNECTED

        c1.subscribe("/queue/foo")

        c2.subscribe("/queue/foo2")

        c2.send("/queue/foo", "A message")
        assert c2.received_frames.qsize() == 0

        frame = c1.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.MESSAGE
        assert frame.body == b"A message"

    def test_disconnect(self, c1: Client) -> None:
        """Test the 'polite' disconnect."""
        c1.connect()
        frame = c1.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.CONNECTED
        c1.disconnect()
        with pytest.raises(Empty):
            c1.received_frames.get(timeout=0.5)

    def test_send_binary(self, c1: Client, c2: Client) -> None:
        """Test sending binary data."""
        c1.connect()
        frame = c1.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.CONNECTED

        c2.connect()
        frame = c2.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.CONNECTED

        c1.subscribe("/queue/foo")

        # Read some random binary data.
        # (This should be cross-platform.)
        message = b"This is the message that will be compressed."
        c2.send("/queue/foo", zlib.compress(message))

        frame = c1.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.MESSAGE
        assert zlib.decompress(frame.body) == message

    def test_send_utf8(self, c1: Client, c2: Client) -> None:
        """Test sending utf-8-encoded strings."""
        c1.connect()
        frame = c1.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.CONNECTED

        c2.connect()
        frame = c2.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.CONNECTED

        c1.subscribe("/queue/foo")

        unicodemsg = "我能吞下玻璃而不伤身体"
        utf8msg = unicodemsg.encode("utf-8")

        c2.send("/queue/foo", utf8msg)

        frame = c1.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.MESSAGE
        assert frame.body == utf8msg

    def test_send_large_message(self, c1: Client, c2: Client) -> None:
        """Test sending a large message after a short one."""
        c1.connect()
        frame = c1.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.CONNECTED

        c2.connect()
        frame = c2.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.CONNECTED

        c1.subscribe("/queue/foo")

        shortmessage = b"x"
        longmessage = b"y" * 1024 * 16

        c2.send("/queue/foo", shortmessage)

        frame = c1.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.MESSAGE
        assert frame.body == shortmessage

        c2.send("/queue/foo", longmessage)

        frame = c1.received_frames.get(timeout=0.5)
        assert frame.cmd == frames.MESSAGE
        assert frame.body == longmessage
