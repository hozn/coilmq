"""Tests for topic-related functionality."""

from coilmq.topic import TopicManager
from coilmq.util import frames
from coilmq.util.frames import Frame
from tests.mock import MockConnection

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


class TestTopicManager:
    """Test :class:`TopicManager`."""

    def test_subscribe(self):
        """Test subscribing a connection to the topic."""
        tm = TopicManager()
        conn = MockConnection()

        dest = "/topic/dest"

        tm.subscribe(conn, dest)
        f = Frame(frames.MESSAGE, headers={"destination": dest}, body="Empty")
        tm.send(f)

        assert len(conn.frames) == 1
        subscription = conn.frames[0].headers.pop("subscription", None)
        assert subscription == 0
        assert conn.frames[0] == f

    def test_unsubscribe(self):
        """Test unsubscribing a connection from the queue."""
        tm = TopicManager()
        conn = MockConnection()

        dest = "/topic/dest"

        tm.subscribe(conn, dest)
        f = Frame(frames.MESSAGE, headers={"destination": dest}, body="Empty")
        tm.send(f)

        assert len(conn.frames) == 1
        subscription = conn.frames[0].headers.pop("subscription", None)
        assert subscription == 0
        assert conn.frames[0] == f

        tm.unsubscribe(conn, dest)
        f = Frame(frames.MESSAGE, headers={"destination": dest}, body="Empty")
        tm.send(f)

        assert len(conn.frames) == 1

    def test_send_simple(self):
        """Test a basic send command."""
        tm = TopicManager()

        dest = "/topic/dest"

        f = Frame(frames.SEND, headers={"destination": dest}, body="Empty")
        tm.send(f)

        # Assert some side-effects
        assert "message-id" in f.headers
        assert f.cmd == frames.MESSAGE

    def test_send_subscriber_timeout(self):
        """Test a send command when one subscriber errs out."""
        tm = TopicManager()
        conn = MockConnection()

        class TimeoutConnection:
            reliable_subscriber = False

            def send_frame(self, frame):
                raise TimeoutError("timed out")

            def reset(self): ...

        dest = "/topic/dest"

        bad_client = TimeoutConnection()

        # Subscribe both a good client and a bad client.
        tm.subscribe(bad_client, dest)
        tm.subscribe(conn, dest)

        f = Frame(frames.MESSAGE, headers={"destination": dest}, body="Empty")
        tm.send(f)

        # Make sure out good client got the message.
        assert len(conn.frames) == 1
        subscription = conn.frames[0].headers.pop("subscription", None)
        assert subscription == 0
        assert conn.frames[0] == f

        # Make sure our bad client got disconnected
        # (This might be a bit too intimate.)
        connections = {s.connection for s in tm._subscriptions.subscribers(dest)}
        assert bad_client not in connections
