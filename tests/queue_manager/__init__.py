"""Tests for queue-related classes."""

import re
import uuid

import pytest

from coilmq.queue import QueueManager
from coilmq.store import QueueStore
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


class QueueManagerTests:
    """Base class containing tests for :class:`QueueManager`."""

    def test_frames_iterator(self, store: QueueStore, queue_manager: QueueManager):
        dest = "/queue/dest"
        f = Frame(frames.SEND, headers={"destination": dest}, body="Empty")
        queue_manager.send(f)

        assert bool(store.frames(dest))

    def test_subscribe(self, queue_manager: QueueManager, conn: MockConnection):
        """Test subscribing a connection to the queue."""
        dest = "/queue/dest"

        queue_manager.subscribe(conn, dest)
        f = Frame(frames.MESSAGE, headers={"destination": dest}, body="Empty")
        queue_manager.send(f)

        assert len(conn.frames) == 1
        assert conn.frames[0] == f

    def test_unsubscribe(
        self, store: QueueStore, queue_manager: QueueManager, conn: MockConnection
    ):
        """Test unsubscribing a connection from the queue."""
        dest = "/queue/dest"

        queue_manager.subscribe(conn, dest)
        f = Frame(frames.MESSAGE, headers={"destination": dest}, body="Empty")
        queue_manager.send(f)

        assert len(conn.frames) == 1
        assert conn.frames[0] == f

        queue_manager.unsubscribe(conn, dest)
        f = Frame(frames.MESSAGE, headers={"destination": dest}, body="Empty")
        queue_manager.send(f)

        assert len(conn.frames) == 1
        assert len(store.frames(dest)) == 1

    def test_send_simple(self, store: QueueStore, queue_manager: QueueManager):
        """Test a basic send command."""
        dest = "/queue/dest"

        f = Frame(frames.SEND, headers={"destination": dest}, body="Empty")
        queue_manager.send(f)

        assert dest in store.destinations()
        assert len(store.frames(dest)) == 1

        # Assert some side-effects
        assert "message-id" in f.headers
        assert f.cmd == frames.MESSAGE

    def test_send_err(self, queue_manager: QueueManager):
        """Test sending a message when delivery results in error."""

        class ExcThrowingConn:
            reliable_subscriber = True

            def send_frame(self, frame):
                raise RuntimeError("Error sending data.")

        dest = "/queue/dest"

        # This reliable subscriber will be chosen first
        conn = ExcThrowingConn()
        queue_manager.subscribe(conn, dest)

        f = Frame(frames.SEND, headers={"destination": dest}, body="Empty")
        with pytest.raises(RuntimeError, match=re.escape("Error sending data.")):
            queue_manager.send(f)

    def test_send_backlog_err_reliable(
        self, queue_manager: QueueManager, conn: MockConnection
    ):
        """Test errors when sending backlog to reliable subscriber."""

        class ExcThrowingConn:
            reliable_subscriber = True

            def send_frame(self, frame):
                raise RuntimeError("Error sending data.")

        dest = "/queue/send-backlog-err-reliable"

        f = Frame(frames.SEND, headers={"destination": dest}, body="Empty")
        queue_manager.send(f)

        econn = ExcThrowingConn()
        with pytest.raises(RuntimeError, match=re.escape("Error sending data.")):
            queue_manager.subscribe(econn, dest)

        # The message will have been requeued at this point, so add a valid
        # subscriber

        queue_manager.subscribe(conn, dest)

        assert len(conn.frames) == 1, "Expected frame to be delivered"
        subscription = conn.frames[0].headers.pop("subscription", None)
        assert subscription == 0
        assert conn.frames[0] == f

    def test_send_backlog_err_unreliable(
        self, queue_manager: QueueManager, conn: MockConnection
    ):
        """Test errors when sending backlog to reliable subscriber."""

        class ExcThrowingConn:
            reliable_subscriber = False

            def send_frame(self, frame):
                raise RuntimeError("Error sending data.")

        dest = "/queue/dest"

        f = Frame(frames.SEND, headers={"destination": dest}, body="123")
        queue_manager.send(f)

        f2 = Frame(frames.SEND, headers={"destination": dest}, body="12345")
        queue_manager.send(f2)

        econn = ExcThrowingConn()
        with pytest.raises(RuntimeError, match=re.escape("Error sending data.")):
            queue_manager.subscribe(econn, dest)

        # The message will have been requeued at this point, so add a valid
        # subscriber

        queue_manager.subscribe(conn, dest)

        assert len(conn.frames) == 2, "Expected frame to be delivered"
        for frame in conn.frames:
            subscription = frame.headers.pop("subscription", None)
            assert subscription == 0

        assert list(conn.frames) == [f2, f]

    def test_send_reliableFirst(self, queue_manager: QueueManager):
        """Test that messages are prioritized to reliable subscribers.

        This is actually a test of the underlying scheduler more than it is a test
        of the send message, per se.
        """
        dest = "/queue/dest"
        conn1 = MockConnection()
        conn1.reliable_subscriber = True

        queue_manager.subscribe(conn1, dest)

        conn2 = MockConnection()
        conn2.reliable_subscriber = False
        queue_manager.subscribe(conn2, dest)

        f = Frame(
            frames.MESSAGE,
            headers={"destination": dest, "message-id": uuid.uuid4()},
            body="Empty",
        )
        queue_manager.send(f)

        assert len(conn1.frames) == 1
        assert len(conn2.frames) == 0

    def test_clear_transaction_frames(
        self, store: QueueStore, queue_manager: QueueManager
    ):
        """Test the clearing of transaction ACK frames."""
        dest = "/queue/tx"

        f = Frame(
            frames.SEND,
            headers={"destination": dest, "transaction": "1"},
            body="Body-A",
        )
        queue_manager.send(f)

        assert dest in store.destinations()

        conn1 = MockConnection()
        conn1.reliable_subscriber = True
        queue_manager.subscribe(conn1, dest)

        assert len(conn1.frames) == 1

        queue_manager.clear_transaction_frames(conn1, "1")

    def test_ack_basic(self, queue_manager: QueueManager):
        """Test reliable client (ACK) behavior."""
        dest = "/queue/ack-basic"
        conn1 = MockConnection()
        conn1.reliable_subscriber = True

        queue_manager.subscribe(conn1, dest)

        m1 = Frame(
            frames.MESSAGE, headers={"destination": dest}, body="Message body (1)"
        )
        queue_manager.send(m1)

        assert conn1.frames[0] == m1

        m2 = Frame(
            frames.MESSAGE, headers={"destination": dest}, body="Message body (2)"
        )
        queue_manager.send(m2)

        assert len(conn1.frames) == 1
        assert conn1.frames[0] == m1

        ack = Frame(
            frames.ACK,
            headers={"destination": dest, "message-id": m1.headers["message-id"]},
        )
        queue_manager.ack(conn1, ack)

        assert len(conn1.frames) == 2, "Expected 2 frames now, after ACK."
        subscription = conn1.frames[1].headers.pop("subscription", None)
        assert subscription == 0
        assert conn1.frames[1] == m2

    def test_ack_transaction(self, queue_manager: QueueManager):
        """Test the reliable client (ACK) behavior with transactions."""
        dest = "/queue/ack-transaction"
        conn1 = MockConnection()
        conn1.reliable_subscriber = True

        queue_manager.subscribe(conn1, dest)

        m1 = Frame(
            frames.MESSAGE,
            headers={
                "destination": dest,
            },
            body="Message body (1)",
        )
        queue_manager.send(m1)

        assert conn1.frames[0] == m1

        m2 = Frame(
            frames.MESSAGE, headers={"destination": dest}, body="Message body (2)"
        )
        queue_manager.send(m2)

        assert len(conn1.frames) == 1, "Expected connection to still only have 1 frame."
        assert conn1.frames[0] == m1

        ack = Frame(
            frames.ACK,
            headers={
                "destination": dest,
                "transaction": "abc",
                "message-id": m1.headers.get("message-id"),
            },
        )
        queue_manager.ack(conn1, ack, transaction="abc")

        ack = Frame(
            frames.ACK,
            headers={
                "destination": dest,
                "transaction": "abc",
                "message-id": m2.headers.get("message-id"),
            },
        )
        queue_manager.ack(conn1, ack, transaction="abc")

        assert len(conn1.frames) == 2, "Expected 2 frames now, after ACK."
        subscription = conn1.frames[1].headers.pop("subscription", None)
        assert subscription == 0
        assert conn1.frames[1] == m2

        queue_manager.resend_transaction_frames(conn1, transaction="abc")

        assert len(conn1.frames) == 3, "Expected 3 frames after re-transmit."
        pending = {s for s in queue_manager._pending if s.connection == conn1}
        assert len(pending) == 1, "Expected 1 pending (waiting on ACK) frame."

    def test_disconnect_pending_frames(
        self, store: QueueStore, queue_manager: QueueManager
    ):
        """Test a queue disconnect when there are pending frames."""
        dest = "/queue/disconnect-pending-frames"
        conn1 = MockConnection()
        conn1.reliable_subscriber = True

        queue_manager.subscribe(conn1, dest)

        m1 = Frame(
            frames.MESSAGE, headers={"destination": dest}, body="Message body (1)"
        )
        queue_manager.send(m1)

        assert conn1.frames[0] == m1

        queue_manager.disconnect(conn1)

        # Now we need to ensure that the frame we sent is re-queued.
        assert len(store.frames(dest)) == 1
