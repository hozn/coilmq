"""Tests for the transport-agnostic engine module."""

from __future__ import annotations

from typing import Generator

import pytest

from coilmq.auth import Authenticator
from coilmq.engine import StompEngine
from coilmq.util import frames
from coilmq.util.frames import Frame, ReceiptFrame
from tests.mock import (
    MockAuthenticator,
    MockConnection,
    MockQueueManager,
    MockTopicManager,
)

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
def engine(request: pytest.FixtureRequest) -> Generator[StompEngine, None, None]:
    authenticator: Authenticator | None = (
        request.param if hasattr(request, "param") else None
    )

    engine = StompEngine(
        connection=MockConnection(),
        queue_manager=MockQueueManager(),
        topic_manager=MockTopicManager(),
        authenticator=authenticator,
    )

    try:
        yield engine
    finally:
        engine.unbind()


def assertErrorFrame(frame: Frame, msgsub: str) -> None:
    """Assert frame is an ERROR frame whose message header contains msgsub."""
    assert frame.cmd == frames.ERROR
    assert msgsub.lower() in frame.headers["message"].lower()


class TestEngine:
    def test_connect_no_auth(self, engine: StompEngine) -> None:
        """Test the CONNECT command with no auth required."""
        assert engine.connected == False
        engine.process_frame(Frame(frames.CONNECT))
        assert engine.connected == True

    @pytest.mark.parametrize("engine", [MockAuthenticator()], indirect=["engine"])
    def test_connect_auth(self, engine: StompEngine) -> None:
        """Test the CONNECT command when auth is required."""
        assert engine.connected == False
        engine.process_frame(Frame(frames.CONNECT))
        assertErrorFrame(engine.connection.frames[-1], "Auth")
        assert engine.connected == False

        engine.process_frame(
            Frame(
                frames.CONNECT,
                headers={
                    "login": MockAuthenticator.LOGIN,
                    "passcode": MockAuthenticator.PASSCODE,
                },
            )
        )
        assert engine.connected == True

    def test_subscribe_noack(self, engine: StompEngine) -> None:
        """Test subscribing to topics and queues w/ no ACK."""
        engine.process_frame(Frame(frames.CONNECT))
        engine.process_frame(
            Frame(frames.SUBSCRIBE, headers={"destination": "/queue/bar"})
        )
        assert engine.connection in engine.queue_manager.queues["/queue/bar"]

        engine.process_frame(
            Frame(frames.SUBSCRIBE, headers={"destination": "/foo/bar"})
        )
        assert engine.connection in engine.topic_manager.topics["/foo/bar"]

    def test_send(self, engine: StompEngine) -> None:
        """Test sending to a topic and queue."""
        engine.process_frame(Frame(frames.CONNECT))

        msg = Frame(
            frames.SEND, headers={"destination": "/queue/foo"}, body="QUEUEMSG-BODY"
        )
        engine.process_frame(msg)
        assert msg == engine.queue_manager.messages[-1]

        msg = Frame(
            frames.SEND, headers={"destination": "/topic/foo"}, body="TOPICMSG-BODY"
        )
        engine.process_frame(msg)
        assert msg == engine.topic_manager.messages[-1]

        msg = Frame(frames.SEND, headers={}, body="TOPICMSG-BODY")
        engine.process_frame(msg)
        assertErrorFrame(engine.connection.frames[-1], "Missing destination")

    def test_receipt(self, engine: StompEngine) -> None:
        """Test pushing frames with a receipt specified."""
        engine.process_frame(Frame(frames.CONNECT))

        receipt_id = "FOOBAR"
        msg = Frame(
            frames.SEND,
            headers={"destination": "/queue/foo", "receipt": receipt_id},
            body="QUEUEMSG-BODY",
        )
        engine.process_frame(msg)
        rframe = engine.connection.frames[-1]
        assert isinstance(rframe, ReceiptFrame)
        assert receipt_id == rframe.headers.get("receipt-id")

        receipt_id = "FOOBAR2"
        engine.process_frame(
            Frame(
                frames.SUBSCRIBE,
                headers={"destination": "/queue/bar", "receipt": receipt_id},
            )
        )
        rframe = engine.connection.frames[-1]
        assert isinstance(rframe, ReceiptFrame)
        assert receipt_id == rframe.headers.get("receipt-id")

    def test_subscribe_ack(self, engine: StompEngine) -> None:
        """Test subscribing to a queue with ack=true."""
        engine.process_frame(Frame(frames.CONNECT))
        engine.process_frame(
            Frame(
                frames.SUBSCRIBE, headers={"destination": "/queue/bar", "ack": "client"}
            )
        )
        assert engine.connection.reliable_subscriber == True
        assert engine.connection in engine.queue_manager.queues["/queue/bar"]

    def test_unsubscribe(self, engine: StompEngine) -> None:
        """Test the UNSUBSCRIBE command."""
        engine.process_frame(Frame(frames.CONNECT))
        engine.process_frame(
            Frame(frames.SUBSCRIBE, headers={"destination": "/queue/bar"})
        )
        assert engine.connection in engine.queue_manager.queues["/queue/bar"]

        engine.process_frame(
            Frame(frames.UNSUBSCRIBE, headers={"destination": "/queue/bar"})
        )
        assert engine.connection not in engine.queue_manager.queues["/queue/bar"]

        engine.process_frame(
            Frame(frames.UNSUBSCRIBE, headers={"destination": "/invalid"})
        )

    def test_begin(self, engine: StompEngine) -> None:
        """Test transaction BEGIN."""
        engine.process_frame(Frame(frames.CONNECT))

        engine.process_frame(Frame(frames.BEGIN, headers={"transaction": "abc"}))
        assert "abc" in engine.transactions
        assert len(engine.transactions["abc"]) == 0

    def test_commit(self, engine: StompEngine) -> None:
        """Test transaction COMMIT."""
        engine.process_frame(Frame(frames.CONNECT))

        engine.process_frame(Frame(frames.BEGIN, headers={"transaction": "abc"}))
        engine.process_frame(Frame(frames.BEGIN, headers={"transaction": "123"}))
        engine.process_frame(
            Frame(
                frames.SEND,
                headers={"destination": "/dest", "transaction": "abc"},
                body="ASDF",
            )
        )
        engine.process_frame(
            Frame(
                frames.SEND,
                headers={"destination": "/dest", "transaction": "abc"},
                body="ASDF",
            )
        )
        engine.process_frame(
            Frame(
                frames.SEND,
                headers={"destination": "/dest", "transaction": "123"},
                body="ASDF",
            )
        )

        assert len(engine.topic_manager.messages) == 0

        engine.process_frame(Frame(frames.COMMIT, headers={"transaction": "abc"}))

        assert len(engine.topic_manager.messages) == 2

        assert len(engine.transactions) == 1

        engine.process_frame(Frame(frames.COMMIT, headers={"transaction": "123"}))
        assert len(engine.topic_manager.messages) == 3

        assert len(engine.transactions) == 0

    def test_commit_invalid(self, engine: StompEngine) -> None:
        """Test invalid states for transaction COMMIT."""
        engine.process_frame(Frame(frames.CONNECT))

        # Send a message with invalid transaction
        f = Frame(
            frames.SEND,
            headers={"destination": "/dest", "transaction": "123"},
            body="ASDF",
        )
        engine.process_frame(f)
        assertErrorFrame(engine.connection.frames[-1], "invalid transaction")

        # Attempt to commit invalid transaction
        engine.process_frame(Frame(frames.COMMIT, headers={"transaction": "abc"}))

        # Attempt to commit already-committed transaction
        engine.process_frame(Frame(frames.BEGIN, headers={"transaction": "abc"}))
        engine.process_frame(
            Frame(
                frames.SEND,
                headers={"destination": "/dest", "transaction": "abc"},
                body="FOO",
            )
        )
        engine.process_frame(Frame(frames.COMMIT, headers={"transaction": "abc"}))

        engine.process_frame(Frame(frames.COMMIT, headers={"transaction": "abc"}))
        assertErrorFrame(engine.connection.frames[-1], "invalid transaction")

    def test_abort(self, engine: StompEngine) -> None:
        """Test transaction ABORT."""
        engine.process_frame(Frame(frames.CONNECT))

        engine.process_frame(Frame(frames.BEGIN, headers={"transaction": "abc"}))
        engine.process_frame(Frame(frames.BEGIN, headers={"transaction": "123"}))

        f1 = Frame(
            frames.SEND,
            headers={"destination": "/dest", "transaction": "abc"},
            body="ASDF",
        )
        engine.process_frame(f1)
        f2 = Frame(
            frames.SEND,
            headers={"destination": "/dest", "transaction": "abc"},
            body="ASDF",
        )
        engine.process_frame(f2)
        f3 = Frame(
            frames.SEND,
            headers={"destination": "/dest", "transaction": "123"},
            body="ASDF",
        )
        engine.process_frame(f3)

        assert len(engine.topic_manager.messages) == 0

        engine.process_frame(Frame(frames.ABORT, headers={"transaction": "abc"}))
        assert len(engine.topic_manager.messages) == 0

        assert len(engine.transactions) == 1

    def test_abort_invalid(self, engine: StompEngine) -> None:
        """Test invalid states for transaction ABORT."""
        engine.process_frame(Frame(frames.CONNECT))

        engine.process_frame(Frame(frames.ABORT, headers={"transaction": "abc"}))

        assertErrorFrame(engine.connection.frames[-1], "invalid transaction")

        engine.process_frame(Frame(frames.BEGIN, headers={"transaction": "abc"}))
        engine.process_frame(Frame(frames.ABORT, headers={"transaction": "abc"}))

        engine.process_frame(Frame(frames.ABORT, headers={"transaction": "abc2"}))
        assertErrorFrame(engine.connection.frames[-1], "invalid transaction")
