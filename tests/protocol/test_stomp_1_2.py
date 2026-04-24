from __future__ import annotations

import socket

import pytest

from coilmq.engine import StompEngine
from coilmq.protocol import STOMP11, STOMP12
from coilmq.util import frames
from coilmq.util.frames import Frame


@pytest.fixture
def protocol() -> type[STOMP12]:
    """Returns the :func:`engine` fixture's ``protocol`` argument."""
    return STOMP12


class TestSTOMP12:
    def test_host_valid(self, engine: StompEngine) -> None:
        engine.process_frame(
            Frame(
                frames.CONNECT,
                {
                    "host": socket.getfqdn(),
                    "accept-version": "1.2",
                },
            )
        )
        assert engine.connection.frames[-1].cmd == frames.CONNECTED

    def test_host_invalid(self, engine: StompEngine) -> None:
        engine.process_frame(
            Frame(
                frames.CONNECT,
                {
                    "host": "nothing.nowhere.com",
                    "accept-version": "1.2",
                },
            )
        )
        assert engine.connection.frames[-1].cmd == frames.ERROR
        assert (
            engine.connection.frames[-1].headers["message"]
            == "Virtual hosting is not supported or host is unknown"
        )

    def test_no_host_header(self, engine: StompEngine) -> None:
        engine.process_frame(
            Frame(
                frames.CONNECT,
                {"accept-version": "1.2"},
            )
        )
        assert engine.connection.frames[-1].cmd == frames.ERROR
        assert (
            engine.connection.frames[-1].headers["message"]
            == '"host" header is required'
        )

    def test_protocol_downgrade(self, engine: StompEngine) -> None:
        engine.process_frame(
            Frame(
                frames.CONNECT,
                {
                    "host": socket.getfqdn(),
                    "accept-version": "1.1",
                },
            )
        )
        assert engine.connection.frames[-1].cmd == frames.CONNECTED
        assert engine.connection.frames[-1].headers["version"] == "1.1"
        # Note: assertIsInstance is not appropriate here because the test would
        # pass if self.engine.protocol was STOMP10 which is not what we want.
        # `assertIs(type(obj), cls)` is appropriate way to check for an object's
        # exact type.
        assert type(engine.protocol) is STOMP11
