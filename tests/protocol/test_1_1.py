from __future__ import annotations

import time

import pytest

from coilmq.engine import StompEngine
from coilmq.protocol import STOMP11
from coilmq.util import frames
from coilmq.util.frames import ErrorFrame, Frame


@pytest.fixture
def protocol() -> type[STOMP11]:
    """Returns the :func:`engine` fixture's ``protocol`` argument."""
    return STOMP11


class TestSTOMP11:
    def test_heartbeat_from_server(self, engine: StompEngine) -> None:
        engine.process_frame(
            Frame(
                frames.CONNECT,
                headers={"heart-beat": "0,100", "accept-version": "1.1"},
            )
        )
        time.sleep(0.53)
        assert abs(engine.connection.heartbeat_count - 5) < 1

    def test_no_heartbeat_from_client(self, engine: StompEngine) -> None:
        # FIXME: set the protocol's receive_heartbeat_interval to 50; see #64.
        engine.process_frame(
            Frame(
                frames.CONNECT,
                headers={"heart-beat": "100,100", "accept-version": "1.1"},
            )
        )
        assert engine.connected
        assert engine.protocol.timer._running
        time.sleep(0.203)
        assert not engine.connected

    def test_no_heartbeat(self, engine: StompEngine) -> None:
        engine.process_frame(
            Frame(
                frames.CONNECT,
                headers={"heart-beat": "0,0", "accept-version": "1.1"},
            )
        )
        assert engine.connected
        assert engine.protocol.timer._running

    def test_protocol_version_common_exists(self, engine: StompEngine) -> None:
        engine.process_frame(
            Frame(
                frames.CONNECT, headers={"heart-beat": "0,0", "accept-version": "1.1"}
            )
        )
        assert engine.connection.frames[-1].cmd == frames.CONNECTED
        assert "version" in engine.connection.frames[-1].headers

    def test_nack_valid_frame(self, engine: StompEngine) -> None:
        engine.connected = True
        engine.process_frame(
            Frame(frames.NACK, headers={"message-id": 1, "subscription": "foo"})
        )

    def test_nack_invalid_frame(self, engine: StompEngine) -> None:
        engine.connected = True

        engine.process_frame(Frame(frames.NACK, headers={"subscription": "foo"}))
        assert isinstance(engine.connection.frames[-1], ErrorFrame)
        engine.process_frame(Frame(frames.NACK, headers={"message-id": 1}))
        assert isinstance(engine.connection.frames[-1], ErrorFrame)
