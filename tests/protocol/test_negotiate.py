from __future__ import annotations

import re
from contextlib import nullcontext
from typing import TYPE_CHECKING

import pytest

from coilmq.exception import ProtocolError
from coilmq.protocol import STOMP10, STOMP11, STOMP12, protocol_class_from_first_frame
from coilmq.util.frames import CONNECT, SEND, STOMP_CMD, Frame

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from coilmq.protocol import STOMP


@pytest.mark.parametrize(
    ("frame", "expectation"),
    [
        (
            Frame(CONNECT),
            nullcontext(STOMP10),
        ),
        (
            Frame(CONNECT, {"accept-version": "1.0"}),
            nullcontext(STOMP10),
        ),
        (
            Frame(CONNECT, {"accept-version": "1.1"}),
            nullcontext(STOMP11),
        ),
        (
            Frame(CONNECT, {"accept-version": "1.2"}),
            nullcontext(STOMP12),
        ),
        (
            Frame(CONNECT, {"accept-version": "1.0,1.1"}),
            nullcontext(STOMP11),
        ),
        (
            Frame(CONNECT, {"accept-version": "1.1,1.0"}),
            nullcontext(STOMP11),
        ),
        (
            Frame(CONNECT, {"accept-version": "1.2,10.0"}),
            nullcontext(STOMP12),
        ),
        (
            Frame(STOMP_CMD),
            pytest.raises(
                ProtocolError,
                match="No 'accept-version' specified for STOMP command",
            ),
        ),
        (
            Frame(CONNECT, {"accept-version": "10.0"}),
            pytest.raises(
                ProtocolError,
                match=re.escape("Supported protocol versions are 1.0 1.1 1.2"),
            ),
        ),
        (
            Frame(CONNECT, {"accept-version": "1.0 1.2"}),
            pytest.raises(
                ProtocolError,
                match="Malformed 'accept-version'",
            ),
        ),
        (
            Frame(SEND),
            pytest.raises(
                ProtocolError,
                match="Expected CONNECT or STOMP command",
            ),
        ),
        (
            Frame(SEND, {"accept-version": "1.1"}),
            pytest.raises(
                ProtocolError,
                match="Expected CONNECT or STOMP command",
            ),
        ),
    ],
)
def test_protocol_class_from_first_frame(
    frame: Frame,
    expectation: AbstractContextManager[type[STOMP]] | pytest.RaisesExc[ProtocolError],
) -> None:
    """Verify that :func:`protocol_class_from_first_frame` returns the right version."""
    with expectation as e:
        assert protocol_class_from_first_frame(frame) == e
