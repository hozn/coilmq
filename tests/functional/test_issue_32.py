from __future__ import annotations

from queue import Empty
from typing import TYPE_CHECKING

import pytest

from coilmq.protocol import STOMP11
from coilmq.util.frames import CONNECTED

if TYPE_CHECKING:
    from coilmq.protocol import STOMP
    from tests.functional import Client


@pytest.fixture
def protocol_class() -> type[STOMP]:
    """Returns the :func:`server` fixture's ``protocol_class`` argument."""
    return STOMP11


def test_connected_response(c1: Client) -> None:
    """Verify that the server only sends one CONNECTED frame.

    This test is intended to demonstrate the existence of :issue:`32`.

    """
    c1.connect(headers={"accept-version": "1.0"})
    frame = c1.received_frames.get(timeout=0.5)
    assert frame.cmd == CONNECTED

    with pytest.raises(Empty):
        c1.received_frames.get(timeout=0.5)
