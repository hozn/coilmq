from __future__ import annotations

from queue import Empty
from typing import TYPE_CHECKING

import pytest

from coilmq.util.frames import CONNECTED

if TYPE_CHECKING:
    from tests.functional import Client


def test_connected_response(c1: Client) -> None:
    """Verify that the server only sends one CONNECTED frame.

    This test is intended to demonstrate the existence of :issue:`32`.

    """
    c1.connect(headers={"accept-version": "1.0"})
    frame = c1.received_frames.get(timeout=0.5)
    assert frame.cmd == CONNECTED

    with pytest.raises(Empty):
        c1.received_frames.get(timeout=0.5)
