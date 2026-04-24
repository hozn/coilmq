"""Test memory queue storage."""

import uuid

import pytest

from coilmq.store.memory import MemoryQueue
from coilmq.util import frames
from coilmq.util.frames import Frame
from tests.store import BaseQueueTests

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
    return MemoryQueue()


class TestMemoryQueue(BaseQueueTests):
    def test_dequeue_identity(self, store: MemoryQueue) -> None:
        """Test the dequeue() method."""
        dest = "/queue/foo"
        frame = Frame(
            frames.MESSAGE, headers={"message-id": str(uuid.uuid4())}, body="some data"
        )
        store.enqueue(dest, frame)

        assert store.has_frames(dest)
        assert store.size(dest) == 1

        rframe = store.dequeue(dest)
        assert frame == rframe
        # Currently we expect these to be the /same/ object.
        assert frame is rframe

        assert not store.has_frames(dest)
        assert store.size(dest) == 0
