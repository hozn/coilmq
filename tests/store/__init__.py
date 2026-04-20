"""Queue storage tests."""

import uuid

from coilmq.store import QueueStore
from coilmq.util import frames
from coilmq.util.frames import Frame

__authors__ = ['"Hans Lellelid" <hans@xmpl.org>']
__copyright__ = "Copyright 2009 Hans Lellelid"
__license__ = """Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""


class BaseQueueTests:
    """An abstract set of base tests for queue storage engines."""

    def test_enqueue(self, store: QueueStore) -> None:
        """Test the enqueue() method."""
        dest = "/queue/foo"
        frame = Frame(
            frames.MESSAGE, headers={"message-id": str(uuid.uuid4())}, body="some data"
        )
        store.enqueue(dest, frame)

        assert store.has_frames(dest)
        assert store.size(dest) == 1

    def test_dequeue(self, store: QueueStore) -> None:
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

        # We cannot generically assert whether or not frame and rframe are
        # the *same* object.

        assert not store.has_frames(dest)
        assert store.size(dest) == 0

    def test_dequeue_specific(self, store: QueueStore) -> None:
        """Test that we only dequeue from the correct queue."""
        dest = "/queue/foo"
        notdest = "/queue/other"

        frame1 = Frame(
            frames.MESSAGE, headers={"message-id": str(uuid.uuid4())}, body="message-1"
        )
        store.enqueue(dest, frame1)

        frame2 = Frame(
            frames.MESSAGE, headers={"message-id": str(uuid.uuid4())}, body="message-2"
        )
        store.enqueue(notdest, frame2)

        frame3 = Frame(
            frames.MESSAGE, headers={"message-id": str(uuid.uuid4())}, body="message-3"
        )
        store.enqueue(dest, frame3)

        assert store.has_frames(dest)
        assert store.size(dest) == 2

        rframe1 = store.dequeue(dest)
        assert frame1 == rframe1

        rframe2 = store.dequeue(dest)
        assert frame3 == rframe2

        assert not store.has_frames(dest)
        assert store.size(dest) == 0

    def test_dequeue_order(self, store: QueueStore) -> None:
        """Test the order that frames are returned by dequeue() method."""
        dest = "/queue/foo"

        frame1 = Frame(
            frames.MESSAGE, headers={"message-id": str(uuid.uuid4())}, body="message-1"
        )
        store.enqueue(dest, frame1)

        frame2 = Frame(
            frames.MESSAGE, headers={"message-id": str(uuid.uuid4())}, body="message-2"
        )
        store.enqueue(dest, frame2)

        frame3 = Frame(
            frames.MESSAGE, headers={"message-id": str(uuid.uuid4())}, body="message-3"
        )
        store.enqueue(dest, frame3)

        assert store.has_frames(dest)
        assert store.size(dest) == 3

        rframe1 = store.dequeue(dest)
        assert frame1 == rframe1

        rframe2 = store.dequeue(dest)
        assert frame2 == rframe2

        rframe3 = store.dequeue(dest)
        assert frame3 == rframe3

        assert not store.has_frames(dest)
        assert store.size(dest) == 0

    def test_dequeue_empty(self, store: QueueStore) -> None:
        """Test dequeue() with empty queue."""
        assert store.dequeue("/queue/nonexist") is None

        assert not store.has_frames("/queue/nonexist")
        assert store.size("/queue/nonexist") == 0
