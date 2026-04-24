"""Test DBM queue storage."""

import time
import uuid
from contextlib import closing
from pathlib import Path
from typing import Generator

import pytest

from coilmq.store import QueueStore
from coilmq.store.dbm import DbmQueue
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
def store(tmp_path: Path) -> Generator[QueueStore, None, None]:
    with closing(DbmQueue(tmp_path)) as queue:
        yield queue


class TestDbmQueue(BaseQueueTests):
    def test_dequeue_identity(self, store: QueueStore) -> None:
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
        assert frame is not rframe

        assert not store.has_frames(dest)
        assert store.size(dest) == 0

    @pytest.mark.xfail(reason="https://github.com/hozn/coilmq/issues/41")
    def test_sync_checkpoint_ops(self, tmp_path: Path) -> None:
        """Test a expected sync behavior with checkpoint_operations param."""
        max_ops = 5
        with closing(DbmQueue(tmp_path, checkpoint_operations=max_ops)) as store:
            dest = "/queue/foo"

            for i in range(max_ops + 1):
                frame = Frame(
                    frames.MESSAGE,
                    headers={"message-id": str(uuid.uuid4())},
                    body=f"some data - {i}",
                )
                store.enqueue(dest, frame)

            assert store.size(dest) == max_ops + 1

            # No close()!

            with closing(DbmQueue(tmp_path)) as store2:
                assert store2.size(dest) == max_ops + 1

    @pytest.mark.xfail(reason="https://github.com/hozn/coilmq/issues/41")
    def test_sync_checkpoint_timeout(self, tmp_path: Path) -> None:
        """Test a expected sync behavior with checkpoint_timeout param."""
        with closing(DbmQueue(tmp_path, checkpoint_timeout=0.5)) as store:
            dest = "/queue/foo"

            frame = Frame(
                frames.MESSAGE,
                headers={"message-id": str(uuid.uuid4())},
                body="some data -1",
            )
            store.enqueue(dest, frame)

            time.sleep(0.5)

            frame = Frame(
                frames.MESSAGE,
                headers={"message-id": str(uuid.uuid4())},
                body="some data -2",
            )
            store.enqueue(dest, frame)

            assert store.size(dest) == 2

            # No close()!

            with closing(DbmQueue(tmp_path)) as store2:
                assert store2.size(dest) == 2

    def test_sync_close(self, tmp_path: Path) -> None:
        """Test a expected sync behavior of close() call."""
        with closing(DbmQueue(tmp_path)) as store:
            dest = "/queue/foo"
            frame = Frame(
                frames.MESSAGE,
                headers={"message-id": str(uuid.uuid4())},
                body="some data",
            )
            store.enqueue(dest, frame)
            assert store.size(dest) == 1

        with closing(DbmQueue(tmp_path)) as store2:
            assert store2.size(dest) == 1

    @pytest.mark.xfail(reason="https://github.com/hozn/coilmq/issues/41")
    def test_sync_loss(self, tmp_path: Path, store: DbmQueue) -> None:
        """Test metadata loss behavior."""
        dest = "/queue/foo"
        frame = Frame(
            frames.MESSAGE,
            headers={"message-id": str(uuid.uuid4())},
            body="some data",
        )
        store.enqueue(dest, frame)
        assert store.size(dest) == 1

        with closing(DbmQueue(tmp_path)) as store2:
            assert store2.size(dest) == 0
