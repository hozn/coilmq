"""Test of the QueueManager when using a DBM backend (store)."""

from pathlib import Path

import pytest

from coilmq.store.dbm import DbmQueue
from tests.queue_manager import QueueManagerTests

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
def store(tmp_path: Path) -> DbmQueue:
    """Returns the configured :class:`QueueStore` instance to use.

    Can be overridden by subclasses that wish to change out any queue store parameters.

    :rtype: QueueStore
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    return DbmQueue(
        data_dir,
        checkpoint_operations=100,
        checkpoint_timeout=20,
    )


class TestQueueManagerDbmQueue(QueueManagerTests):
    """Run all the tests from BasicTest using a :class:`DbmQueue` database store."""
