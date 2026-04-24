from typing import Generator

import pytest

from coilmq.queue import QueueManager
from coilmq.store import QueueStore
from tests.mock import MockConnection


@pytest.fixture
def queue_manager(store: QueueStore) -> Generator[QueueManager, None, None]:
    queue_manager = QueueManager(store)

    try:
        yield queue_manager
    finally:
        queue_manager.close()


@pytest.fixture
def conn() -> MockConnection:
    return MockConnection()
