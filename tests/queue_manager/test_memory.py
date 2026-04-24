import pytest

from coilmq.store.memory import MemoryQueue
from tests.queue_manager import QueueManagerTests


@pytest.fixture
def store() -> MemoryQueue:
    return MemoryQueue()


class TestQueueManagerMemoryQueue(QueueManagerTests):
    pass
