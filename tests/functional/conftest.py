from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Generator

import pytest

from coilmq.protocol import STOMP10
from coilmq.queue import QueueManager
from coilmq.scheduler import FavorReliableSubscriberScheduler, RandomQueueScheduler
from coilmq.server.socket_server import ThreadedStompServer
from coilmq.store.memory import MemoryQueue
from coilmq.topic import TopicManager
from tests.functional import Client

if TYPE_CHECKING:
    from coilmq.auth import Authenticator
    from coilmq.protocol import STOMP
    from coilmq.store import QueueStore


@pytest.fixture
def store() -> MemoryQueue:
    """Returns the :func:`server` fixture's ``store`` argument."""
    return MemoryQueue()


@pytest.fixture
def protocol_class() -> type[STOMP]:
    """Returns the :func:`server` fixture's ``protocol_class`` argument."""
    return STOMP10


@pytest.fixture
def server(
    request: pytest.FixtureRequest,
    store: QueueStore,
    protocol_class: type[STOMP],
) -> Generator[ThreadedStompServer, None, None]:
    """Returns a :class:`ThreadedStompServer` instance.

    This function creates a new server and runs it in a thread.  The default ``store``
    fixture returns an ephemeral, memory-backed queue store.

    :param request: if a test needs to provide a :class:`Authenticator`
        instance, pass it as ``request.param`` via indirect parametrization
    :param store: is a fixture-provided :class:`QueueStore` instance

    """
    authenticator: Authenticator | None = (
        request.param if hasattr(request, "param") else None
    )

    server = ThreadedStompServer(
        ("127.0.0.1", 0),
        authenticator=authenticator,
        queue_manager=QueueManager(
            store,
            FavorReliableSubscriberScheduler(),
            RandomQueueScheduler(),
        ),
        topic_manager=TopicManager(),
        protocol=protocol_class,
    )
    thread = threading.Thread(target=server.serve_forever, name="server")
    thread.start()

    try:
        yield server
    finally:
        server.server_close()
        thread.join()


@pytest.fixture
def server_addr(server: ThreadedStompServer) -> tuple[str, int]:
    """Returns ``(host, port)`` for the server started by :func:`server`."""
    return server.socket.getsockname()


@pytest.fixture
def c1(server_addr: tuple[str, int]) -> Generator[Client, None, None]:
    """Returns a :class:`Client` instance."""
    with Client(server_addr) as client:
        yield client


@pytest.fixture
def c2(server_addr: tuple[str, int]) -> Generator[Client, None, None]:
    """Returns a :class:`Client` instance.

    This fixture is useful if a test needs a second client.

    """
    with Client(server_addr) as client:
        yield client


@pytest.fixture
def c3(server_addr: tuple[str, int]) -> Generator[Client, None, None]:
    """Returns a :class:`Client` instance.

    This fixture is useful if a test needs a third client.

    """
    with Client(server_addr) as client:
        yield client
