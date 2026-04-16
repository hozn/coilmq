"""Storage containers for durable queues and (planned) durable topics."""

import abc
import logging
import threading

from coilmq.util.concurrency import synchronized

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

lock = threading.RLock()


class QueueStore(abc.ABC):
    """Abstract base class for queue storage.

    Extensions/implementations of this class must be thread-safe.

    :var log: A logger for this class.
    :vartype log: logging.Logger
    """

    def __init__(self):
        """A base constructor that sets up logging.

        If you extend this class, you should either call this method or at minimum make sure these values
        get set.
        """
        self.log = logging.getLogger(f"{self.__module__}.{self.__class__.__name__}")

    @abc.abstractmethod
    @synchronized(lock)
    def enqueue(self, destination, frame):
        """Store message (frame) for specified destination.

        :param destination: The destination queue name for this message (frame).
        :type destination: str
        :param frame: The message (frame) to send to specified destination.
        :type frame: coilmq.util.frames.Frame

        """

    @abc.abstractmethod
    @synchronized(lock)
    def dequeue(self, destination):
        """Removes and returns an item from the queue (or :py:obj:`None` if no items in queue).

        :param destination: The queue name (destination).
        :type destination: str

        :returns: The first frame in the specified queue, or :py:obj:`None` if there are
            none.
        :rtype: coilmq.util.frames.Frame
        """

    @synchronized(lock)
    def requeue(self, destination, frame):
        """Requeue a message (frame) for storing at specified destination.

        :param destination: The destination queue name for this message (frame).
        :type destination: str
        :param frame: The message (frame) to send to specified destination.
        :type frame: coilmq.util.frames.Frame

        """
        self.enqueue(destination, frame)

    @synchronized(lock)
    def size(self, destination):
        """Size of the queue for specified destination.

        :param destination: The queue destination (e.g. /queue/foo)
        :type destination: str

        :returns: The number of frames in specified queue.
        :rtype: int
        """
        raise NotImplementedError()

    @synchronized(lock)
    def has_frames(self, destination):
        """Whether specified destination has any frames.

        Default implementation uses :meth:`QueueStore.size` to determine if there
        are any frames in queue.  Subclasses may choose to optimize this.

        :param destination: The queue destination (e.g. /queue/foo)
        :type destination: str

        :returns: The number of frames in specified queue.
        :rtype: int
        """
        return self.size(destination) > 0

    @synchronized(lock)
    def destinations(self):
        """Provides a set of destinations (queue "addresses") available.

        :returns: A list of the destinations available.
        :rtype: set
        """
        raise NotImplementedError

    @synchronized(lock)
    def close(self):
        """May be implemented to perform any necessary cleanup operations when store is closed."""

    # This is intentionally not synchronized, since it does not directly
    # expose any shared data.
    def frames(self, destination):
        """Returns an iterator for frames in specified queue.

        The iterator simply wraps calls to L{dequeue} method, so the order of the
        frames from the iterator will be the reverse of the order in which the
        frames were enqueued.

        :param destination: The queue destination (e.g. /queue/foo)
        :type destination: str
        """
        return QueueFrameIterator(self, destination)


class QueueFrameIterator:
    """Provides an ``iterable`` over the frames for a specified destination in a queue.

    :var store: The queue store.
    :vartype store: coilmq.store.QueueStore
    :var destination: The destination for this iterator.
    :vartype destination: str

    """

    def __init__(self, store, destination):
        self.store = store
        self.destination = destination

    def __iter__(self):
        return self

    def __next__(self):
        frame = self.store.dequeue(self.destination)
        if not frame:
            raise StopIteration()
        return frame

    def __len__(self):
        return self.store.size(self.destination)


class TopicStore:
    """Abstract base class for non-durable topic storage."""


class DurableTopicStore(TopicStore):
    """Abstract base class for durable topic storage."""
