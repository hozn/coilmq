"""Queue storage module that stores the queue information and frames in a DBM-style database.

The current implementation uses :py:mod:`shelve` which, in turn, uses a DBM implementation
under the hood from :py:mod:`dbm`.

Because of how the :py:mod:`shelve` module works (and how we're using it) and caveats in the Python
documentation this is likely a BAD storage module to use if you are expecting to traffic in
large frames.
"""
import os
import os.path
import shelve
import threading
from collections import deque
from configparser import NoOptionError
from datetime import datetime, timedelta

from coilmq.config import config
from coilmq.exception import ConfigError
from coilmq.store import QueueStore
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


def make_dbm():
    """Creates a DBM queue store, pulling config values from the CoilMQ configuration."""
    try:
        data_dir = config.get('coilmq', 'qstore.dbm.data_dir')
        cp_ops = config.getint('coilmq', 'qstore.dbm.checkpoint_operations')
        cp_timeout = config.getint('coilmq', 'qstore.dbm.checkpoint_timeout')
    except NoOptionError as e:
        raise ConfigError(f'Missing configuration parameter: {e}') from e

    if not os.path.exists(data_dir):
        raise ConfigError(f'DBM directory does not exist: {data_dir}')
    # FIXME: how do these get applied? Is OR appropriate?
    if not os.access(data_dir, os.W_OK | os.R_OK):
        raise ConfigError(f'Cannot read and write DBM directory: {data_dir}')

    store = DbmQueue(data_dir, checkpoint_operations=cp_ops,
                     checkpoint_timeout=cp_timeout)
    return store


class DbmQueue(QueueStore):
    """A QueueStore implementation that stores messages and queue information in DBM-style
    database.

    Several database files will be used to support this functionality: metadata about the
    queues will be stored in its own database and each queue will also have its own
    database file.

    This class uses a :py:class:`threading.RLock` to guard access to the memory store, since it
    appears that at least some of the underlying implementations that :py:mod:`dbm` uses are not
    thread-safe.

    Due to some impedance mismatch between the types of data we need to store in queues
    (specifically lists) and the types of data that are best stored in DBM databases
    (specifically dicts), this class uses :py:mod:`shelve` to abstract away some
    of the ugliness.  The consequence of this is that we only persist objects periodically
    to the datastore, for performance reasons.  How periodic is determined by the
    :paramref:`checkpoint_operations` and :paramref:`checkpoint_timeout` instance variables.

    :var data_dir: The directory where DBM files will be stored.
    :vartype data_dir: str
    :var queue_metadata: A Shelf (DBM) database that tracks stats & delivered message
        ids for all the queues.
    :vartype queue_metadata: shelve.Shelf
    :var frame_store: A Shelf (DBM) database that contains frame contents indexed by
        message id.
    :vartype frame_store: shelve.Shelf
    :var _opcount: Internal counter for keeping track of unpersisted operations.
    :vartype _opcount: int
    :var checkpoint_operations: Number of operations between syncs.
    :vartype checkpoint_operations: int
    :var checkpoint_timeout: Max time (in seconds) that can elapse between sync of
        cache.
    :vartype checkpoint_timeout: datetime.timedelta

    """

    def __init__(self, data_dir, checkpoint_operations=100, checkpoint_timeout=30):
        """:param data_dir: :py:class:`str`
        :param checkpoint_operations: Number of operations between syncs.
        :type checkpoint_operations: int
        :param checkpoint_timeout: Max time (in seconds) that can elapse between sync of
            cache.
        :type checkpoint_timeout: float

        """
        QueueStore.__init__(self)

        self._opcount = 0
        self._last_sync = datetime.now()

        self.data_dir = data_dir
        self.checkpoint_operations = checkpoint_operations
        self.checkpoint_timeout = timedelta(seconds=checkpoint_timeout)

        # Should this be in constructor?

        # The queue metadata stores mutable (dict) objects.  For this reason we set
        # writeback=True and rely on the sync() method to keep the cache & disk
        # in-sync.
        self.queue_metadata = shelve.open(os.path.join(  # noqa: SIM115
            self.data_dir, 'metadata'), writeback=True)

        # Since we do not need mutable objects on the frame stores (we don't modify them, we just
        # put/get values), we do NOT use writeback=True here.  This should also conserve on memory
        # usage, since apparently that can get hefty with the caching when
        # writeback=True.
        self.frame_store = shelve.open(os.path.join(  # noqa: SIM115
            self.data_dir, 'frames'), writeback=False)

    @synchronized(lock)
    def enqueue(self, destination, frame):
        """Store message (frame) for specified destination.

        :param destination: The destination queue name for this message (frame).
        :type destination: str
        :param frame: The message (frame) to send to specified destination.
        :type frame: coilmq.util.frames.Frame

        """
        message_id = frame.headers.get('message-id')
        if not message_id:
            raise ValueError("Cannot queue a frame without message-id set.")

        if not destination in self.queue_metadata:
            self.log.info(
                "Destination %s not in metadata; creating new entry and queue database.", destination)
            self.queue_metadata[destination] = {
                'frames': deque(), 'enqueued': 0, 'dequeued': 0, 'size': 0}

        self.queue_metadata[destination]['frames'].appendleft(message_id)
        self.queue_metadata[destination]['enqueued'] += 1

        self.frame_store[message_id] = frame

        self._opcount += 1
        self._sync()

    @synchronized(lock)
    def dequeue(self, destination):
        """Removes and returns an item from the queue (or :py:obj:`None` if no items in queue).

        :param destination: The queue name (destination).
        :type destination: str

        :returns: The first frame in the specified queue, or :py:obj:`None` if there are
            none.
        :rtype: coilmq.util.frames.Frame
        """
        if not self.has_frames(destination):
            return None

        message_id = self.queue_metadata[destination]['frames'].pop()
        self.queue_metadata[destination]['dequeued'] += 1

        frame = self.frame_store[message_id]
        del self.frame_store[message_id]

        self._opcount += 1
        self._sync()

        return frame

    @synchronized(lock)
    def has_frames(self, destination):
        """Whether specified queue has any frames.

        :param destination: The queue name (destination).
        :type destination: str

        :returns: Whether there are any frames in the specified queue.
        :rtype: bool
        """
        return (destination in self.queue_metadata) and bool(self.queue_metadata[destination]['frames'])

    @synchronized(lock)
    def size(self, destination):
        """Size of the queue for specified destination.

        :param destination: The queue destination (e.g. /queue/foo)
        :type destination: str

        :returns: The number of frames in specified queue.
        :rtype: int
        """
        if not destination in self.queue_metadata:
            return 0
        else:
            return len(self.queue_metadata[destination]['frames'])

    @synchronized(lock)
    def close(self):
        """Closes the databases, freeing any resources (and flushing any unsaved changes to disk)."""
        self.queue_metadata.close()
        self.frame_store.close()

    @synchronized(lock)
    def destinations(self):
        """Provides a list of destinations (queue "addresses") available.

        :returns: A list of the destinations available.
        :rtype: set
        """
        return set(self.queue_metadata.keys())

    def _sync(self):
        """Synchronize the cached data with the underlying database.

        Uses an internal transaction counter and compares to the checkpoint_operations
        and checkpoint_timeout parameters to determine whether to persist the memory store.

        In this implementation, this method wraps calls to :py:meth:`shelve.Shelf.sync`.
        """
        if (self._opcount > self.checkpoint_operations or
                datetime.now() > self._last_sync + self.checkpoint_timeout):
            self.log.debug("Synchronizing queue metadata.")
            self.queue_metadata.sync()
            self._last_sync = datetime.now()
            self._opcount = 0
        else:
            self.log.debug("NOT synchronizing queue metadata.")
