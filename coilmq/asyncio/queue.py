"""
Queue manager, queue implementation, and supporting classes.

This code is inspired by the design of the Ruby stompserver project, by 
Patrick Hurley and Lionel Bouton.  See http://stompserver.rubyforge.org/
"""
import logging
import uuid
from collections import defaultdict

from coilmq.server import StompConnection
from coilmq.scheduler import FavorReliableSubscriberScheduler, RandomQueueScheduler, QueuePriorityScheduler, SubscriberPriorityScheduler
from coilmq.asyncio.store import QueueStore
from coilmq.asyncio.subscription import SubscriptionManager, Subscription, DEFAULT_SUBSCRIPTION_ID
from coilmq.util.frames import MESSAGE, Frame

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



class QueueManager(object):
    """
    Class that manages distribution of messages to queue subscribers.

    This class uses C{threading.RLock} to guard the public methods.  This is probably
    a bit excessive, given 1) the actomic nature of basic C{dict} read/write operations 
    and  2) the fact that most of the internal data structures are keying off of the 
    STOMP connection, which is going to be thread-isolated.  That said, this seems like 
    the technically correct approach and should increase the chance of this code being
    portable to non-GIL systems. 

    @ivar store: The queue storage backend to use.
    @type store: L{coilmq.asyncio.store.QueueStore}

    @ivar subscriber_scheduler: The scheduler that chooses which subscriber to send
                                    messages to.
    @type subscriber_scheduler: L{coilmq.scheduler.SubscriberPriorityScheduler}

    @ivar queue_scheduler: The scheduler that chooses which queue to select for sending
                                    backlogs for a single connection.
    @type queue_scheduler: L{coilmq.scheduler.QueuePriorityScheduler}

    @ivar _subscriptions: A dict of registered queues, keyed by destination.
    @type _subscriptions: L{coilmq.asyncio.subscription.SubscriptionManager}

    @ivar _pending: All messages waiting for ACK from clients.
    @type _pending: C{dict} of L{coilmq.asyncio.subscription.SubscriptionManager} to C{stompclient.frame.Frame}

    @ivar _transaction_frames: Frames that have been ACK'd within a transaction.
    @type _transaction_frames: C{dict} of L{coilmq.asyncio.subscription.Subscription} to C{dict} of C{str} to C{stompclient.frame.Frame}
    """

    def __init__(
            self,
            store: QueueStore,
            subscriber_scheduler: SubscriberPriorityScheduler = None,
            queue_scheduler: QueuePriorityScheduler = None,
    ):
        """
        @param store: The queue storage backend.
        @type store: L{coilmq.asyncio.store.QueueStore}

        @param subscriber_scheduler: The scheduler that chooses which subscriber to send
                                    messages to.
        @type subscriber_scheduler: L{coilmq.scheduler.SubscriberPriorityScheduler}

        @param queue_scheduler: The scheduler that chooses which queue to select for sending
                                    backlogs for a single connection.
        @type queue_scheduler: L{coilmq.scheduler.QueuePriorityScheduler}
        """
        self.log = logging.getLogger(
            '%s.%s' % (__name__, self.__class__.__name__))

        # Use default schedulers, if they're not specified
        if subscriber_scheduler is None:
            subscriber_scheduler = FavorReliableSubscriberScheduler()

        if queue_scheduler is None:
            queue_scheduler = RandomQueueScheduler()

        # This lock var is required by L{synchronized} decorator.

        self.store = store
        self.subscriber_scheduler = subscriber_scheduler
        self.queue_scheduler = queue_scheduler

        self._subscriptions = SubscriptionManager()
        self._transaction_frames = defaultdict(lambda: defaultdict(list))
        self._pending = {}

    async def close(self):
        """
        Closes all resources/backends associated with this queue manager.
        """
        self.log.info("Shutting down queue manager.")
        if hasattr(self.store, 'close'):
            await self.store.close()

        if hasattr(self.subscriber_scheduler, 'close'):
            self.subscriber_scheduler.close()

        if hasattr(self.queue_scheduler, 'close'):
            self.queue_scheduler.close()

    async def subscriber_count(self, destination: str = None):
        """
        Returns a count of the number of subscribers.

        If destination is specified then it only returns count of subscribers 
        for that specific destination.

        @param destination: The optional topic/queue destination (e.g. '/queue/foo')
        @type destination: C{str}

        @return: The number of subscribers.
        @rtype: C{int}
        """
        return await self._subscriptions.subscriber_count(destination=destination)

    async def subscribe(self, connection: StompConnection, destination: str, id: str = None):
        """
        Subscribes a connection to the specified destination (topic or queue). 

        @param connection: The connection to subscribe.
        @type connection: L{coilmq.asyncio.server.StompConnection}

        @param destination: The topic/queue destination (e.g. '/queue/foo')
        @type destination: C{str}

        @param id: subscription identifier (optional)
        @type id: C{str}
        """
        self.log.debug("Subscribing %s to %s" % (connection, destination))
        subscription = await self._subscriptions.subscribe(connection, destination, id=id)
        await self._send_backlog(subscription, destination)

    async def unsubscribe(self, connection:StompConnection, destination: str, id: str = None):
        """
        Unsubscribes a connection from a destination (topic or queue).

        @param connection: The client connection to unsubscribe.
        @type connection: L{coilmq.asyncio.server.StompConnection}

        @param destination: The topic/queue destination (e.g. '/queue/foo') (optional)
        @type destination: C{str}

        @param id: subscription identifier (optional)
        @type id: C{str}
        """
        if id and not destination:
            self.log.debug("Unsubscribing %s for id %s" % (connection, id))
        else:
            self.log.debug("Unsubscribing %s from %s" % (connection, destination))
        await self._subscriptions.unsubscribe(connection, destination, id=id)

    async def disconnect(self, connection: StompConnection):
        """
        Removes a subscriber connection, ensuring that any pending commands get requeued.

        @param connection: The client connection to unsubscribe.
        @type connection: L{coilmq.asyncio.server.StompConnection}
        """
        self.log.debug("Disconnecting %s" % connection)
        for subscription, pending_frame in list(self._pending.items()):
            if subscription.connection == connection:
                await self.store.requeue(pending_frame.headers.get(
                    'destination'), pending_frame)
                del self._pending[subscription]
        await self._subscriptions.disconnect(connection)

    async def send(self, message: Frame):
        """
        Sends a MESSAGE frame to an eligible subscriber connection.

        Note that this method will modify the incoming message object to 
        add a message-id header (if not present) and to change the command
        to 'MESSAGE' (if it is not).

        @param message: The message frame.
        @type message: C{coilmq.util.frames.Frame}
        """
        dest = message.headers.get('destination')
        if not dest:
            raise ValueError(
                "Cannot send frame with no destination: %s" % message)

        message.cmd = MESSAGE

        message.headers.setdefault('message-id', str(uuid.uuid4()))

        # Grab all subscribers for this destination that do not have pending
        # frames
        subscribers = [s for s in await self._subscriptions.subscribers(dest)
                       if s not in self._pending]

        if not subscribers:
            self.log.debug(
                "No eligible subscribers; adding message %s to queue %s" % (message, dest))
            await self.store.enqueue(dest, message)
        else:
            selected = self.subscriber_scheduler.choice(subscribers, message)
            self.log.debug("Delivering message %s to subscriber %s" %
                           (message, selected))
            await self._send_frame(selected, message)

    async def ack(self, connection: StompConnection, frame: Frame, transaction: str = None, id: str = None):
        """
        Acknowledge receipt of a message.

        If the `transaction` parameter is non-null, the frame being ack'd
        will be queued so that it can be requeued if the transaction
        is rolled back. 

        @param connection: The connection that is acknowledging the frame.
        @type connection: L{coilmq.asyncio.server.StompConnection}

        @param frame: The frame being acknowledged.

        """
        self.log.debug("ACK %s for %s" % (frame, connection))

        subscription = Subscription.factory(connection=connection, id=id)
        pending_frame = self._pending.get(subscription, None)

        if pending_frame is not None:
            # Make sure that the frame being acknowledged matches
            # the expected frame
            message_id = frame.headers.get('message-id')
            if pending_frame.headers.get('message-id') != message_id:
                self.log.warning(
                    "Got a ACK for unexpected message-id: %s", message_id)
                await self.store.requeue(pending_frame.destination, pending_frame)
                # (The pending frame will be removed further down)

            if transaction is not None:
                self._transaction_frames[subscription][transaction].append(pending_frame)

            self._pending.pop(subscription)
            await self._send_backlog(subscription)
        else:
            self.log.debug("No pending messages for %s" % subscription)

    async def resend_transaction_frames(self, connection: StompConnection, transaction: str):
        """
        Resend the messages that were ACK'd in specified transaction.

        This is called by the engine when there is an abort command.

        @param connection: The client connection that aborted the transaction.
        @type connection: L{coilmq.asyncio.server.StompConnection}

        @param transaction: The transaction id (which was aborted).
        @type transaction: C{str}
        """
        for subscription, frames in self._transaction_frames.items():
            if subscription.connection == connection:
                for frame in frames[transaction]:
                    await self.send(frame)

    async def clear_transaction_frames(self, connection: StompConnection, transaction: str):
        """
        Clears out the queued ACK frames for specified transaction. 

        This is called by the engine when there is a commit command.

        @param connection: The client connection that committed the transaction.
        @type connection: L{coilmq.asyncio.server.StompConnection}

        @param transaction: The transaction id (which was committed).
        @type transaction: C{str}
        """
        for subscription, frames in self._transaction_frames.items():
            if subscription.connection == connection:
                try:
                    del frames[transaction]
                except KeyError:
                    # There may not have been any ACK frames for this transaction.
                    pass

    async def _send_backlog(self, subscription: Subscription, destination: str = None):
        """
        Sends any queued-up messages for the (optionally) specified destination to subscription.

        If the destination is not provided, a destination is chosen using the 
        L{QueueManager.queue_scheduler} scheduler algorithm.

        (This method assumes it is being called from within a lock-guarded public
        method.)  

        @param subscription: The client subscription.
        @type subscription: L{coilmq.asyncio.subscription.Subscription}

        @param destination: The topic/queue destination (e.g. '/queue/foo')
        @type destination: C{str} 

        @raise Exception: if the underlying connection object raises an error, the message
                            will be re-queued and the error will be re-raised.  
        """
        if destination is None:
            # Find all destinations that have frames and that contain this subscription.
            eligible_subscriptions = dict((dest, s) for (dest, s) in await self._subscriptions.all_subscribers()
                                    if subscription in s and self.store.has_frames(dest))
            destination = self.queue_scheduler.choice(
                eligible_subscriptions,
                subscription,
            )
            if destination is None:
                self.log.debug(
                    "No eligible queues (with frames) for subscriber %s" % subscription)
                return

        self.log.debug("Sending backlog to %s for destination %s" %
                       (subscription, destination))
        if subscription.connection.reliable_subscriber:
            # only send one message (waiting for ack)
            frame = self.store.dequeue(destination)
            if frame:
                try:
                    await self._send_frame(subscription, frame)
                except Exception as x:
                    self.log.error(
                        "Error sending message %s (requeueing): %s" % (frame, x))
                    await self.store.requeue(destination, frame)
                    raise
        else:
            for frame in await self.store.frames(destination):
                try:
                    await self._send_frame(subscription, frame)
                except Exception as x:
                    self.log.error(
                        "Error sending message %s (requeueing): %s" % (frame, x))
                    await self.store.requeue(destination, frame)
                    raise

    async def _send_frame(self, subscription: Subscription, frame: Frame):
        """
        Sends a frame to a specific subscription.

        (This method assumes it is being called from within a lock-guarded public
        method.)

        @param subscription: The subscriber connection object to send to.
        @type subscription: L{coilmq.asyncio.subscription.Subscription}

        @param frame: The frame to send.
        @type frame: L{coilmq.util.frames.Frame}
        """
        assert subscription is not None
        assert frame is not None

        if frame.cmd == MESSAGE and subscription.id != DEFAULT_SUBSCRIPTION_ID:
            frame.headers["subscription"] = subscription.id

        self.log.debug("Delivering frame %s to subscription %s" %
                       (frame, subscription))

        if subscription.connection.reliable_subscriber:
            if subscription in self._pending:
                raise RuntimeError("Connection already has a pending frame.")
            self.log.debug(
                "Tracking frame %s as pending for subscription %s" % (frame, subscription))
            self._pending[subscription] = frame

        await subscription.connection.send_frame(frame)
