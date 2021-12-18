import itertools
from dataclasses import dataclass
from collections import defaultdict
from typing import Any
from coilmq.server import StompConnection

DEFAULT_SUBSCRIPTION_ID = 0


@dataclass(frozen=True)
class Subscription:
    connection: StompConnection
    id: Any

    @classmethod
    def from_frame(cls, frame, connection):
        """
        @param frame: STOMP frame.
        @type frame: L{coilmq.util.frames.Frame}

        @param connection: The connection to subscribe.
        @type connection: L{coilmq.server.StompConnection}

        @return: The subscription.
        @rtype: C{Subscription}
        """
        id = frame.headers.get("id", DEFAULT_SUBSCRIPTION_ID)
        return cls(connection=connection, id=id)


class SubscriptionManager:
    def __init__(self):
        """
        @ivar _subscriptions: A dict of registered subscriptions, keyed by destination.
        @type _subscriptions: C{dict} of C{str} to C{set} of L{coilmq.subscription.Subscription}
        """
        self._subscriptions = defaultdict(set)

    def subscribe(self, connection, destination, id=None):
        """
        Subscribes a connection to the specified destination.

        @param destination: The destination (e.g. '/queue/foo')
        @type destination: C{str}

        @param connection: The connection to subscribe.
        @type connection: L{coilmq.server.StompConnection}

        @param id: subscription identifier (optional)
        @type id: C{int}

        @return: The subscription.
        @rtype: C{Subscription}
        """
        if id is None:
            id = DEFAULT_SUBSCRIPTION_ID
        subscription = Subscription(connection=connection, id=id)
        self._subscriptions[destination].add(subscription)
        return subscription

    def unsubscribe(self, connection, destination, id=None):
        """
        Unsubscribes a connection from a destination.

        @param connection: The client connection to unsubscribe.
        @type connection: L{coilmq.server.StompConnection}

        @param destination: The topic/queue destination (e.g. '/queue/foo')
        @type destination: C{str}

        @param id: subscription identifier (optional)
        @type id: C{int}
        """
        subscriptions = self._subscriptions.get(destination, None)
        if subscriptions is None:
            return
        if id is None:
            id = DEFAULT_SUBSCRIPTION_ID
        subscription = Subscription(connection=connection, id=id)
        try:
            subscriptions.remove(subscription)
        except KeyError:
            pass
        if not subscriptions:
            del self._subscriptions[destination]

    def disconnect(self, connection):
        """
        Removes a client connection.

        @param connection: The client connection to unsubscribe.
        @type connection: L{coilmq.server.StompConnection}
        """
        for destination, subscriptions in list(self._subscriptions.items()):
            subscriptions = {
                subscription
                for subscription in subscriptions
                if subscription.connection != connection
            }
            if subscriptions:
                self._subscriptions[destination] = subscriptions
            else:
                del self._subscriptions[destination]

    def subscriber_count(self, destination=None):
        """
        Returns a count of the number of subscribers.

        If destination is specified then it only returns count of subscribers
        for that specific destination.

        @param destination: The optional topic/queue destination (e.g. '/queue/foo')
        @type destination: C{str}

        @return: The number of subscribers.
        @rtype: C{int}
        """
        if destination:
            return len(self._subscriptions.get(destination, set()))
        else:
            return sum(map(len, self._subscriptions.values()))

    def subscribers(self, destination):
        """
        Returns subscribers to a single destination.

        @param destination: The optional topic/queue destination (e.g. '/queue/foo')
        @type destination: C{str}

        @return: The subscribers.
        @rtype: C{set}
        """
        return self._subscriptions.get(destination, set())

    def all_subscribers(self):
        """
        Yields all subscribers.
        """
        yield from itertools.chain(self._subscriptions.items())
