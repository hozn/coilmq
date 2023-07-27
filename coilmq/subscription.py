import itertools
from dataclasses import dataclass
from collections import defaultdict
from typing import Any
from coilmq.server import StompConnection

DEFAULT_SUBSCRIPTION_ID = 'coilmq_default'


@dataclass(frozen=True)
class Subscription:
    connection: StompConnection
    id: str

    @classmethod
    def factory(cls, connection: StompConnection, id: str = None):
        """
        @param connection: The connection to subscribe.
        @type connection: L{coilmq.server.StompConnection}

        @param id: The subscription identifier (optional in STOMP 1.0, required in 1.1+).
        @type id: C{str}

        @return: The subscription.
        @rtype: C{Subscription}
        """
        if id is None:
            id = DEFAULT_SUBSCRIPTION_ID
        return cls(connection=connection, id=id)


class SubscriptionManager:
    def __init__(self):
        """
        @ivar _subscriptions: A dict of registered subscriptions, keyed by destination.
        @type _subscriptions: C{dict} of C{str} to C{set} of L{coilmq.subscription.Subscription}
        """
        self._subscriptions = defaultdict(set)
        self._id_destinations: dict = {}  # For lookup if subscribing with an id

    def destination_for_id(self, id: str):
        return self._id_destinations.get(id)

    def subscribe(self, connection: StompConnection, destination: str, id: str = None):
        """
        Subscribes a connection to the specified destination.

        @param connection: The connection to subscribe.
        @type connection: L{coilmq.server.StompConnection}

        @param destination: The destination (e.g. '/queue/foo')
        @type destination: C{str}

        @param id: subscription identifier (optional)
        @type id: C{str}

        @return: The subscription.
        @rtype: C{Subscription}
        """
        if id is None:
            id = DEFAULT_SUBSCRIPTION_ID
        else:
            self._id_destinations[id] = destination
        subscription = Subscription(connection=connection, id=id)
        self._subscriptions[destination].add(subscription)
        return subscription

    def unsubscribe(self, connection: StompConnection, destination: str = None, id: str = None):
        """
        Unsubscribes a connection from a destination.

        @param connection: The client connection to unsubscribe.
        @type connection: L{coilmq.server.StompConnection}

        @param destination: The topic/queue destination (e.g. '/queue/foo') (optional)
        @type destination: C{str}

        @param id: subscription identifier (optional)
        @type id: C{str}
        """
        if id and not destination:
            destination = self._id_destinations.get(id)

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

    def disconnect(self, connection: StompConnection):
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

    def subscriber_count(self, destination: str = None) -> int:
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

    def subscribers(self, destination: str):
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
