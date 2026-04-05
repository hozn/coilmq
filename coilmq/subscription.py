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
    def factory(cls, connection, id=None):
        """:param connection: The connection to subscribe.
        :type connection: coilmq.server.StompConnection
        :param id: The subscription identifier (introduced in STOMP 1.1).
        :type id: typing.Any

        :returns: The subscription.
        :rtype: Subscription
        """
        if id is None:
            id = DEFAULT_SUBSCRIPTION_ID
        return cls(connection=connection, id=id)


class SubscriptionManager:
    """Manage subscriptions associated with connections."""

    def __init__(self):
        """:var _subscriptions: A dict of registered subscriptions, keyed by destination.
        :vartype _subscriptions: dict[str, set[coilmq.subscription.Subscription]].
        """
        self._subscriptions = defaultdict(set)

    def subscribe(self, connection, destination, id=None):
        """Subscribes a connection to the specified destination.

        :param destination: The destination (e.g. '/queue/foo')
        :type destination: str
        :param connection: The connection to subscribe.
        :type connection: coilmq.server.StompConnection
        :param id: subscription identifier (optional)
        :type id: int

        :returns: The subscription.
        :rtype: Subscription
        """
        if id is None:
            id = DEFAULT_SUBSCRIPTION_ID
        subscription = Subscription(connection=connection, id=id)
        self._subscriptions[destination].add(subscription)
        return subscription

    def unsubscribe(self, connection, destination, id=None):
        """Unsubscribes a connection from a destination.

        :param connection: The client connection to unsubscribe.
        :type connection: coilmq.server.StompConnection
        :param destination: The topic/queue destination (e.g. '/queue/foo')
        :type destination: str
        :param id: subscription identifier (optional)
        :type id: int

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
        """Removes a client connection.

        :param connection: The client connection to unsubscribe.
        :type connection: coilmq.server.StompConnection
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
        """Returns a count of the number of subscribers.

        If destination is specified then it only returns count of subscribers
        for that specific destination.

        :param destination: The optional topic/queue destination (e.g. '/queue/foo')
        :type destination: str

        :returns: The number of subscribers.
        :rtype: int
        """
        if destination:
            return len(self._subscriptions.get(destination, set()))
        else:
            return sum(map(len, self._subscriptions.values()))

    def subscribers(self, destination):
        """Returns subscribers to a single destination.

        :param destination: The optional topic/queue destination (e.g. '/queue/foo')
        :type destination: str

        :returns: The subscribers.
        :rtype: set
        """
        return self._subscriptions.get(destination, set())

    def all_subscribers(self):
        """Yields all subscribers."""
        yield from itertools.chain(self._subscriptions.items())
