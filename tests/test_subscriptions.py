"""
Tests for the subscription management.
"""
import unittest

from coilmq.subscription import SubscriptionManager
from tests.mock import MockConnection


class SubscriptionManagerTest(unittest.TestCase):
    """Tests for the subscription manager."""

    def test_subscribing(self):
        """Test (un)subscribing."""

        subscriptions = SubscriptionManager()
        conn1 = MockConnection()
        conn2 = MockConnection()

        for _ in range(2):
            subscriptions.subscribe(conn1, "dest1", id=1)
            subscriptions.subscribe(conn1, "dest1", id=2)
            subscriptions.subscribe(conn2, "dest1", id=1)
            subscriptions.subscribe(conn1, "dest2", id=1)
            subscriptions.subscribe(conn1, "dest2", id=2)
            subscriptions.subscribe(conn2, "dest2", id=1)

        self.assertEqual(subscriptions.subscriber_count("dest1"), 3)
        self.assertEqual(subscriptions.subscriber_count("dest2"), 3)
        self.assertEqual(subscriptions.subscriber_count(), 6)

        subscriptions.unsubscribe(conn1, "dest2", id=2)

        self.assertEqual(subscriptions.subscriber_count("dest1"), 3)
        self.assertEqual(subscriptions.subscriber_count("dest2"), 2)
        self.assertEqual(subscriptions.subscriber_count(), 5)

        subscriptions.disconnect(conn2)
        self.assertEqual(subscriptions.subscriber_count("dest1"), 2)
        self.assertEqual(subscriptions.subscriber_count("dest2"), 1)
        self.assertEqual(subscriptions.subscriber_count(), 3)
