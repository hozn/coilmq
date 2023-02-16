"""
Tests for the scheduler implementation.
"""
import unittest

from coilmq.scheduler import FavorReliableSubscriberScheduler
from tests.mock import MockSubscription

__authors__ = ['"Hans Lellelid" <hans@xmpl.org>']
__copyright__ = "Copyright 2009 Hans Lellelid"
__license__ = """Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""


class QueueDeliverySchedulerTest(unittest.TestCase):
    """ Tests for various message delivery schedulers. """

    def test_favorReliable(self):
        """ Test the favor reliable delivery scheduler. """

        sched = FavorReliableSubscriberScheduler()

        sub1 = MockSubscription()
        sub1.connection.reliable_subscriber = True

        sub2 = MockSubscription()
        sub2.connection.reliable_subscriber = False

        choice = sched.choice((sub1, sub2), None)

        self.assertIs(
            choice, sub1, "Expected reliable connection to be selected.")
