"""
Tests for the scheduler implementation.
"""
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
import unittest


from coilmq.scheduler import FavorReliableSubscriberScheduler
from coilmq.tests.mock import MockConnection

class QueueDeliverySchedulerTest(unittest.TestCase):
    """ Tests for various message delivery schedulers. """
     
    def test_favorReliable(self):
        """ Test the favor reliable delivery scheduler. """
        
        sched = FavorReliableSubscriberScheduler()
        
        conn1 = MockConnection()
        conn1.reliable_subscriber = True
        
        conn2 = MockConnection()
        conn2.reliable_subscriber = False
        
        choice = sched.choice((conn1, conn2), None)
        
        assert choice is conn1, "Expected reliable connection to be selected."