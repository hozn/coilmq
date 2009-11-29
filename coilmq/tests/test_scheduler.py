"""
Tests for the scheduler implementation.
"""
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