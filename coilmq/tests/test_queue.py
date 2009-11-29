"""
Tests for queue-related classes.
"""
import unittest
import uuid

from coilmq.frame import StompFrame
from coilmq.queue import QueueManager
from coilmq.store.memory import MemoryQueue
 
from coilmq.tests.mock import MockConnection

class QueueManagerTest(unittest.TestCase):
    """ Test the QueueManager class. """
    
    def setUp(self):
        self.store = MemoryQueue() 
        self.qm = QueueManager(self.store)
        self.conn = MockConnection()
    
    def test_subscribe(self):
        """ Test subscribing a connection to the queue. """
        dest = '/queue/dest'
        
        self.qm.subscribe(self.conn, dest)
        f = StompFrame('MESSAGE', headers={'destination': dest}, body='Empty')
        self.qm.send(f)
        
        print self.conn.frames
        assert len(self.conn.frames) == 1
        assert self.conn.frames[0] == f
    
    def test_unsubscribe(self):
        """ Test unsubscribing a connection from the queue. """
        dest = '/queue/dest'
        
        self.qm.subscribe(self.conn, dest)
        f = StompFrame('MESSAGE', headers={'destination': dest}, body='Empty')
        self.qm.send(f)
        
        print self.conn.frames
        assert len(self.conn.frames) == 1
        assert self.conn.frames[0] == f
        
        self.qm.unsubscribe(self.conn, dest)
        f = StompFrame('MESSAGE', headers={'destination': dest}, body='Empty')
        self.qm.send(f)
        
        assert len(self.conn.frames) == 1
        assert len(self.store.frames(dest)) == 1
        
    def send_simple(self):
        """ Test a basic send command. """
        dest = '/queue/dest'
        
        f = StompFrame('SEND', headers={'destination': dest}, body='Empty')
        self.qm.send(f)
        
        assert len(self.store.frames(dest)) == 1
        
        # Assert some side-effects
        assert 'message-id' in f.headers
        assert f.cmd == 'MESSAGE'
        
        
    def test_send_reliableFirst(self):
        """
        Test that messages are prioritized to reliable subscribers.
        
        This is actually a test of the underlying scheduler more than it is a test
        of the send message, per se.
        """
        
        dest = '/queue/dest'
        conn1 = MockConnection()
        conn1.reliable_subscriber = True
        
        self.qm.subscribe(conn1, dest)
        
        conn2 = MockConnection()
        conn2.reliable_subscriber = False
        self.qm.subscribe(conn2, dest)
        
        f = StompFrame('MESSAGE', headers={'destination': dest, 'message-id': uuid.uuid4()}, body='Empty')
        self.qm.send(f)
            
        print conn1.frames
        print conn2.frames
        assert len(conn1.frames) == 1
        assert len(conn2.frames) == 0
    
    def test_ack_basic(self):
        """ Test reliable client (ACK) behavior. """
        
        dest = '/queue/dest'
        conn1 = MockConnection()
        conn1.reliable_subscriber = True
        
        self.qm.subscribe(conn1, dest)
        
        m1 = StompFrame('MESSAGE', headers={'destination': dest}, body='Message body (1)')
        self.qm.send(m1)
        
        assert conn1.frames[0] == m1
        
        m2 = StompFrame('MESSAGE', headers={'destination': dest}, body='Message body (2)')
        self.qm.send(m2)
        
        assert len(conn1.frames) == 1, "Expected connection to still only have 1 frame."
        assert conn1.frames[0] == m1
        
        ack = StompFrame('ACK', headers={'destination': dest, 'message-id': m1.message_id})
        self.qm.ack(conn1, ack)
        
        print conn1.frames
        assert len(conn1.frames) == 2, "Expected 2 frames now, after ACK."
        assert conn1.frames[1] == m2
        
    def test_ack_transaction(self):
        """ Test the reliable client (ACK) behavior with transactions. """
                
        dest = '/queue/dest'
        conn1 = MockConnection()
        conn1.reliable_subscriber = True
        
        self.qm.subscribe(conn1, dest)
        
        m1 = StompFrame('MESSAGE', headers={'destination': dest, }, body='Message body (1)')
        self.qm.send(m1)
        
        assert conn1.frames[0] == m1
        
        m2 = StompFrame('MESSAGE', headers={'destination': dest}, body='Message body (2)')
        self.qm.send(m2)
        
        assert len(conn1.frames) == 1, "Expected connection to still only have 1 frame."
        assert conn1.frames[0] == m1
        
        ack = StompFrame('ACK', headers={'destination': dest, 'transaction': 'abc', 'message-id': m1.message_id})
        self.qm.ack(conn1, ack, transaction='abc')
        
        ack = StompFrame('ACK', headers={'destination': dest, 'transaction': 'abc', 'message-id': m2.message_id})
        self.qm.ack(conn1, ack, transaction='abc')
        
        assert len(conn1.frames) == 2, "Expected 2 frames now, after ACK."
        assert conn1.frames[1] == m2
        
        self.qm.resend_transaction_frames(conn1, transaction='abc')
        
        assert len(conn1.frames) == 3, "Expected 3 frames after re-transmit."
        assert bool(self.qm._pending[conn1]) == True, "Expected 1 pending (waiting on ACK) frame.""" 
        