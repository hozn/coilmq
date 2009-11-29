"""
Tests for topic-related functionality.
"""
import unittest

from coilmq.frame import StompFrame
from coilmq.topic import TopicManager

from coilmq.tests.mock import MockConnection

class TopicManagerTest(unittest.TestCase):
    """ Tests for the L{TopicManager} class. """
    
    def setUp(self):
        self.tm = TopicManager()
        self.conn = MockConnection()
    
    def test_subscribe(self):
        """ Test subscribing a connection to the topic. """
        dest = '/topic/dest'
        
        self.tm.subscribe(self.conn, dest)
        f = StompFrame('MESSAGE', headers={'destination': dest}, body='Empty')
        self.tm.send(f)
        
        print self.conn.frames
        assert len(self.conn.frames) == 1
        assert self.conn.frames[0] == f
    
    def test_unsubscribe(self):
        """ Test unsubscribing a connection from the queue. """
        dest = '/topic/dest'
        
        self.tm.subscribe(self.conn, dest)
        f = StompFrame('MESSAGE', headers={'destination': dest}, body='Empty')
        self.tm.send(f)
        
        print self.conn.frames
        assert len(self.conn.frames) == 1
        assert self.conn.frames[0] == f
        
        self.tm.unsubscribe(self.conn, dest)
        f = StompFrame('MESSAGE', headers={'destination': dest}, body='Empty')
        self.tm.send(f)
        
        assert len(self.conn.frames) == 1
        
    def send_simple(self):
        """ Test a basic send command. """
        dest = '/topic/dest'
        
        f = StompFrame('SEND', headers={'destination': dest}, body='Empty')
        self.tm.send(f)
        
        # Assert some side-effects
        assert 'message-id' in f.headers
        assert f.cmd == 'MESSAGE'

