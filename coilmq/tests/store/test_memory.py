"""
Test DBM queue storage.
"""
import unittest
import tempfile
import shutil
import uuid

from coilmq.store.memory import MemoryQueue
from coilmq.frame import StompFrame

class MemoryQueueTest(unittest.TestCase):
    
    def setUp(self):
        self.store = MemoryQueue()
    
    def test_enqueue(self):
        """ Test the enqueue() method. """
        dest = '/queue/foo'
        frame = StompFrame('MESSAGE', headers={'message-id': str(uuid.uuid4())}, body='some data') 
        self.store.enqueue(dest, frame)
        
        assert self.store.has_frames(dest) == True
        assert self.store.size(dest) == 1
        
    def test_dequeue(self):
        """ Test the dequeue() method. """
        dest = '/queue/foo'
        frame = StompFrame('MESSAGE', headers={'message-id': str(uuid.uuid4())}, body='some data') 
        self.store.enqueue(dest, frame)
        
        assert self.store.has_frames(dest) == True
        assert self.store.size(dest) == 1
        
        rframe = self.store.dequeue(dest)
        assert frame == rframe
        assert frame is rframe # Currently we expect these to be the /same/ object.  
        
        assert self.store.has_frames(dest) == False
        assert self.store.size(dest) == 0
        
    def test_dequeue_empty(self):
        """ Test dequeue() with empty queue. """
        
        r = self.store.dequeue('/queue/nonexist')
        assert r is None
        
        assert self.store.has_frames('/queue/nonexist') == False
        assert self.store.size('/queue/nonexist') == 0