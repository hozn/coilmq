"""
Test DBM queue storage.
"""
import unittest
import tempfile
import shutil
import uuid
import time

from coilmq.store.dbm import DbmQueue
from coilmq.frame import StompFrame

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

class DbmQueueTest(unittest.TestCase):
    
    def setUp(self):
        self.data_dir = tempfile.mkdtemp(prefix='coilmq-dbm-test')
        self.store = DbmQueue(self.data_dir)
    
    def tearDown(self):
        shutil.rmtree(self.data_dir)
    
    def test_enqueue(self):
        """ Test the enqueue() method. """
        dest = '/queue/foo'
        frame = StompFrame('MESSAGE', headers={'message-id': str(uuid.uuid4())}, body='some data') 
        self.store.enqueue(dest, frame)
        
        #print self.store.queue_metadata.keys()
        print self.store.queue_metadata[dest]
        #print "Frames? %s" % bool(self.store.queue_metadata[dest]['frames'])
        
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
        assert frame is not rframe 
        
        assert self.store.has_frames(dest) == False
        assert self.store.size(dest) == 0
        
    def test_dequeue_empty(self):
        """ Test dequeue() with empty queue. """
        
        r = self.store.dequeue('/queue/nonexist')
        assert r is None
        
        assert self.store.has_frames('/queue/nonexist') == False
        assert self.store.size('/queue/nonexist') == 0
    
    def test_sync_checkpoint_ops(self):
        """ Test a expected sync behavior with checkpoint_operations param. """
        
        data_dir = tempfile.mkdtemp(prefix='coilmq-dbm-test')
        max_ops = 5
        try:
            store = DbmQueue(data_dir, checkpoint_operations=max_ops)
            dest = '/queue/foo'
            
            for i in range(max_ops+1):
                frame = StompFrame('MESSAGE', headers={'message-id': str(uuid.uuid4())}, body='some data - %d' % i)
                store.enqueue(dest, frame)
            
            print store.queue_metadata[dest]    
            assert store.size(dest) == max_ops + 1
            
            # No close()!
            
            store2 = DbmQueue(data_dir)
            print store2.queue_metadata[dest]
            assert store2.size(dest) == max_ops + 1
            
        except:
            shutil.rmtree(data_dir, ignore_errors=True)
            raise
    
    def test_sync_checkpoint_timeout(self):
        """ Test a expected sync behavior with checkpoint_timeout param. """
        
        data_dir = tempfile.mkdtemp(prefix='coilmq-dbm-test')
        try:
            store = DbmQueue(data_dir, checkpoint_timeout=0.5)
            dest = '/queue/foo'
            
            frame = StompFrame('MESSAGE', headers={'message-id': str(uuid.uuid4())}, body='some data -1')
            store.enqueue(dest, frame)
            
            time.sleep(0.5)
            
            frame = StompFrame('MESSAGE', headers={'message-id': str(uuid.uuid4())}, body='some data -2')
            store.enqueue(dest, frame)
            
            print store.queue_metadata[dest]    
            assert store.size(dest) == 2
            
            # No close()!
            
            store2 = DbmQueue(data_dir)
            print store2.queue_metadata[dest]
            assert store2.size(dest) == 2
            
        except:
            shutil.rmtree(data_dir, ignore_errors=True)
            raise
        
    def test_sync_close(self):
        """ Test a expected sync behavior of close() call. """
        
        data_dir = tempfile.mkdtemp(prefix='coilmq-dbm-test')
        try:
            store = DbmQueue(data_dir)
            dest = '/queue/foo'
            frame = StompFrame('MESSAGE', headers={'message-id': str(uuid.uuid4())}, body='some data')
            store.enqueue(dest, frame)
            assert store.size(dest) == 1
            
            store.close()
            
            store2 = DbmQueue(data_dir)
            assert store2.size(dest) == 1
            
        except:
            shutil.rmtree(data_dir, ignore_errors=True)
            raise
        
    def test_sync_loss(self):
        """ Test metadata loss behavior. """
        
        data_dir = tempfile.mkdtemp(prefix='coilmq-dbm-test')
        try:
            store = DbmQueue(data_dir)
            dest = '/queue/foo'
            frame = StompFrame('MESSAGE', headers={'message-id': str(uuid.uuid4())}, body='some data')
            store.enqueue(dest, frame)
            assert store.size(dest) == 1
            
            store2 = DbmQueue(data_dir)
            assert store2.size(dest) == 0
            
        except:
            shutil.rmtree(data_dir, ignore_errors=True)
            raise