"""
Test DBM queue storage.
"""
import unittest
import tempfile
import shutil
import uuid
import time
import datetime

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import select, func, and_, text

from coilmq.store.sa import meta, model
from coilmq.store.sa.model import init_model
from coilmq.store.sa import SAQueue
from coilmq.frame import StompFrame

from coilmq.tests.store import CommonQueueTestsMixin

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

class SAQueueTest(unittest.TestCase, CommonQueueTestsMixin):
    
    def setUp(self):
        meta.engine = create_engine('sqlite:///:memory:', echo=True)
        meta.metadata = MetaData(bind=meta.engine)
        meta.Session = scoped_session(sessionmaker(bind=meta.engine))
        init_model()
        self.store = SAQueue()
    
    def tearDown(self):
        print dir(meta.engine)
        meta.Session.remove()
        
    def test_dequeue_order(self):
        """ Test the order that frames are returned by dequeue() method. """
        dest = '/queue/foo'
        
        frame1 = StompFrame('MESSAGE', headers={'message-id': 'id-1'}, body='message-1') 
        self.store.enqueue(dest, frame1)
        
        frame2 = StompFrame('MESSAGE', headers={'message-id': 'id-2'}, body='message-2') 
        self.store.enqueue(dest, frame2)
        
        frame3 = StompFrame('MESSAGE', headers={'message-id': 'id-3'}, body='message-3') 
        self.store.enqueue(dest, frame3)
        
        assert self.store.has_frames(dest) == True
        assert self.store.size(dest) == 3
        
        # Perform some updates to change the expected order.
        
        sess = meta.Session()
        sess.execute(model.frames_table.update().where(model.frames_table.c.message_id=='id-1').values(queued=datetime.datetime(2010, 01, 01)))
        sess.execute(model.frames_table.update().where(model.frames_table.c.message_id=='id-2').values(queued=datetime.datetime(2009, 01, 01)))
        sess.execute(model.frames_table.update().where(model.frames_table.c.message_id=='id-3').values(queued=datetime.datetime(2004, 01, 01)))
        sess.commit()
        
        rframe1 = self.store.dequeue(dest)
        assert frame3 == rframe1
         
        rframe2 = self.store.dequeue(dest)
        assert frame2 == rframe2
        
        rframe3 = self.store.dequeue(dest)
        assert frame1 == rframe3
        
        assert self.store.has_frames(dest) == False
        assert self.store.size(dest) == 0

    