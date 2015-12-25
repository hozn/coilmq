"""
Test SQLAlchemy storage.
"""
import datetime
import unittest

from sqlalchemy import create_engine

from coilmq.store.sa import SAQueue
from coilmq.store.sa import init_model
from coilmq.store.sa import meta, model
from coilmq.util.frames import Frame
from tests.store import CommonQueueTest

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


class SAQueueTest(CommonQueueTest, unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite:///:memory:', echo=True)
        init_model(engine)
        self.store = SAQueue()

    def tearDown(self):
        meta.Session.remove()

    def test_dequeue_order(self):
        """ Test the order that frames are returned by dequeue() method. """
        dest = '/queue/foo'

        frame1 = Frame('MESSAGE', headers={
                       'message-id': 'id-1'}, body='message-1')
        self.store.enqueue(dest, frame1)

        frame2 = Frame('MESSAGE', headers={
                       'message-id': 'id-2'}, body='message-2')
        self.store.enqueue(dest, frame2)

        frame3 = Frame('MESSAGE', headers={
                       'message-id': 'id-3'}, body='message-3')
        self.store.enqueue(dest, frame3)

        self.assertTrue(self.store.has_frames(dest))
        self.assertEqual(self.store.size(dest), 3)

        # Perform some updates to change the expected order.

        sess = meta.Session()
        sess.execute(model.frames_table.update().where(
            model.frames_table.c.message_id == 'id-1').values(queued=datetime.datetime(2010, 1, 1)))
        sess.execute(model.frames_table.update().where(
            model.frames_table.c.message_id == 'id-2').values(queued=datetime.datetime(2009, 1, 1)))
        sess.execute(model.frames_table.update().where(
            model.frames_table.c.message_id == 'id-3').values(queued=datetime.datetime(2004, 1, 1)))
        sess.commit()

        rframe1 = self.store.dequeue(dest)
        assert frame3 == rframe1

        rframe2 = self.store.dequeue(dest)
        assert frame2 == rframe2

        rframe3 = self.store.dequeue(dest)
        assert frame1 == rframe3

        assert self.store.has_frames(dest) == False
        assert self.store.size(dest) == 0
