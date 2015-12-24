"""
Test memory queue storage.
"""
import unittest
import uuid


from coilmq.util.frames import Frame
from coilmq.store.memory import MemoryQueue

from coilmq.tests.store import CommonQueueTest

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


class MemoryQueueTest(CommonQueueTest, unittest.TestCase):

    def setUp(self):
        self.store = MemoryQueue()

    def test_dequeue_identity(self):
        """ Test the dequeue() method. """
        dest = '/queue/foo'
        frame = Frame('MESSAGE', headers={
                      'message-id': str(uuid.uuid4())}, body='some data')
        self.store.enqueue(dest, frame)

        self.assertTrue(self.store.has_frames(dest))
        self.assertEqual(self.store.size(dest), 1)

        rframe = self.store.dequeue(dest)
        self.assertEqual(frame, rframe)
        # Currently we expect these to be the /same/ object.
        self.assertIs(frame, rframe)

        self.assertFalse(self.store.has_frames(dest))
        self.assertEqual(self.store.size(dest), 0)
