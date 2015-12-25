"""
Queue storage tests.
"""
import unittest
import uuid

from coilmq.util.frames import Frame

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


class CommonQueueTest(object):
    """
    An abstract set of base tests for queue storage engines.

    This class must be mixed in with something that extends C{unittest.TestCase}.
    """

    def test_enqueue(self):
        """ Test the enqueue() method. """
        dest = '/queue/foo'
        frame = Frame('MESSAGE', headers={
                      'message-id': str(uuid.uuid4())}, body='some data')
        self.store.enqueue(dest, frame)

        self.assertTrue(self.store.has_frames(dest))
        self.assertEqual(self.store.size(dest), 1)

    def test_dequeue(self):
        """ Test the dequeue() method. """
        dest = '/queue/foo'
        frame = Frame('MESSAGE', headers={
                      'message-id': str(uuid.uuid4())}, body='some data')
        self.store.enqueue(dest, frame)

        self.assertTrue(self.store.has_frames(dest))
        self.assertEqual(self.store.size(dest), 1)

        rframe = self.store.dequeue(dest)
        self.assertEqual(frame, rframe)

        # We cannot generically assert whether or not frame and rframe are
        # the *same* object.

        self.assertFalse(self.store.has_frames(dest))
        self.assertEqual(self.store.size(dest), 0)

    def test_dequeue_specific(self):
        """ Test that we only dequeue from the correct queue. """
        dest = '/queue/foo'
        notdest = '/queue/other'

        frame1 = Frame('MESSAGE', headers={
                       'message-id': str(uuid.uuid4())}, body='message-1')
        self.store.enqueue(dest, frame1)

        frame2 = Frame('MESSAGE', headers={
                       'message-id': str(uuid.uuid4())}, body='message-2')
        self.store.enqueue(notdest, frame2)

        frame3 = Frame('MESSAGE', headers={
                       'message-id': str(uuid.uuid4())}, body='message-3')
        self.store.enqueue(dest, frame3)

        self.assertTrue(self.store.has_frames(dest))
        self.assertEqual(self.store.size(dest), 2)

        rframe1 = self.store.dequeue(dest)
        self.assertEqual(frame1, rframe1)

        rframe2 = self.store.dequeue(dest)
        self.assertEqual(frame3, rframe2)

        self.assertFalse(self.store.has_frames(dest))
        self.assertEqual(self.store.size(dest), 0)

    def test_dequeue_order(self):
        """ Test the order that frames are returned by dequeue() method. """
        dest = '/queue/foo'

        frame1 = Frame('MESSAGE', headers={
                       'message-id': str(uuid.uuid4())}, body='message-1')
        self.store.enqueue(dest, frame1)

        frame2 = Frame('MESSAGE', headers={
                       'message-id': str(uuid.uuid4())}, body='message-2')
        self.store.enqueue(dest, frame2)

        frame3 = Frame('MESSAGE', headers={
                       'message-id': str(uuid.uuid4())}, body='message-3')
        self.store.enqueue(dest, frame3)

        self.assertTrue(self.store.has_frames(dest))
        self.assertEqual(self.store.size(dest), 3)

        rframe1 = self.store.dequeue(dest)
        self.assertEqual(frame1, rframe1)

        rframe2 = self.store.dequeue(dest)
        self.assertEqual(frame2, rframe2)

        rframe3 = self.store.dequeue(dest)
        self.assertEqual(frame3, rframe3)

        self.assertFalse(self.store.has_frames(dest))
        self.assertEqual(self.store.size(dest), 0)

    def test_dequeue_empty(self):
        """ Test dequeue() with empty queue. """

        self.assertIsNone(self.store.dequeue('/queue/nonexist'))

        self.assertFalse(self.store.has_frames('/queue/nonexist'))
        self.assertEqual(self.store.size('/queue/nonexist'), 0)
