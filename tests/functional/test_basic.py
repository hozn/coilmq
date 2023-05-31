# -*- coding: utf-8 -*-
"""
Functional tests that use the default memory-based storage backends and default
scheduler implementations.
"""
import zlib

import six

from coilmq.auth.simple import SimpleAuthenticator
from coilmq.util import frames
from tests.functional import BaseFunctionalTestCase, Empty as QueueEmpty

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


class BasicTest(BaseFunctionalTestCase):
    """
    Functional tests using default storage engine, etc.
    """

    def test_connect(self):
        """ Test a basic (non-auth) connection. """
        c = self._new_client()

    def test_connect_auth(self):
        """ Test connecting when auth is required. """
        self.server.authenticator = SimpleAuthenticator(store={'user': 'pass'})

        c1 = self._new_client(connect=False)
        c1.connect()
        r = c1.received_frames.get(timeout=1)
        self.assertEqual(r.cmd, frames.ERROR)
        self.assertIn(b'Auth', r.body)

        c2 = self._new_client(connect=False)
        c2.connect(headers={'login': 'user', 'passcode': 'pass'})
        r2 = c2.received_frames.get(timeout=1)

        self.assertEqual(r2.cmd, frames.CONNECTED)

        c3 = self._new_client(connect=False)
        c3.connect(headers={'login': 'user', 'passcode': 'pass-invalid'})
        r3 = c3.received_frames.get(timeout=1)

        self.assertEqual(r3.cmd, frames.ERROR)

    def test_send_receipt(self):
        c1 = self._new_client()
        c1.send('/topic/foo', 'A message', extra_headers={'receipt': 'FOOBAR'})
        r = c1.received_frames.get(timeout=1)

    def test_subscribe(self):
        c1 = self._new_client()
        c1.subscribe('/queue/foo')

        c2 = self._new_client()
        c2.subscribe('/queue/foo2')

        c2.send('/queue/foo', 'A message')
        self.assertEqual(c2.received_frames.qsize(), 0)

        r = c1.received_frames.get()
        self.assertEqual(r.cmd, frames.MESSAGE)
        self.assertEqual(r.body, b'A message')

    def test_disconnect(self):
        """
        Test the 'polite' disconnect.
        """
        c1 = self._new_client()
        c1.connect()
        response = c1.received_frames.get(timeout=0.5)
        self.assertEqual(response.cmd, frames.CONNECTED)
        c1.disconnect()
        self.assertRaises(QueueEmpty, lambda: c1.received_frames.get(block=False))

    def test_send_binary(self):
        """
        Test sending binary data.
        """
        c1 = self._new_client()
        c1.subscribe('/queue/foo')

        # Read some random binary data.
        # (This should be cross-platform.)
        message = six.b('This is the message that will be compressed.')
        c2 = self._new_client()
        c2.send('/queue/foo', zlib.compress(message))

        res = c1.received_frames.get()
        self.assertEqual(res.cmd, frames.MESSAGE)
        self.assertEqual(zlib.decompress(res.body), message)

    def test_send_utf8(self):
        """
        Test sending utf-8-encoded strings.
        """
        c1 = self._new_client()
        c1.subscribe('/queue/foo')

        unicodemsg = u'我能吞下玻璃而不伤身体'
        utf8msg = unicodemsg.encode('utf-8')

        c2 = self._new_client()

        c2.send('/queue/foo', utf8msg)

        res = c1.received_frames.get()
        self.assertEqual(res.cmd, frames.MESSAGE)
        self.assertEqual(res.body, utf8msg)
