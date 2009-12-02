# -*- coding: utf-8 -*-
#
# Copyright 2009 Hans Lellelid
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Functional tests that use the default memory-based storage backends and default
scheduler implementations.
"""
import hashlib
import zlib
import random

from coilmq.tests.functional import BaseFunctionalTestCase

class BasicTest(BaseFunctionalTestCase):
    """
    Functional tests using default storage engine, etc.
    """
        
    def test_connect(self):
        c = self._new_client()
        
    def test_subscribe(self):
        c1 = self._new_client()
        c1.subscribe('/queue/foo')
        
        c2 = self._new_client()
        c2.subscribe('/queue/foo2')
        
        c2.send('/queue/foo', 'A message')
        assert c2.received_frames.qsize() == 0
       
        r = c1.received_frames.get()
        assert r.cmd == 'MESSAGE'
        assert r.body == 'A message'
    
    def test_disconnect(self):
        """
        Test the 'polite' disconnect.
        """
        c1 = self._new_client()
        c1.connect()
        c1.disconnect()
        assert c1.received_frames.qsize() == 0
        
    def test_send_binary(self):
        """
        Test sending binary data.
        """
        c1 = self._new_client()
        c1.subscribe('/queue/foo')
        
        # Read some random binary data.
        # (This should be cross-platform.)
        message = 'This is the message that will be compressed.'
        c2 = self._new_client()
        compressed = zlib.compress(message)
        print '%r' % compressed
        c2.send('/queue/foo', zlib.compress(message))
       
        r = c1.received_frames.get()
        assert r.cmd == 'MESSAGE'
        print '%r' % r.body
        assert zlib.decompress(r.body) == message 
    
    def test_send_utf8(self):
        """
        Test sending utf-8-encoded strings.
        """
        c1 = self._new_client()
        c1.subscribe('/queue/foo')
        
        unicodemsg = u'我能吞下玻璃而不伤身体'
        utf8msg = unicodemsg.encode('utf-8')
        
        print "len(unicodemsg) = %d" % len(unicodemsg)
        print "len(utf8msg) = %d" % len(utf8msg)
        c2 = self._new_client()
        
        print '%r' % utf8msg
        c2.send('/queue/foo', utf8msg)
        
        r = c1.received_frames.get()
        assert r.cmd == 'MESSAGE'
        print '%r' % r.body
        assert r.body == utf8msg