"""
Tests for topic-related functionality.
"""
import unittest
import socket

from coilmq.util.frames import Frame
from coilmq.topic import TopicManager
from coilmq.tests.mock import MockConnection

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

class TopicManagerTest(unittest.TestCase):
    """ Tests for the L{TopicManager} class. """
    
    def setUp(self):
        self.tm = TopicManager()
        self.conn = MockConnection()
    
    def test_subscribe(self):
        """ Test subscribing a connection to the topic. """
        dest = '/topic/dest'
        
        self.tm.subscribe(self.conn, dest)
        f = Frame('MESSAGE', headers={'destination': dest}, body='Empty')
        self.tm.send(f)
        
        self.assertEqual(len(self.conn.frames), 1)
        self.assertEqual(self.conn.frames[0], f)
    
    def test_unsubscribe(self):
        """ Test unsubscribing a connection from the queue. """
        dest = '/topic/dest'
        
        self.tm.subscribe(self.conn, dest)
        f = Frame('MESSAGE', headers={'destination': dest}, body='Empty')
        self.tm.send(f)
        
        self.assertEqual(len(self.conn.frames), 1)
        self.assertEqual(self.conn.frames[0], f)
        
        self.tm.unsubscribe(self.conn, dest)
        f = Frame('MESSAGE', headers={'destination': dest}, body='Empty')
        self.tm.send(f)
        
        self.assertEqual(len(self.conn.frames), 1)
        
    def send_simple(self):
        """ Test a basic send command. """
        dest = '/topic/dest'
        
        f = Frame('SEND', headers={'destination': dest}, body='Empty')
        self.tm.send(f)
        
        # Assert some side-effects
        self.assertIn('message-id', f.headers)
        self.assertEqual(f.cmd == 'message')

    def send_subscriber_timeout(self):
        """ Test a send command when one subscriber errs out. """
        
        class TimeoutConnection(object):
            reliable_subscriber = False
                
            def send_frame(self, frame):
                raise socket.timeout("timed out")
            
            def reset(self):
                pass
                
        dest = '/topic/dest'
        
        bad_client = TimeoutConnection()
        
        # Subscribe both a good client and a bad client.
        self.tm.subscribe(bad_client, dest)
        self.tm.subscribe(self.conn, dest)
        
        f = Frame('message', headers={'destination': dest}, body='Empty')
        self.tm.send(f)

        # Make sure out good client got the message.
        self.assertEqual(len(self.conn.frames), 1)
        self.assertEqual(self.conn.frames[0], f)
        
        # Make sure our bad client got disconnected
        # (This might be a bit too intimate.)
        self.assertNotIn(bad_client, self.tm._topics[dest])