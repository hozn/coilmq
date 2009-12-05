"""
Tests for the transport-agnostic engine module.
"""
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
import unittest

from coilmq import auth
from coilmq.frame import StompFrame
from coilmq.engine import StompEngine, ProtocolError
from coilmq.store.memory import MemoryQueue
from coilmq.queue import QueueManager
from coilmq.topic import TopicManager

from coilmq.tests.mock import (MockAuthenticator, MockConnection, MockQueueManager, 
                               MockTopicManager)

class EngineTest(unittest.TestCase):
    
    def setUp(self):
        self.qm = MockQueueManager()
        self.tm = MockTopicManager()
        self.conn = MockConnection()
        self.auth = MockAuthenticator()
        self.engine = StompEngine(connection=self.conn,
                                  queue_manager=self.qm,
                                  topic_manager=self.tm,
                                  authenticator=None)
    def tearDown(self):
        self.conn.reset()
    
    def _connect(self):
        """ Call the engine connect() method so that we have a valid 'session'. """
        self.engine.connect(StompFrame('CONNECT'))
    
    def assertErrorFrame(self, frame, msgsub):
        """ Assert that the passed in frame is an error frame and that message contains specified
        string.
        """
        assert frame.cmd == 'ERROR'
        assert msgsub.lower() in frame.headers['message'].lower()
        
    def test_connect_no_auth(self):
        """ Test the CONNECT command with no auth required. """
        
        assert self.engine.connected == False
        self.engine.processFrame(StompFrame('CONNECT'))
        assert self.engine.connected == True
        
    def test_connect_auth(self):
        """ Test the CONNECT command when auth is required. """
        self.engine.authenticator = self.auth
        
        assert self.engine.connected == False
        self.engine.processFrame(StompFrame('CONNECT'))
        self.assertErrorFrame(self.conn.frames[-1], 'Auth')
        assert self.engine.connected == False
        
        self.engine.processFrame(StompFrame('CONNECT', headers={'login': MockAuthenticator.LOGIN,
                                                                'passcode': MockAuthenticator.PASSCODE}))
        assert self.engine.connected == True
    
    def test_subscribe_noack(self):
        """ Test subscribing to topics and queues w/ no ACK. """
        self._connect()
        self.engine.processFrame(StompFrame('SUBSCRIBE', headers={'destination': '/queue/bar'}))
        assert self.conn in self.qm.queues['/queue/bar']
        
        self.engine.processFrame(StompFrame('SUBSCRIBE', headers={'destination': '/foo/bar'}))
        assert self.conn in self.tm.topics['/foo/bar']
    
    def test_send(self):
        """ Test sending to a topic and queue. """
        self._connect()
        
        msg = StompFrame('SEND', headers={'destination': '/queue/foo'}, body='QUEUEMSG-BODY')
        self.engine.processFrame(msg)        
        assert msg == self.qm.messages[-1]
        
        msg = StompFrame('SEND', headers={'destination': '/topic/foo'}, body='TOPICMSG-BODY')
        self.engine.processFrame(msg)
        assert msg == self.tm.messages[-1]
        
        msg = StompFrame('SEND', headers={}, body='TOPICMSG-BODY')
        self.engine.processFrame(msg)
        self.assertErrorFrame(self.conn.frames[-1], 'Missing destination')
    
    def test_subscribe_ack(self):
        """ Test subscribing to a queue with ack=true """
        self._connect()
        self.engine.processFrame(StompFrame('SUBSCRIBE', headers={'destination': '/queue/bar',
                                                                  'ack': 'client'}))
        assert self.conn.reliable_subscriber == True
        assert self.conn in self.qm.queues['/queue/bar']
        
    def test_unsubscribe(self):
        """ Test the UNSUBSCRIBE command. """
        self._connect()
        self.engine.processFrame(StompFrame('SUBSCRIBE', headers={'destination': '/queue/bar'}))
        assert self.conn in self.qm.queues['/queue/bar']
        
        print self.conn.frames
        
        self.engine.processFrame(StompFrame('UNSUBSCRIBE', headers={'destination': '/queue/bar'}))
        assert self.conn not in self.qm.queues['/queue/bar']
        
        print self.conn.frames
        
        self.engine.processFrame(StompFrame('UNSUBSCRIBE', headers={'destination': '/invalid'}))
        print self.conn.frames[-1]
    
    def test_begin(self):
        """ Test transaction BEGIN. """
        self._connect()
        
        self.engine.processFrame(StompFrame('BEGIN', headers={'transaction': 'abc'}))
        assert 'abc' in self.engine.transactions
        assert len(self.engine.transactions['abc']) == 0
    
    def test_commit(self):
        """ Test transaction COMMIT. """
        self._connect()
         
        self.engine.processFrame(StompFrame('BEGIN', headers={'transaction': 'abc'}))
        self.engine.processFrame(StompFrame('BEGIN', headers={'transaction': '123'}))
        self.engine.processFrame(StompFrame('SEND', headers={'destination': '/dest', 'transaction': 'abc'}, body='ASDF'))
        self.engine.processFrame(StompFrame('SEND', headers={'destination': '/dest', 'transaction': 'abc'}, body='ASDF'))
        self.engine.processFrame(StompFrame('SEND', headers={'destination': '/dest', 'transaction': '123'}, body='ASDF'))
        
        assert len(self.tm.messages) == 0
        
        self.engine.processFrame(StompFrame('COMMIT', headers={'transaction': 'abc'}))
        
        print self.conn.frames
        
        assert len(self.tm.messages) == 2
        
        assert len(self.engine.transactions) == 1
        
        self.engine.processFrame(StompFrame('COMMIT', headers={'transaction': '123'}))
        assert len(self.tm.messages) == 3
        
        assert len(self.engine.transactions) == 0
        
    def test_commit_invalid(self):
        """ Test invalid states for transaction COMMIT. """
        self._connect()
        
        # Send a message with invalid transaction 
        f = StompFrame('SEND', headers={'destination': '/dest', 'transaction': '123'}, body='ASDF')
        self.engine.processFrame(f)
        self.assertErrorFrame(self.conn.frames[-1], 'invalid transaction')
        
        # Attempt to commit invalid transaction
        self.engine.processFrame(StompFrame('COMMIT', headers={'transaction': 'abc'}))
        
        # Attempt to commit already-committed transaction
        self.engine.processFrame(StompFrame('BEGIN', headers={'transaction': 'abc'}))
        self.engine.processFrame(StompFrame('SEND', headers={'destination': '/dest', 'transaction': 'abc'}, body='FOO'))
        self.engine.processFrame(StompFrame('COMMIT', headers={'transaction': 'abc'}))
        
        self.engine.processFrame(StompFrame('COMMIT', headers={'transaction': 'abc'}))
        self.assertErrorFrame(self.conn.frames[-1], 'invalid transaction')
    
    def test_abort(self):
        """ Test transaction ABORT. """
        self._connect()
         
        self.engine.processFrame(StompFrame('BEGIN', headers={'transaction': 'abc'}))
        self.engine.processFrame(StompFrame('BEGIN', headers={'transaction': '123'}))
        
        f1 = StompFrame('SEND', headers={'destination': '/dest', 'transaction': 'abc'}, body='ASDF')
        self.engine.processFrame(f1)
        f2 = StompFrame('SEND', headers={'destination': '/dest', 'transaction': 'abc'}, body='ASDF')
        self.engine.processFrame(f2)
        f3 = StompFrame('SEND', headers={'destination': '/dest', 'transaction': '123'}, body='ASDF')
        self.engine.processFrame(f3)
        
        assert len(self.tm.messages) == 0
        
        self.engine.processFrame(StompFrame('ABORT', headers={'transaction': 'abc'}))
        assert len(self.tm.messages) == 0
        
        assert len(self.engine.transactions) == 1
        
    def test_abort_invalid(self):
        """ Test invalid states for transaction ABORT. """
        self._connect()
         
        self.engine.processFrame(StompFrame('ABORT', headers={'transaction': 'abc'}))
        
        self.assertErrorFrame(self.conn.frames[-1], 'invalid transaction')
        
        self.engine.processFrame(StompFrame('BEGIN', headers={'transaction': 'abc'}))
        self.engine.processFrame(StompFrame('ABORT', headers={'transaction': 'abc'}))
        
        self.engine.processFrame(StompFrame('ABORT', headers={'transaction': 'abc2'}))
        self.assertErrorFrame(self.conn.frames[-1], 'invalid transaction')         