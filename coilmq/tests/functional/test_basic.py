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
Some basic functional tests.
"""
import unittest
import logging
import socket
import threading
from Queue import Queue

from coilmq.frame import StompFrame
from coilmq.queue import QueueManager
from coilmq.topic import TopicManager
from coilmq.store.memory import MemoryQueue
from coilmq.scheduler import FavorReliableSubscriberScheduler, RandomQueueScheduler    

from coilmq.tests.functional import TestStompServer, TestStompClient

class BasicTest(unittest.TestCase):
    """
    A basic set of functional tests.
    
    We have to jump through a few hoops to test the multi-threaded code.  We're
    suing a combination of C{threading.Event} and C{Queue.Queue} objects to faciliate
    inter-thread communication and lock-stepping the assertions. 
    """
    def setUp(self):
        
        self.clients = []
        self.server_address = None # This gets set in the server thread.
        self.ready_event = threading.Event()
        
        qm = QueueManager(store=MemoryQueue(),
                          subscriber_scheduler=FavorReliableSubscriberScheduler(),
                          queue_scheduler=RandomQueueScheduler())
        tm = TopicManager()
        
        addr_bound = threading.Event()
        def start_server():
            self.server = TestStompServer(('127.0.0.1', 0),
                                          ready_event=self.ready_event,
                                          authenticator=None,
                                          queue_manager=qm,
                                          topic_manager=tm)
            self.server_address = self.server.socket.getsockname()
            addr_bound.set()
            #print "Server address: %s" % (self.server_address,)
            self.server.serve_forever()
            
        self.server_thread = threading.Thread(target=start_server, name='server')
        self.server_thread.start()
        self.ready_event.wait()
        addr_bound.wait()
        
    def tearDown(self):
        for c in self.clients:
            print "Disconnecting %s" % c
            c.close()
        self.server.shutdown()
        self.server_thread.join()
        self.ready_event.clear()
        del self.server_thread
        
    def _new_client(self):
        """
        Get a new L{TestStompClient} connected to our test server. 
        """
        client = TestStompClient(self.server_address)
        self.clients.append(client)
        client.connect()
        print "Client created: %s" % (client)
        r = client.received_frames.get(timeout=1)
        assert r.cmd == 'CONNECTED'
        return client
        
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
        
    