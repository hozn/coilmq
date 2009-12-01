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
Functional tests to test full stack (but not actual socket layer).
"""
import unittest
import logging
import socket
import threading
from Queue import Queue

from coilmq.server.socketserver import StompServer, StompRequestHandler
from coilmq.util.buffer import StompFrameBuffer
from coilmq.store.memory import MemoryQueue
from coilmq.scheduler import FavorReliableSubscriberScheduler, RandomQueueScheduler

from coilmq.tests import mock
    
class TestStompRequestHandler(StompRequestHandler):
    
    def setup(self):
        StompRequestHandler.setup(self)
        self.ready_event = self.server.ready_event
        self.quit_event = self.server.quit_event
        self.done_event = self.server.done_event

class StompClient(object):
    """
    A stomp client for use in testing.
    
    This client spawns a listener thread and pushes anything that comes in onto the 
    read_frames queue.
    
    @ivar received_frames: A queue of StompFrame instances that have been received.
    @type received_frames: C{Queue.Queue} containing any received L{coilmq.frame.StompFrame}
    """
    def __init__(self, addr, quit_event, connect=True):
        """
        @param addr: The (host,port) tuple for connection.
        @type addr: C{tuple}
        
        @param quite_event: A C{threading.Event} to signal that we should stop receiving data.
        @type quite_event: C{threading.Event}
        
        @param connect: Whether to connect socket to specified addr.
        @type connect: C{bool}
        """
        self.log = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.addr = addr
        self.received_frames = Queue()
        self.quit_event = quit_event
        self.buffer = StompFrameBuffer()
        if connect:
            self.connect()
        
    def send(self, frame):
        """
        Sends a stomp frame.
        @param frame: The stomp frame to send.
        @type frame: L{coilmq.frame.StompFrame}
        """
        if not self.connected:
            raise RuntimeError("Not connected")
        self.sock.send(frame.pack())
    
    def connect(self):
        self.sock.connect(self.addr)
        t = threading.Thread(target=self._read_loop, name="StompClient.Receiver")
        t.start()
        self.connected = True
    
    def _read_loop(self):
        while not self.quit_event.is_set():
            data = self.sock.recv(1024)
            self.log.debug("Data received: %r" % data)
            self.buffer.append(data)
            for frame in self.buffer:
                self.log.debug("Processing frame: %s" % frame)
                self.received_frames.put(frame)
                
    def disconnect(self):
        if not self.connected:
            raise RuntimeError("Not connected")
        self.sock.close()
        self.connected = False
        
class TestStompServer(StompServer):
    
    def __init__(self, server_address,
                 bind_and_activate=True,
                 ready_event=None,
                 quit_event=None,
                 done_event=None,
                 authenticator=None,
                 queue_manager=None,
                 topic_manager=None):
        self.ready_event = ready_event
        self.quit_event = quit_event
        self.done_event = done_event
        StompServer.__init__(self, server_address, TestStompRequestHandler,
                             bind_and_activate=bind_and_activate,
                             authenticator=authenticator,
                             queue_manager=queue_manager,
                             topic_manager=topic_manager)

    def server_activate(self):
        print "Server activating..."
        self.ready_event.set()
        StompServer.server_activate(self)
    
    def handle_request(self):
        print "Handled request."
        self.ready_event.set()
        StompServer.handle_request(self)
        
    def get_request(self):
        print "In get_request()"
        return StompServer.get_request(self)
