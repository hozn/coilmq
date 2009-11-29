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
The default SocketServer-based server implementation. 
"""
__authors__ = [
  '"Hans Lellelid" <hans@xmpl.org>',
]
import logging
from SocketServer import BaseRequestHandler, TCPServer, ThreadingMixIn

from coilmq.server import StompConnection
from coilmq.engine import StompEngine
from coilmq.util.buffer import StompFrameBuffer

from coilmq.topic import TopicManager
from coilmq.queue import QueueManager
from coilmq.store.memory import MemoryQueue
from coilmq.scheduler import FavorReliableSubscriberScheduler, RandomQueueScheduler    
    
class StompRequestHandler(BaseRequestHandler, StompConnection):
    """
    Class that will be instantiated to handle STOMP connections.

    This class will be instantiated once per connection to the server.  In a multi-threaded
    context, that means that instances of this class are scoped to a single thread.  It should
    be noted that while the L{coilmq.engine.StompEngine} instance will be thread-local, the 
    storage containers configured into the engine are not thread-local (and hence must be
    thread-safe). 
    
    @ivar buffer: A StompBuffer instance which buffers received data (to ensure we deal with
                    complete STOMP messages.
    @type buffer: L{stomper.stompbuffer.StompBuffer}
    
    @ivar engine: The STOMP protocol engine.
    @type engine: L{coilmq.engine.StompEngine}
    """
    
    def setup(self):
        self.log = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))
        self.buffer = StompFrameBuffer()
        self.engine = StompEngine(connection=self,
                                  authenticator=self.server.authenticator,
                                  queue_manager=self.server.queue_manager,
                                  topic_manager=self.server.topic_manager)
        
    def handle(self):
        """
        Handle a new socket connection.
        """
        # self.request is the TCP socket connected to the client
        try:
            while True:
                data = self.request.recv(1024)
                if not data:
                    break
                
                self.log.debug("Data received: %r" % data)
                self.buffer.append(data)
                
                for frame in self.buffer:
                    self.log.debug("Processing frame: %s" % frame)
                    self.engine.processFrame(frame)
        except Exception, e:
            self.log.error("Error receiving data (unbinding): %s" % e)
            self.engine.unbind()
            raise

    def send_frame(self, frame):
        """ Sends a frame to connected socket client.
        
        @param frame: The frame to send.
        @type frame: L{coilmq.frame.StompFrame}
        """
        self.request.sendall(frame.pack())

class StompServer(TCPServer):
    """
    Subclass of C{StompServer.TCPServer} to handle new connections with 
    instances of L{StompRequestHandler}.
    
    @ivar authenticator: The authenticator to use.
    @type authenticator: L{coilmq.auth.Authenticator}
    
    @ivar queue_manager: The queue manager to use.
    @type queue_manager: L{coilmq.queue.QueueManager}
    
    @ivar topic_manager: The topic manager to use.
    @type topic_manager: L{coilmq.topic.TopicManager}
    """
    
    def __init__(self, server_address, bind_and_activate=True, authenticator=None, queue_manager=None, topic_manager=None):
        """
        Extension to C{TCPServer} constructor to provide mechanism for providing implementation classes.
        
        @keyword authenticator: The configure L{coilmq.auth.Authenticator} object to use.
        @keyword queue_manager: The configured L{coilmq.queue.QueueManager} object to use.
        @keyword topic_manager: The configured L{coilmq.topic.TopicManager} object to use. 
        """
        TCPServer.__init__(self, server_address, StompRequestHandler, bind_and_activate=bind_and_activate)
        self.authenticator = authenticator
        self.queue_manager = queue_manager
        self.topic_manager = topic_manager
        
class ThreadedStompServer(ThreadingMixIn, StompServer):
    pass

def main():
    """ Start the socket server. """
    
    # TODO: Replace with config-based setup
    logging.basicConfig(level=logging.DEBUG)

    server = ThreadedStompServer(('0.0.0.0', 61613),
                                 queue_manager=QueueManager(store=MemoryQueue(),
                                                            subscriber_scheduler=FavorReliableSubscriberScheduler(),
                                                            queue_scheduler=RandomQueueScheduler()),
                                 topic_manager=TopicManager())
    server.serve_forever()
    
if __name__ == '__main__':
    main()