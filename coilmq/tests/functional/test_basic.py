"""

"""
import unittest
import socket
import threading
from Queue import Queue

from coilmq.frame import StompFrame
from coilmq.queue import QueueManager
from coilmq.topic import TopicManager
from coilmq.store.memory import MemoryQueue
from coilmq.scheduler import FavorReliableSubscriberScheduler, RandomQueueScheduler    

from coilmq.tests.functional import TestStompRequestHandler, TestStompServer, StompClient

class SocketObjTestCase(unittest.TestCase):
    
    def setUp(self):
        self.server_address = ('127.0.0.1', 61613)
        self.ready_event = threading.Event()
        self.done_event = threading.Event()
        self.quit_event = threading.Event()
        
        qm = QueueManager(store=MemoryQueue(),
                          subscriber_scheduler=FavorReliableSubscriberScheduler(),
                          queue_scheduler=RandomQueueScheduler())
        tm = TopicManager()
        
        def start_server():
#            s = socket.socket()
#            s.bind(("0.0.0.0", 9999))
#            s.listen(3)
#            print "Listen(3)"
            self.server = TestStompServer(self.server_address,
                                          ready_event=self.ready_event,
                                          quit_event=self.quit_event,
                                          done_event=self.done_event,
                                          authenticator=None,
                                          queue_manager=qm,
                                          topic_manager=tm)
            print "Serving forever..."
            self.server.serve_forever()
            
        self.server_thread = threading.Thread(target=start_server, name='StompServer')
        self.server_thread.start()
        print "Waiting on ready_event."
        self.ready_event.wait()
        
        
    def tearDown(self):
        self.server.shutdown()
        self.quit_event.set()
        self.server_thread.join()
        del self.server_thread
        #self.done_event.wait()
        self.ready_event.clear()
        self.done_event.clear()
        self.quit_event.clear()

    def test_connect(self):
        client = StompClient(self.server_address, self.quit_event)
        client.send(StompFrame('CONNECT'))
        received = client.received_frames.get(timeout=1) 
        print received
        assert False