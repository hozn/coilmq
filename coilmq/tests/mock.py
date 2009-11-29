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
Classes to be used for mock objects.
"""
from collections import defaultdict

from coilmq import auth

class MockConnection(object):
    
    def __init__(self):
        self.frames = []
        self.reliable_subscriber = False
        
    def send_frame(self, frame):
        self.frames.append(frame)
    
    def reset(self):
        self.frames = []

class MockAuthenticator(auth.Authenticator):
    LOGIN = 'foo'
    PASSCODE = 'bar'
        
    def authenticate(self, login, passcode):
        return (login == self.LOGIN and passcode == self.PASSCODE)

class MockQueueManager(object):
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        self.queues = defaultdict(set)
        self.acks = []
        self.messages = []
        self.transaction_frames = defaultdict(lambda: defaultdict(list))
        
    def subscribe(self, conn, dest):
        self.queues[dest].add(conn)
    
    def unsubscribe(self, conn, dest):
        if dest in self.queues:
            self.queues[dest].remove(conn)
        
    def send(self, message):
        self.messages.append(message) 
        
    def ack(self, connection, frame, transaction=None):
        
        if transaction:
            self.transaction_frames[connection][transaction].append(frame)
        
        self.acks.append(frame.message_id)
        

    def resend_transaction_frames(self, connection, transaction):
        """ Resend the messages that were ACK'd in specified transaction.
        
        @param connection: The client connection that aborted the transaction.
        @type connection: L{coilmq.server.StompConnection}
        
        @param transaction: The transaction id (which was aborted).
        @type transaction: C{str}
        """
        for frame in self.transaction_frames[connection][transaction]:
            self.send(frame)
    
    def clearTransactionFrames(self, connection, transaction):
        """ Clears out the queued ACK frames for specified transaction. 
        
        @param connection: The client connection that committed the transaction.
        @type connection: L{coilmq.server.StompConnection}
        
        @param transaction: The transaction id (which was committed).
        @type transaction: C{str}
        """
        try:
            del self.transaction_frames[connection][transaction]
        except KeyError:
            pass
        
        
class MockTopicManager(object):
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        self.topics = defaultdict(set)
        self.messages = []
        
    def subscribe(self, conn, dest):
        self.topics[dest].add(conn)
    
    def unsubscribe(self, conn, dest):
        if dest in self.topics:
            self.topics[dest].remove(conn)
        
    def send(self, message):
        self.messages.append(message)