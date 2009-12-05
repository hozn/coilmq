"""
Core STOMP server logic, abstracted from socket transport implementation.

While this is abstracted from the socket transport implementation, it does
operate on the assumption that there is an available connection to send response
frames.

We're also making some simplified assumptions here which may make this engine
impractical for [high-performance] use in specifically asynchronous frameworks.
More specifically, this class was not explicitly designed around async patterns, 
meaning that it would likely be problematic to use with a framework like Twisted 
if the underlying storage implementations were processor intensive (e.g. database
access).  For the default memory storage engines, this shouldn't be a problem.

This code is inspired by the design of the Ruby stompserver project, by 
Patrick Hurley and Lionel Bouton.  See http://stompserver.rubyforge.org/
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

from stomper import VALID_COMMANDS
import logging
import uuid
from collections import defaultdict

from coilmq.frame import ConnectedFrame, ReceiptFrame, ErrorFrame
from coilmq.auth import AuthError

VALID_COMMANDS = ['connect', 'send', 'subscribe', 'unsubscribe', 'begin', 'commit', 'abort', 'ack', 'disconnect']

class ProtocolError(RuntimeError):
    pass

class StompEngine(object):
    """ 
    The engine provides the core business logic that we use to respond to STOMP protocol
    messages.  
    
    This class is transport-agnostic; it exposes methods that expect STOMP frames and
    uses the attached connection to send frames to connected clients.
    
    @ivar connection: The connection (aka "protocol") backing this engine.
    @type connection: L{coilmq.server.StompConnection}
    
    @ivar authenticator: An C{Authenticator} implementation to use.  Setting this value
                            will implicitly cause authentication to be required.
    @type authenticator: L{coilmq.auth.Authenticator}
    
    @ivar queue_manager: The C{QueueManager} implementation to use.
    @type queue_manager: L{coilmq.queue.QueueManager}
    
    @ivar topic_manager: The C{TopicManager} implementation to use.
    @type topic_manager: L{coilmq.topic.TopicManager}
    
    @ivar transactions: Active transactions for this connection.
    @type transactions: C{dict} of C{str} to C{list} 
    
    @ivar connected: Whether engine is connected.
    @type connected: C{bool}
    """
    def __init__(self, connection, authenticator, queue_manager, topic_manager):
        """
        @param connection: The stomp connection backing this engine.
        @type connection: L{coilmq.server.StompConnection}
        """
        self.log = logging.getLogger('%s.%s' % (self.__class__.__module__, self.__class__.__name__))
        self.connection = connection
        self.authenticator = authenticator
        self.queue_manager = queue_manager
        self.topic_manager = topic_manager
        self.connected = False
        self.transactions = defaultdict(list)
    
    def processFrame(self, frame):
        """
        Dispatches a received frame to the appropriate internal method.
        
        @param frame: The frame that was received.
        @type frame: L{coilmq.frame.StompFrame} 
        """
        cmd_method = frame.cmd.lower()
        
        if not cmd_method in VALID_COMMANDS:
            raise ProtocolError("Invalid STOMP command: %s" % frame.cmd)
        
        method = getattr(self, cmd_method, None)
        
        if not self.connected and method != self.connect:
            raise ProtocolError("Not connected.")
        
        try:
            transaction = frame.headers.get('transaction')
            if not transaction or method in (self.begin, self.commit, self.abort):
                method(frame) 
            else:
                if not transaction in self.transactions:
                    raise ProtocolError("Invalid transaction specified: %s" % transaction)
                self.transactions[transaction].append(frame) 
        except Exception, e:
            self.log.error("Error processing STOMP frame: %s" % e)
            self.log.exception(e)
            try:
                self.connection.send_frame(ErrorFrame(str(e), str(e)))
            except Exception, e:
                self.log.error("Could not send error frame: %s" % e)
                self.log.exception(e)
        
    def connect(self, frame):
        """
        Handle CONNECT command: Establishes a new connection and checks auth (if applicable).
        """
        self.log.debug("CONNECT")
        
        if self.authenticator:
            login = frame.headers.get('login')
            passcode = frame.headers.get('passcode')
            if not self.authenticator.authenticate(login, passcode):
                raise AuthError("Authentication failed for %s" % login)
        
        self.connected = True
        
        # TODO: Do we want to do anything special to track sessions?
        # (Actually, I don't think the spec actually does anything with this at all.)
        self.connection.send_frame(ConnectedFrame(session=uuid.uuid4()))
    
    def send(self, frame):
        """
        Handle the SEND command: Delivers a message to a queue or topic (default).
        """
        dest = frame.headers.get('destination')
        if not dest:
            raise ProtocolError('Missing destination for SEND command.')
        
        if dest.startswith('/queue/'):
            self.queue_manager.send(frame)
        else:
            self.topic_manager.send(frame)
            
    def subscribe(self, frame):
        """
        Handle the SUBSCRIBE command: Adds this connection to destination.
        """
        ack = frame.headers.get('ack')
        reliable = ack and ack.lower() == 'client'
        
        self.connection.reliable_subscriber = reliable
        
        dest = frame.headers.get('destination')
        if not dest:
            raise ProtocolError('Missing destination for SUBSCRIBE command.')
        
        if dest.startswith('/queue/'):
            self.queue_manager.subscribe(self.connection, dest)
        else:
            self.topic_manager.subscribe(self.connection, dest)
            
    def unsubscribe(self, frame):
        """
        Handle the UNSUBSCRIBE command: Removes this connection from destination.
        """
        dest = frame.headers.get('destination')
        if not dest:
            raise ProtocolError('Missing destination for UNSUBSCRIBE command.')
        
        if dest.startswith('/queue/'):
            self.queue_manager.unsubscribe(self.connection, dest)
        else:
            self.topic_manager.unsubscribe(self.connection, dest)
    
    def begin(self, frame):
        """
        Handles BEGING command: Starts a new transaction.
        """
        if not frame.transaction:
            raise ProtocolError("Missing transaction for BEGIN command.")
        
        self.transactions[frame.transaction] = []
    
    def commit(self, frame):
        """
        Handles COMMIT command: Commits specified transaction.
        """
        if not frame.transaction:
            raise ProtocolError("Missing transaction for COMMIT command.")
        
        if not frame.transaction in self.transactions:
            raise ProtocolError("Invalid transaction: %s" % frame.transaction)
        
        for tframe in self.transactions[frame.transaction]:
            del tframe.headers['transaction']
            print "Processing frame: %s" % tframe
            self.processFrame(tframe)
        
        self.queue_manager.clearTransactionFrames(self.connection, frame.transaction)
        del self.transactions[frame.transaction]
    
    def abort(self, frame):
        """
        Handles ABORT command: Rolls back specified transaction.
        """
        if not frame.transaction:
            raise ProtocolError("Missing transaction for ABORT command.")
        
        if not frame.transaction in self.transactions:
            raise ProtocolError("Invalid transaction: %s" % frame.transaction)
        
        self.queue_manager.resend_transaction_frames(self.connection, frame.transaction)
        del self.transactions[frame.transaction]
        
    def ack(self, frame):
        """
        Handles the ACK command: Acknowledges receipt of a message.
        """
        if not frame.message_id:
            raise ProtocolError("No message-id specified for ACK command.")
        self.queue_manager.ack(self.connection, frame)
        
    def disconnect(self, frame):
        """
        Handles the DISCONNECT command: Unbinds the connection. 
        
        Clients are supposed to send this command, but in practice it should not be
        relied upon.
        """
        self.log.debug("Disconnect")
        self.unbind()
    
    def unbind(self):
        """
        Unbinds this connection from queue and topic managers (freeing up resources)
        and resets state.
        """
        self.connected = False
        self.queue_manager.disconnect(self.connection)
        self.topic_manager.disconnect(self.connection)
    