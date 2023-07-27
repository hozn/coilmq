"""
Non-durable topic support functionality.

This code is inspired by the design of the Ruby stompserver project, by 
Patrick Hurley and Lionel Bouton.  See http://stompserver.rubyforge.org/
"""
import logging
import uuid
from copy import deepcopy
from coilmq.server import StompConnection
from coilmq.asyncio.subscription import SubscriptionManager, DEFAULT_SUBSCRIPTION_ID
from coilmq.util.frames import MESSAGE, Frame

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


class TopicManager(object):
    """
    Class that manages distribution of messages to topic subscribers.


    @ivar _subscriptions: A dict of registered topics, keyed by destination.
    @type _subscriptions: C{dict} of C{str} to C{set} of L{coilmq.server.StompConnection}
    """

    def __init__(self):
        self.log = logging.getLogger(
            '%s.%s' % (__name__, self.__class__.__name__))

        self._subscriptions = SubscriptionManager()

        # TODO: If we want durable topics, we'll need a store for topics.

    async def close(self):
        """
        Closes all resources associated with this topic manager.

        (Currently this is simply here for API conformity w/ L{coilmq.queue.QueueManager}.)
        """
        self.log.info("Shutting down topic manager.")  # pragma: no cover

    async def subscribe(self, connection: StompConnection, destination: str, id: str = None):
        """
        Subscribes a connection to the specified topic destination. 

        @param connection: The client connection to subscribe.
        @type connection: L{coilmq.server.StompConnection}

        @param destination: The topic destination (e.g. '/topic/foo')
        @type destination: C{str}

        @param id: subscription identifier (optional)
        @type id: C{str}
        """
        self.log.debug("Subscribing %s to %s" % (connection, destination))
        await self._subscriptions.subscribe(connection, destination, id=id)

    async def unsubscribe(self, connection: StompConnection, destination: str = None, id: str = None):
        """
        Unsubscribes a connection from the specified topic destination. 

        @param connection: The client connection to unsubscribe.
        @type connection: L{coilmq.server.StompConnection}

        @param destination: The topic destination (e.g. '/topic/foo') (optional)
        @type destination: C{str}

        @param id: subscription identifier (optional)
        @type id: C{str}
        """
        if id and not destination:
            self.log.debug("Unsubscribing %s for id %s" % (connection, id))
        else:
            self.log.debug("Unsubscribing %s from %s" % (connection, destination))
        await self._subscriptions.unsubscribe(connection, destination, id=id)

    async def disconnect(self, connection: StompConnection):
        """
        Removes a subscriber connection.

        @param connection: The client connection to unsubscribe.
        @type connection: L{coilmq.server.StompConnection}
        """
        self.log.debug("Disconnecting %s" % connection)
        await self._subscriptions.disconnect(connection)

    async def send(self, message: Frame):
        """
        Sends a message to all subscribers of destination.

        @param message: The message frame.  (The frame will be modified to set command 
                            to MESSAGE and set a message id.)
        @type message: L{stompclient.frame.Frame}
        """
        dest = message.headers.get('destination')
        if not dest:
            raise ValueError(
                "Cannot send frame with no destination: %s" % message)

        message.cmd = MESSAGE

        message.headers.setdefault('message-id', str(uuid.uuid4()))

        bad_subscribers = set()
        for subscriber in self._subscriptions.subscribers(dest):
            frame = deepcopy(message)
            if subscriber.id != DEFAULT_SUBSCRIPTION_ID:
                frame.headers["subscription"] = subscriber.id
            try:
                await subscriber.connection.send_frame(frame)
            except:
                self.log.exception(
                    "Error delivering message to subscriber %s; client will be disconnected." % subscriber)
                # We queue for deletion so we are not modifying the topics dict
                # while iterating over it.
                bad_subscribers.add(subscriber)

        for s in bad_subscribers:
            await self.unsubscribe(s.connection, dest, id=s.id)
