'''
The TAC file can be used with the twistd script to start this application.
'''
import logging

from twisted.application import internet, service
from twisted.internet import protocol
from twisted.python import log

# Do the logging setup before we import other packages.

observer = log.PythonLoggingObserver()
observer.start()

# TODO: Replace with config-based setup
logging.basicConfig(level=logging.DEBUG)

from coilmq.server import StompFactory
from coilmq.queue import QueueManager
from coilmq.topic import TopicManager

# FIXME: Replace with config-based
from coilmq.store.memory import MemoryQueue
from coilmq.scheduler import FavorReliableSubscriberScheduler, RandomQueueScheduler

application = service.Application('CoilMQ')

factory = StompFactory(queue_manager=QueueManager(store=MemoryQueue(),
                                                  subscriber_scheduler=FavorReliableSubscriberScheduler(),
                                                  queue_scheduler=RandomQueueScheduler()),
                       topic_manager=TopicManager())

server = internet.TCPServer(61613, factory)
server.setServiceParent(application)