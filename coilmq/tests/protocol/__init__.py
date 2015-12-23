import unittest

from coilmq.engine import StompEngine
from coilmq.protocol import STOMP11, STOMP12
from coilmq.tests.mock import (MockAuthenticator, MockConnection, MockQueueManager, MockTopicManager)
from coilmq.util.frames import Frame


class ProtocolBaseTestCase(unittest.TestCase):

    def get_protocol(self):
        return STOMP12

    def setUp(self):
        self.qm = MockQueueManager()
        self.tm = MockTopicManager()
        self.conn = MockConnection()
        self.auth = MockAuthenticator()
        self.engine = StompEngine(connection=self.conn,
                                  queue_manager=self.qm,
                                  topic_manager=self.tm,
                                  authenticator=None,
                                  protocol=self.get_protocol())

    def feed_frame(self, cmd, headers=None, body=''):
        self.engine.process_frame(Frame(cmd, headers or {}, body))
        return self.conn.frames[-1]

    def tearDown(self):
        self.conn.reset()
