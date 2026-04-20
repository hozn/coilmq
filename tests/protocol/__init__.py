from coilmq.engine import StompEngine
from coilmq.protocol import STOMP12
from coilmq.util.frames import Frame
from tests.mock import (
    MockAuthenticator,
    MockConnection,
    MockQueueManager,
    MockTopicManager,
)


class ProtocolTestsFixture:
    def get_protocol(self):
        return STOMP12

    def setup_method(self, method):
        self.qm = MockQueueManager()
        self.tm = MockTopicManager()
        self.conn = MockConnection()
        self.auth = MockAuthenticator()
        self.engine = StompEngine(
            connection=self.conn,
            queue_manager=self.qm,
            topic_manager=self.tm,
            authenticator=None,
            protocol=self.get_protocol(),
        )

    def feed_frame(self, cmd, headers=None, body=""):
        self.engine.process_frame(Frame(cmd, headers or {}, body))
        return self.conn.frames[-1]

    def teardown_method(self, method):
        self.conn.reset()
