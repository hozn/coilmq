import time
import unittest
from contextlib import contextmanager

from coilmq.engine import StompEngine
from coilmq.exception import ProtocolError
from coilmq.protocol import STOMP11
from coilmq.tests.mock import (MockAuthenticator, MockConnection, MockQueueManager, MockTopicManager)
from coilmq.util.frames import Frame, ErrorFrame
from coilmq.util import frames


class STOMP11TestCase(unittest.TestCase):

    def setUp(self):
        self.qm = MockQueueManager()
        self.tm = MockTopicManager()
        self.conn = MockConnection()
        self.auth = MockAuthenticator()
        self.engine = StompEngine(connection=self.conn,
                                  queue_manager=self.qm,
                                  topic_manager=self.tm,
                                  authenticator=None)

        self.engine.protocol = STOMP11(self.engine, send_heartbeat_interval=1)

    def get_engine(self):
        return StompEngine(connection=self.conn,
                           queue_manager=self.qm,
                           topic_manager=self.tm,
                           authenticator=None)

    @contextmanager
    def with_heartbeat(self, protocol):
        try:
            old = self.engine.protocol
            self.engine.protocol = protocol
            yield
        finally:
            protocol.disable_heartbeat()

    def test_heartbeat_from_server(self):
        with self.with_heartbeat(self.engine.protocol):
            self.engine.process_frame(Frame(frames.CONNECT, headers={'heart-beat': '0,100', 'accept-version': '1.1'}))
            time.sleep(0.53)
            heartbeats = [frame for frame in self.conn.frames if frame.headers.get('message') == 'heartbeat']
            self.assertAlmostEqual(len(heartbeats), 5, delta=1)

    def test_no_heartbeat_from_client(self):
        with self.with_heartbeat(STOMP11(self.engine, receive_heartbeat_interval=50)):
            self.engine.process_frame(Frame(frames.CONNECT, headers={'heart-beat': '50,50', 'accept-version': '1.1'}))
            self.assertTrue(self.engine.connected)
            self.assertTrue(self.engine.protocol.timer._running)
            time.sleep(0.53)
            self.assertFalse(self.engine.connected)

    def test_no_heartbeat(self):
        with self.with_heartbeat(STOMP11(self.engine)):
            self.engine.process_frame(Frame(frames.CONNECT, headers={'heart-beat': '0,0', 'accept-version': '1.1'}))
            self.assertTrue(self.engine.connected)
            self.assertTrue(self.engine.protocol.timer._running)

    def test_protocol_version_common_exists(self):
        self.engine.process_frame(Frame(frames.CONNECT, headers={'heart-beat': '0,0', 'accept-version': '1.1'}))
        self.assertEqual(self.conn.frames[-1].cmd, frames.CONNECTED)
        self.assertIn('version', self.conn.frames[-1].headers)

    def test_nack_valid_frame(self):
        self.engine.connected = True
        self.engine.process_frame(Frame(frames.NACK, headers={'message-id': 1, 'subscription': 'foo'}))
        # just make sure it works
        self.assertTrue(1)

    def test_nack_invalid_frame(self):
        self.engine.connected = True

        self.engine.process_frame(Frame(frames.NACK, headers={'subscription': 'foo'}))
        self.assertIsInstance(self.conn.frames[-1], ErrorFrame)
        self.engine.process_frame(Frame(frames.NACK, headers={'message-id': 1}))
        self.assertIsInstance(self.conn.frames[-1], ErrorFrame)

    def tearDown(self):
        self.conn.reset()
