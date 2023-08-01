import time
from contextlib import contextmanager

from coilmq.protocol import STOMP11
from coilmq.util import frames
from coilmq.util.frames import Frame, ErrorFrame
from tests.protocol import ProtocolBaseTestCase


class STOMP11TestCase(ProtocolBaseTestCase):

    def get_protocol(self):
        return STOMP11

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
            self.assertAlmostEqual(self.conn.heartbeat_count, 5, delta=1)

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
