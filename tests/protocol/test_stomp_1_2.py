import socket

from coilmq.protocol import STOMP11
from coilmq.util import frames
from tests.protocol import ProtocolBaseTestCase


class STOMP12TestCase(ProtocolBaseTestCase):

    def setUp(self):
        super(STOMP12TestCase, self).setUp()
        self.host = socket.getfqdn()

    def test_host_valid(self):
        response = self.feed_frame(frames.CONNECT, {'host': self.host, 'accept-version': '1.2'})
        self.assertEqual(response.cmd, frames.CONNECTED)

    def test_host_invalid(self):
        host = 'nothing.nowhere.com'
        response = self.feed_frame(frames.CONNECT, {'host': host, 'accept-version': '1.2'})
        self.assertEqual(response.cmd, frames.ERROR)
        self.assertEqual(response.headers['message'], 'Virtual hosting is not supported or host is unknown')

    def test_no_host_header(self):
        response = self.feed_frame(frames.CONNECT, {'accept-version': '1.2'})
        self.assertEqual(response.cmd, frames.ERROR)
        self.assertEqual(response.headers['message'], '`host` header is required for `CONNECT` command')

    def test_protocol_downgrade(self):
        response = self.feed_frame(frames.CONNECT, {'host': self.host, 'accept-version': '1.1'})
        self.assertEqual(response.cmd, frames.CONNECTED)
        self.assertEqual(response.headers['version'], '1.1')
        self.assertEqual(type(self.engine.protocol), STOMP11)
