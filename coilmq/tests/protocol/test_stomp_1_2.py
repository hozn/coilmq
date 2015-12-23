import socket

from coilmq.tests.protocol import ProtocolBaseTestCase
from coilmq.util import frames


class STOMP12TestCase(ProtocolBaseTestCase):

    def test_host_valid(self):
        host = socket.getfqdn()
        response = self.feed_frame(frames.CONNECT, {'host': host, 'accept-version': '1.2'})
        self.assertEqual(response.cmd, frames.CONNECTED)

    def test_host_invalid(self):
        host = 'nothing.nowhere.com'
        response = self.feed_frame(frames.CONNECT, {'host': host, 'accept-version': '1.2'})
        self.assertEqual(response.cmd, frames.ERROR.lower())
        self.assertEqual(response.headers['message'], 'Virtual hosting is not supported or host is unknown')

    def test_no_host_header(self):
        response = self.feed_frame(frames.CONNECT, {'accept-version': '1.2'})
