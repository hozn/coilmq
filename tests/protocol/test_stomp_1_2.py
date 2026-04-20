import socket

from coilmq.protocol import STOMP11
from coilmq.util import frames
from tests.protocol import ProtocolBaseTestCase


class STOMP12TestCase(ProtocolBaseTestCase):
    def setUp(self):
        super().setUp()
        self.host = socket.getfqdn()

    def test_host_valid(self):
        response = self.feed_frame(
            frames.CONNECT, {"host": self.host, "accept-version": "1.2"}
        )
        assert response.cmd == frames.CONNECTED

    def test_host_invalid(self):
        host = "nothing.nowhere.com"
        response = self.feed_frame(
            frames.CONNECT, {"host": host, "accept-version": "1.2"}
        )
        assert response.cmd == frames.ERROR
        assert (
            response.headers["message"]
            == "Virtual hosting is not supported or host is unknown"
        )

    def test_no_host_header(self):
        response = self.feed_frame(frames.CONNECT, {"accept-version": "1.2"})
        assert response.cmd == frames.ERROR
        assert response.headers["message"] == '"host" header is required'

    def test_protocol_downgrade(self):
        response = self.feed_frame(
            frames.CONNECT, {"host": self.host, "accept-version": "1.1"}
        )
        assert response.cmd == frames.CONNECTED
        assert response.headers["version"] == "1.1"
        # Note: assertIsInstance is not appropriate here because the test would
        # pass if self.engine.protocol was STOMP10 which is not what we want.
        # `assertIs(type(obj), cls)` is appropriate way to check for an object's
        # exact type.
        assert type(self.engine.protocol) is STOMP11
