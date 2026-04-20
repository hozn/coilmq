from coilmq.server.socket_server import ThreadedStompServer
from coilmq.start import server_from_config


class TestStart:
    def test_server_from_config_default(self):

        assert isinstance(server_from_config(), ThreadedStompServer)
