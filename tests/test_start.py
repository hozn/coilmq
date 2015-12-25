import unittest

from coilmq.server.socket_server import ThreadedStompServer
from coilmq.start import server_from_config

class GetServerTestCase(unittest.TestCase):

    def test_server_from_config_default(self):

        self.assertIsInstance(server_from_config(), ThreadedStompServer)