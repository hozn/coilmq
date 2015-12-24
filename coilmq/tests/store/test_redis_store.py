import unittest

import fakeredis

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

try:
    from unittest import mock
except ImportError:
    import mock

from coilmq.tests.store import CommonQueueTest
from coilmq.store.rds import RedisQueueStore, make_redis_store


class RedisStoreTestCase(CommonQueueTest, unittest.TestCase):

    def setUp(self):
        self.store = RedisQueueStore(redis_conn=fakeredis.FakeStrictRedis())


@mock.patch('coilmq.store.rds.redis.Redis', fakeredis.FakeStrictRedis)
class RedisStoreFactoryTestCase(unittest.TestCase):
    def test_from_config(self):

        config = ConfigParser()
        config.add_section('redis')
        config.set('redis', 'host', value='localhost')
        config.set('redis', 'port', value='28222')

        store = make_redis_store(cfg=config)
        self.assertIsInstance(store, RedisQueueStore)


