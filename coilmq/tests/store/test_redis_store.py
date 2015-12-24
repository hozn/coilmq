import unittest

import fakeredis

from coilmq.tests.store import CommonQueueTest

from coilmq.store.rds import RedisQueueStore


class RedisStoreTestCase(CommonQueueTest, unittest.TestCase):

    def setUp(self):
        self.store = RedisQueueStore(redis_conn=fakeredis.FakeStrictRedis())
