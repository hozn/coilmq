from configparser import ConfigParser
from unittest import mock

import fakeredis

from coilmq.store.rds import RedisQueueStore, make_redis_store
from tests.store import BaseQueueTests


class TestRedisQueueStore(BaseQueueTests):
    def setup_method(self, method):
        self.store = RedisQueueStore(redis_conn=fakeredis.FakeStrictRedis())


@mock.patch("coilmq.store.rds.redis.Redis", fakeredis.FakeStrictRedis)
def test_make_redis_store():
    config = ConfigParser()
    config.add_section("redis")
    config.set("redis", "host", value="localhost")
    config.set("redis", "port", value="28222")

    store = make_redis_store(cfg=config)
    assert isinstance(store, RedisQueueStore)
