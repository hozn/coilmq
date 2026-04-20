from configparser import ConfigParser
from unittest import mock

import fakeredis
import pytest

from coilmq.store.rds import RedisQueueStore, make_redis_store
from tests.store import BaseQueueTests


@pytest.fixture
def store() -> RedisQueueStore:
    return RedisQueueStore(redis_conn=fakeredis.FakeStrictRedis())


class TestRedisQueueStore(BaseQueueTests):
    pass


@mock.patch("coilmq.store.rds.redis.Redis", fakeredis.FakeStrictRedis)
def test_make_redis_store() -> None:
    config = ConfigParser()
    config.add_section("redis")
    config.set("redis", "host", value="localhost")
    config.set("redis", "port", value="28222")

    store = make_redis_store(cfg=config)
    assert isinstance(store, RedisQueueStore)
