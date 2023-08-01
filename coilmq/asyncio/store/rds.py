try:
    import redis.asyncio
except ImportError:  # pragma: no cover
    import sys; sys.exit('please, install redis-py package to use redis-store')
import pickle

from coilmq.store import QueueStore
from coilmq.config import config

__authors__ = ('"Hans Lellelid" <hans@xmpl.org>', '"Alexander Zhukov" <zhukovaa90@gmail.com>')
__copyright__ = "Copyright 2009 Hans Lellelid"
__license__ = """Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""


def make_redis_store(cfg=None):
    return RedisQueueStore(
        redis_conn=redis.asyncio.Redis(**dict((cfg or config).items('redis'))))


class RedisQueueStore(QueueStore):
    """Simple Queue with Redis Backend"""
    def __init__(self, redis_conn=None):
        """The default connection parameters are: host='localhost', port=6379, db=0"""
        self.__db = redis_conn or redis.asyncio.Redis()
        # self.key = '{0}:{1}'.format(namespace, name)
        super(RedisQueueStore, self).__init__()

    async def enqueue(self, destination, frame):
        await self.__db.rpush(destination, pickle.dumps(frame))

    async def dequeue(self, destination):
        item = await self.__db.lpop(destination)
        if item:
            return pickle.loads(item)

    async def requeue(self, destination, frame):
        await self.enqueue(destination, frame)

    async def size(self, destination):
        return await self.__db.llen(destination)

    async def has_frames(self, destination):
        return await self.size(destination) > 0

    async def destinations(self):
        return await self.__db.keys()
