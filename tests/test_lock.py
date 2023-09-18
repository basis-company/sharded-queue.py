from pytest import mark
from redis.asyncio import Redis

from sharded_queue.drivers import RedisLock, RuntimeLock
from sharded_queue.protocols import Lock


@mark.asyncio
async def test_runtime_storage() -> None:
    await runner(RuntimeLock())


@mark.asyncio
async def test_redis_storage() -> None:
    redis: Redis = Redis(decode_responses=True)
    await redis.flushall()
    await runner(RedisLock(redis))
    await redis.close()


async def runner(lock: Lock):
    assert await lock.acquire('tester')
    assert await lock.exists('tester')
    assert not await lock.acquire('tester')
    assert await lock.exists('tester')
    assert await lock.ttl('tester', 1)

    await lock.release('tester')
    assert not await lock.exists('tester')
    assert not await lock.ttl('tester', 1)

    assert await lock.acquire('tester')
    await lock.release('tester')
