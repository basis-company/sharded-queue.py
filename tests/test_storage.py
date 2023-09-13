from pytest import mark
from redis.asyncio import Redis

from sharded_queue.drivers import RedisStorage, RuntimeStorage


@mark.asyncio
async def test_runtime_storage() -> None:
    await runner(RuntimeStorage())


@mark.asyncio
async def test_redis_storage() -> None:
    redis: Redis = Redis(decode_responses=True)
    await redis.flushall()
    await runner(RedisStorage(redis))
    await redis.close()


async def runner(storage) -> None:
    await storage.append('tester', 'q')
    assert not await storage.contains('tester', 'w')
    await storage.append('tester', 'w')
    assert await storage.contains('tester', 'w')
    await storage.append('tester', 'e')
    assert await storage.length('tester') == 3
    assert await storage.pipes() == ['tester']
    await storage.append('tester', 'r', 't', 'y')
    assert await storage.length('tester') == 6
    assert await storage.length('tester2') == 0
    assert await storage.range('tester', 1) == ['q']
    assert await storage.range('tester', 2) == ['q', 'w']
    assert await storage.range('tester', 3) == ['q', 'w', 'e']
    assert await storage.pop('tester', 1) == ['q']
    assert await storage.range('tester', 1) == ['w']
    assert await storage.range('tester', 2) == ['w', 'e']
    assert await storage.pop('tester', 2) == ['w', 'e']
    assert await storage.pop('tester', 10) == ['r', 't', 'y']
    assert await storage.pop('tester', 1) == []
