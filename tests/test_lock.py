from asyncio import gather, sleep

from pytest import mark
from redis.asyncio import Redis

from sharded_queue import Handler, Queue, Route, Tube, Worker
from sharded_queue.drivers import RedisLock, RuntimeLock, RuntimeStorage
from sharded_queue.protocols import Lock


class Request:
    ...


class TestHandler(Handler):
    async def handle(self, *requests: Request) -> None:
        ...


@mark.asyncio
async def test_lock_prolongate() -> None:
    queue: Queue = Queue(RuntimeStorage())
    redis: Redis = Redis(decode_responses=True)
    await redis.flushall()
    worker: Worker = Worker(RedisLock(redis), queue)
    pipe: str = worker.lock.settings.prefix + Tube(TestHandler, Route()).pipe

    async def register_task():
        ttl1 = await redis.ttl(pipe)
        await sleep(1)
        ttl2 = await redis.ttl(pipe)
        assert ttl1 == ttl2, 'ttl is updated'
        await queue.register(TestHandler, Request())

    await queue.register(TestHandler, Request())
    await gather(worker.loop(2), register_task())
    await redis.close()


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
