from asyncio import sleep
from datetime import timedelta
from typing import NamedTuple

from pytest import mark

from sharded_queue import DeferredHandler, Handler, Queue, Route, Tube, Worker
from sharded_queue.drivers import RuntimeLock, RuntimeStorage
from sharded_queue.protocols import Storage
from sharded_queue.settings import settings


class BucketRequest(NamedTuple):
    bucket: int


class DropBucket(Handler):
    async def handle(self, *requests: BucketRequest) -> None:
        pass


@mark.asyncio
async def test_deferred() -> None:
    storage: Storage = RuntimeStorage()
    queue: Queue = Queue(storage)
    await queue.register(
        DropBucket,
        BucketRequest(1),
        defer=timedelta(milliseconds=10),
    )

    deferred_pipe: str = Tube(DeferredHandler, Route()).pipe
    drop_pipe: str = Tube(DropBucket, Route()).pipe
    settings.deferred_retry_delay = 0

    assert await queue.storage.length(drop_pipe) == 0
    assert await queue.storage.length(deferred_pipe) == 1

    await Worker(RuntimeLock(), queue).loop(1)
    assert await queue.storage.length(drop_pipe) == 0
    assert await queue.storage.length(deferred_pipe) == 1

    await sleep(0.01)

    await Worker(RuntimeLock(), queue).loop(1)
    assert await queue.storage.length(drop_pipe) == 1
    assert await queue.storage.length(deferred_pipe) == 0

    await Worker(RuntimeLock(), queue).loop(1)
    assert await queue.storage.length(drop_pipe) == 0
    assert await queue.storage.length(deferred_pipe) == 0
