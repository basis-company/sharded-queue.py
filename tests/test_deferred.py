from asyncio import sleep
from datetime import timedelta
from typing import NamedTuple

from pytest import mark

from sharded_queue import DeferredHandler, Handler, Queue, Route, Tube, Worker
from sharded_queue.drivers import RuntimeLock, RuntimeStorage
from sharded_queue.protocols import Storage


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
    worker = Worker(RuntimeLock(), queue)
    worker.settings.deferred_retry_delay = 0

    assert await queue.storage.length(drop_pipe) == 0
    assert await queue.storage.length(deferred_pipe) == 1

    await queue.register(
        DropBucket,
        BucketRequest(1),
        defer=timedelta(milliseconds=5),
    )

    assert await queue.storage.length(drop_pipe) == 0
    assert await queue.storage.length(deferred_pipe) == 2

    append_order = await queue.storage.range(deferred_pipe, 2)

    await worker.loop(2)
    assert await queue.storage.length(drop_pipe) == 0
    assert await queue.storage.length(deferred_pipe) == 2

    timestamp_order = await queue.storage.range(deferred_pipe, 2)
    assert list(reversed(append_order)) == timestamp_order

    await sleep(0.01)

    await worker.loop(2)
    assert await queue.storage.length(drop_pipe) == 1
    assert await queue.storage.length(deferred_pipe) == 0

    await worker.loop(1)
    assert await queue.storage.length(drop_pipe) == 0
    assert await queue.storage.length(deferred_pipe) == 0
