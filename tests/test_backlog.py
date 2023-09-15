from asyncio import sleep
from datetime import timedelta
from typing import NamedTuple

from pytest import mark

from sharded_queue import BacklogHandler, Handler, Queue, Route, Tube, Worker
from sharded_queue.drivers import RuntimeLock, RuntimeStorage
from sharded_queue.protocols import Storage


class BucketRequest(NamedTuple):
    bucket: int


class DropBucket(Handler):
    async def handle(self, *requests: BucketRequest) -> None:
        pass


@mark.asyncio
async def test_backlog() -> None:
    storage: Storage = RuntimeStorage()
    queue: Queue = Queue(storage)
    await queue.register(
        DropBucket,
        BucketRequest(1),
        delay=timedelta(milliseconds=10),
    )

    backlog_pipe: str = Tube(BacklogHandler, Route()).pipe
    drop_pipe: str = Tube(DropBucket, Route()).pipe

    assert await queue.storage.length(drop_pipe) == 0
    assert await queue.storage.length(backlog_pipe) == 1

    await Worker(RuntimeLock(), queue).loop(1)
    assert await queue.storage.length(drop_pipe) == 0
    assert await queue.storage.length(backlog_pipe) == 1

    await sleep(0.01)

    await Worker(RuntimeLock(), queue).loop(1)
    assert await queue.storage.length(drop_pipe) == 1
    assert await queue.storage.length(backlog_pipe) == 0

    await Worker(RuntimeLock(), queue).loop(1)
    assert await queue.storage.length(drop_pipe) == 0
    assert await queue.storage.length(backlog_pipe) == 0
