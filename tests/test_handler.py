from pytest import mark, raises

from sharded_queue import Handler, Queue, Worker
from sharded_queue.drivers import RuntimeLock, RuntimeStorage
from sharded_queue.protocols import Storage


class AbstractRequest:
    ...


class AbstractHandler(Handler):
    ...


@mark.asyncio
async def test_handler() -> None:
    storage: Storage = RuntimeStorage()
    queue: Queue = Queue(storage)
    await queue.register(AbstractHandler, AbstractRequest())

    with raises(NotImplementedError):
        await Worker(RuntimeLock(), queue).loop(1)
