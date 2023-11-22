from asyncio import sleep
from datetime import datetime, timedelta
from typing import NamedTuple

from pytest import mark

from sharded_queue import (DeferredHandler, DeferredRequest, Handler, Queue,
                           RecurrentHandler, RecurrentRequest, Route, Tube,
                           Worker)
from sharded_queue.drivers import RuntimeLock, RuntimeStorage
from sharded_queue.protocols import Lock, Storage


class CompanyRequest(NamedTuple):
    company: int


class ValidateAccess(Handler):
    async def handle(self, *requests: CompanyRequest) -> None:
        pass


@mark.asyncio
async def test_recurrent() -> None:
    storage: Storage = RuntimeStorage()
    lock: Lock = RuntimeLock()
    queue: Queue = Queue(storage)

    worker = Worker(lock, queue)
    worker.settings.deferred_retry_delay = 0
    worker.settings.recurrent_check_interval = 1

    deferred_pipe: str = Tube(DeferredHandler, Route()).pipe
    recurrent_pipe: str = Tube(RecurrentHandler, Route()).pipe
    validatation_pipe: str = Tube(ValidateAccess, Route()).pipe

    async def stats() -> tuple[int, int, int]:
        return (
            await queue.storage.length(deferred_pipe),
            await queue.storage.length(recurrent_pipe),
            await queue.storage.length(validatation_pipe),
        )

    await queue.register(
        ValidateAccess, CompanyRequest(1), recurrent=timedelta(seconds=1)
    )
    assert await stats() == (0, 1, 0), 'recurrent pipe contains request'

    deffered_registration = datetime.now().timestamp()
    await Worker(lock, queue).loop(1)
    assert await stats() == (1, 1, 0), 'added defered request'
    assert await lock.exists(recurrent_pipe)

    [deferred] = await queue.storage.range(deferred_pipe, 1)
    request = queue.serializer.deserialize(DeferredRequest, deferred)
    assert request.timestamp >= deffered_registration

    await lock.release(recurrent_pipe)
    await Worker(lock, queue).loop(1, handler=RecurrentHandler)
    assert await stats() == (1, 1, 0), 'no deffered duplicates'

    await sleep(1)

    await lock.release(recurrent_pipe)
    await Worker(lock, queue).loop(1, handler=RecurrentHandler)
    assert await stats() == (1, 1, 0), 'no deffered duplicates'

    await worker.loop(1, handler=DeferredHandler)
    assert await stats() == (0, 1, 1), 'deferred processed, validation added'

    await lock.release(recurrent_pipe)
    await worker.loop(1, handler=RecurrentHandler)
    assert await stats() == (1, 1, 1), 'deferred added'

    await sleep(1)

    await worker.loop(1, handler=DeferredHandler)
    assert await stats() == (0, 1, 1), 'no validation duplicate'

    [recurrent] = await queue.storage.range(recurrent_pipe, 1)
    request = queue.serializer.deserialize(RecurrentRequest, recurrent)
    assert request.interval == 1
    await queue.register(
        ValidateAccess, CompanyRequest(1), recurrent=timedelta(seconds=2)
    )

    assert await queue.storage.length(recurrent_pipe) == 1

    [recurrent] = await queue.storage.range(recurrent_pipe, 1)
    request = queue.serializer.deserialize(RecurrentRequest, recurrent)
    assert request.interval == 2
