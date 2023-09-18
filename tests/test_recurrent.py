from asyncio import sleep
from datetime import timedelta
from typing import NamedTuple

from pytest import mark

from sharded_queue import (DeferredHandler, Handler, Queue, RecurrentHandler,
                           Route, Tube, Worker)
from sharded_queue.drivers import RuntimeLock, RuntimeStorage
from sharded_queue.protocols import Lock, Storage
from sharded_queue.settings import settings


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

    settings.deferred_retry_delay = 0
    settings.recurrent_check_interval = 1

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
        ValidateAccess, CompanyRequest(1), recurrent=timedelta(milliseconds=10)
    )
    assert await stats() == (0, 1, 0), 'recurrent pipe contains request'

    await Worker(lock, queue).loop(1)
    assert await stats() == (1, 1, 0), 'added defered request'
    assert await lock.exists(recurrent_pipe)

    await lock.release(recurrent_pipe)
    await Worker(lock, queue).loop(1, handler=RecurrentHandler)
    assert await stats() == (1, 1, 0), 'no deffered duplicates'

    await sleep(0.01)

    await lock.release(recurrent_pipe)
    await Worker(lock, queue).loop(1, handler=RecurrentHandler)
    assert await stats() == (1, 1, 0), 'no deffered duplicates'

    await Worker(lock, queue).loop(1, handler=DeferredHandler)
    assert await stats() == (0, 1, 1), 'deferred processed, validation added'

    await lock.release(recurrent_pipe)
    await Worker(lock, queue).loop(1, handler=RecurrentHandler)
    assert await stats() == (1, 1, 1), 'deferred added'

    await sleep(0.01)

    await Worker(lock, queue).loop(1, handler=DeferredHandler)
    assert await stats() == (0, 1, 1), 'no validation duplicate'
