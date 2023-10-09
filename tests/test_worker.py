from asyncio import get_event_loop, sleep
from typing import NamedTuple

from pytest import mark, raises

from sharded_queue import Handler, Queue, Worker
from sharded_queue.drivers import RuntimeLock, RuntimeStorage


class SignContractRequest(NamedTuple):
    document: int


signed: list[SignContractRequest] = []


class SignContract(Handler):
    async def handle(self, *requests: SignContractRequest) -> None:
        signed.extend(requests)


@mark.asyncio
async def test_worker_pause() -> None:
    queue: Queue = Queue(RuntimeStorage())
    worker = Worker(RuntimeLock(), queue)
    worker.settings.empty_pause = 0.1
    working_loop = worker.loop(2)
    worker_task = get_event_loop().create_task(working_loop)
    get_event_loop().create_task(working_loop)
    await queue.register(SignContract, SignContractRequest(1))
    await sleep(0.2)
    assert len(signed) == 1
    await queue.register(SignContract, SignContractRequest(2))
    await worker_task
    assert len(signed) == 2


@mark.asyncio
async def test_worker_prolongate_invalid_pipe() -> None:
    queue: Queue = Queue(RuntimeStorage())
    worker = Worker(RuntimeLock(), queue)
    with raises(RuntimeError):
        await worker.prolongate_lock()
