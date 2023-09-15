from asyncio import get_event_loop, sleep
from typing import NamedTuple

from pytest import mark

from sharded_queue import Handler, Queue, Worker
from sharded_queue.drivers import RuntimeLock, RuntimeStorage
from sharded_queue.settings import settings


class SignContractRequest(NamedTuple):
    document: int


signed: list[SignContractRequest] = []


class SignContract(Handler):
    async def handle(self, *requests: SignContractRequest) -> None:
        signed.extend(requests)


@mark.asyncio
async def test_worker_pause() -> None:
    settings.worker_empty_pause = 0.1
    queue: Queue = Queue(RuntimeStorage())
    working_loop = Worker(RuntimeLock(), queue).loop(2)
    worker_task = get_event_loop().create_task(working_loop)
    get_event_loop().create_task(working_loop)
    await queue.register(SignContract, SignContractRequest(1))
    await sleep(0.2)
    assert len(signed) == 1
    await queue.register(SignContract, SignContractRequest(2))
    await worker_task
    assert len(signed) == 2
