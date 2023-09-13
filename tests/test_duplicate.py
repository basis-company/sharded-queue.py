from dataclasses import dataclass

from pytest import mark

from sharded_queue import Handler, Queue, Route, Tube
from sharded_queue.drivers import RuntimeStorage


@dataclass
class CompanyRequest:
    company: int


class ProlongateContracts(Handler):
    async def handle(self, *requests: CompanyRequest) -> None:
        pass


@mark.asyncio
async def test_duplicate() -> None:
    storage = RuntimeStorage()
    queue: Queue = Queue(storage)

    await queue.register(
        ProlongateContracts,
        CompanyRequest(1),
        if_not_exists=True,
    )

    assert 1 == await storage.length(Tube(ProlongateContracts, Route()).pipe)

    await queue.register(
        ProlongateContracts,
        CompanyRequest(1),
        if_not_exists=True,
    )

    assert 1 == await storage.length(Tube(ProlongateContracts, Route()).pipe)

    await queue.register(
        ProlongateContracts,
        CompanyRequest(1),
        if_not_exists=False,
    )

    assert 2 == await storage.length(Tube(ProlongateContracts, Route()).pipe)
