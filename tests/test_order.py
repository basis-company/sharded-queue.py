from typing import NamedTuple

from pytest import mark

from sharded_queue import (Handler, JsonTupleSerializer, Queue, Route,
                           RuntimeCoordinator, RuntimeStorage, Tube, Worker)


class TimesheetSign(NamedTuple):
    timesheet: int


processed: list[TimesheetSign] = []


class TimesheetSignHandler(Handler):
    orders = ['q', 'w', 'e']

    @classmethod
    async def route(cls, *requests: TimesheetSign) -> list[Route]:
        mapping: dict[int, str] = {
            1: 'e',
            2: 'w',
            3: 'q',
        }

        return [
            Route(order=mapping[r.timesheet]) for r in requests
        ]

    async def handle(self, *requests: TimesheetSign) -> None:
        processed.extend(requests)


def get_pipe(order: str) -> str:
    return Tube(TimesheetSignHandler, Route('0', order)).pipe


@mark.asyncio
async def test_orders() -> None:
    storage = RuntimeStorage()
    queue: Queue = Queue(JsonTupleSerializer(), storage)
    await queue.register(TimesheetSignHandler, *[
        TimesheetSign(n) for n in range(1, 4)
    ])

    worker = Worker(RuntimeCoordinator(), queue)
    await worker.loop(1)
    assert len(processed) == 1

    assert await storage.length(get_pipe('q')) == 0
    assert await storage.length(get_pipe('w')) == 1
    assert await storage.length(get_pipe('e')) == 1

    await worker.loop(1)
    assert len(processed) == 2

    assert await storage.length(get_pipe('q')) == 0
    assert await storage.length(get_pipe('w')) == 0
    assert await storage.length(get_pipe('e')) == 1

    await worker.loop(1)
    assert len(processed) == 3

    assert await storage.length(get_pipe('q')) == 0
    assert await storage.length(get_pipe('w')) == 0
    assert await storage.length(get_pipe('e')) == 0


