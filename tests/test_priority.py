from typing import NamedTuple

from pytest import mark

from sharded_queue import Handler, Queue, Route, Tube, Worker
from sharded_queue.drivers import RuntimeLock, RuntimeStorage


class TimesheetSign(NamedTuple):
    timesheet: int


processed: list[TimesheetSign] = []


class TimesheetSignHandler(Handler):
    priorities: list[int] = [3, 2, 1]

    @classmethod
    async def route(cls, *requests: TimesheetSign) -> list[Route]:
        return [
            Route(priority=r.timesheet) for r in requests
        ]

    async def handle(self, *requests: TimesheetSign) -> None:
        processed.extend(requests)


def get_pipe(priority: int) -> str:
    return Tube(TimesheetSignHandler, Route(0, priority)).pipe


@mark.asyncio
async def test_piorities() -> None:
    storage = RuntimeStorage()
    queue: Queue = Queue(storage)
    await queue.register(TimesheetSignHandler, *[
        TimesheetSign(n) for n in range(1, 4)
    ])

    worker = Worker(RuntimeLock(), queue)
    await worker.loop(1)
    assert len(processed) == 1

    assert await storage.length(get_pipe(1)) == 1
    assert await storage.length(get_pipe(2)) == 1
    assert await storage.length(get_pipe(3)) == 0

    await worker.loop(1)
    assert len(processed) == 2

    assert await storage.length(get_pipe(1)) == 1
    assert await storage.length(get_pipe(2)) == 0
    assert await storage.length(get_pipe(3)) == 0

    await worker.loop(1)
    assert len(processed) == 3

    assert await storage.length(get_pipe(1)) == 0
    assert await storage.length(get_pipe(2)) == 0
    assert await storage.length(get_pipe(3)) == 0
