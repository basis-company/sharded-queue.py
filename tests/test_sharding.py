from datetime import datetime
from typing import NamedTuple

from pytest import mark

from sharded_queue import Handler, Queue, Route, Tube, Worker, settings
from sharded_queue.drivers import RuntimeLock, RuntimeStorage


class Request(NamedTuple):
    bucket: int


context: dict[str, float] = {
    'started': 0,
    'stopped': 0
}

synced: list[Request] = []


class SyncHandler(Handler):
    @classmethod
    async def route(cls, *requests: Request) -> list[Route]:
        return [
            Route(thread=request.bucket % 2) for request in requests
        ]

    async def start(self) -> None:
        context['started'] = datetime.now().timestamp()

    async def stop(self) -> None:
        context['stopped'] = datetime.now().timestamp()

    async def handle(self, *requests: Request) -> None:
        synced.extend(requests)


@mark.asyncio
async def test_queue() -> None:
    storage = RuntimeStorage()
    queue: Queue = Queue(storage)

    def get_pipe(thread: int) -> str:
        return Tube(SyncHandler, Route(thread=thread)).pipe

    await queue.register(
        SyncHandler,
        Request(1),
        Request(2),
        Request(3),
        Request(4),
        Request(5),
    )

    assert get_pipe(0) in storage.data
    assert get_pipe(1) in storage.data
    assert get_pipe(2) not in storage.data

    assert await storage.length(get_pipe(0)) == 2
    assert await storage.length(get_pipe(1)) == 3
    assert await storage.length(get_pipe(2)) == 0

    assert context['started'] == 0
    assert context['stopped'] == 0

    worker = Worker(RuntimeLock(), queue)
    await worker.loop(2)

    assert context['started'] != 0
    assert context['stopped'] != 0
    assert context['started'] != context['stopped']

    assert len(synced) == 2
    thread = str(synced[0].bucket % 2)

    if thread == '0':
        assert await storage.length(get_pipe(0)) == 0
        assert await storage.length(get_pipe(1)) == 3
    else:
        assert await storage.length(get_pipe(0)) == 2
        assert await storage.length(get_pipe(1)) == 1

    # test worker switch
    settings.worker_empty_pause = 0
    (last_started, last_stopped) = tuple(context.values())
    await worker.loop(3)
    assert len(synced) == 5
    assert await storage.length(get_pipe(0)) == 0
    assert await storage.length(get_pipe(1)) == 0
    assert last_started != context['started']
    assert last_stopped != context['stopped']
